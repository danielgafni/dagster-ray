from __future__ import annotations

import copy
import json
import logging
import time
from typing import TYPE_CHECKING, Any, cast

import dagster as dg
import dagster._check as check
import yaml
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.errors import DagsterExecutionInterruptedError
from dagster._core.pipes.client import (
    PipesClientCompletedInvocation,
    PipesContextInjector,
    PipesMessageReader,
)
from dagster._core.pipes.context import PipesSession
from dagster._core.pipes.utils import PipesEnvContextInjector, open_pipes_session
from dagster_pipes import PipesExtras

from dagster_ray._base.utils import get_dagster_tags
from dagster_ray.core.pipes import (
    PIPES_LAUNCHED_EXTRAS_RAY_ADDRESS_KEY,
    PIPES_LAUNCHED_EXTRAS_RAY_JOB_ID_KEY,
    PipesRayJobMessageReader,
    SubmitJobParams,
    generate_job_id,
)
from dagster_ray.kuberay.client import RayJobClient
from dagster_ray.kuberay.client.rayjob.client import RayJobStatus
from dagster_ray.kuberay.configs import RayJobConfig, RayJobSpec
from dagster_ray.kuberay.utils import normalize_k8s_label_values
from dagster_ray.types import OpOrAssetExecutionContext

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ray.job_submission import JobSubmissionClient


def _ray_job_from_submit_params(
    context: OpOrAssetExecutionContext,
    submit_job_params: SubmitJobParams,
    namespace: str = "ray",
) -> dict[str, Any]:
    """Build a minimal KubeRay RayJob manifest from submit_job_params.

    Reuses ``RayJobConfig`` / ``RayJobSpec`` to generate the manifest,
    consistent with how ``KubeRayCluster`` and ``KubeRayInteractiveJob``
    auto-generate their specs. Uses the ``dagster/image`` run tag for
    the container image.
    """
    runtime_env_yaml = None
    if runtime_env := submit_job_params.get("runtime_env"):
        runtime_env_yaml = yaml.safe_dump(runtime_env, default_style='"')

    ray_job_config = RayJobConfig(
        metadata={"namespace": namespace},
        spec=RayJobSpec(
            runtime_env_yaml=runtime_env_yaml,
            entrypoint_num_cpus=submit_job_params.get("entrypoint_num_cpus"),
            entrypoint_num_gpus=submit_job_params.get("entrypoint_num_gpus"),
            entrypoint_memory=submit_job_params.get("entrypoint_memory"),
            entrypoint_resources=json.dumps(er)
            if (er := submit_job_params.get("entrypoint_resources")) is not None
            else None,
            metadata=submit_job_params.get("metadata"),
            job_id=submit_job_params.get("submission_id"),
        ),
    )

    image = context.run.tags.get("dagster/image")
    ray_job = ray_job_config.to_k8s(context=context, image=image)
    ray_job["metadata"]["namespace"] = namespace
    ray_job["spec"]["entrypoint"] = submit_job_params["entrypoint"]

    return ray_job


def _merge_submit_params_into_ray_job(
    ray_job: dict[str, Any],
    submit_job_params: SubmitJobParams,
) -> dict[str, Any]:
    """Merge ``submit_job_params`` into a KubeRay RayJob manifest, warning on conflicts."""
    ray_job = copy.deepcopy(ray_job)

    def _set(key: str, value: Any) -> None:
        if key in ray_job["spec"] and ray_job["spec"][key] != value:
            logger.warning(
                "[pipes] submit_job_params overwriting ray_job spec field '%s': %r -> %r",
                key,
                ray_job["spec"][key],
                value,
            )
        ray_job["spec"][key] = value

    _set("entrypoint", submit_job_params["entrypoint"])

    if runtime_env := submit_job_params.get("runtime_env"):
        _set("runtimeEnvYAML", yaml.safe_dump(runtime_env, default_style='"'))

    if (num_cpus := submit_job_params.get("entrypoint_num_cpus")) is not None:
        _set("entrypointNumCpus", num_cpus)

    if (num_gpus := submit_job_params.get("entrypoint_num_gpus")) is not None:
        _set("entrypointNumGpus", num_gpus)

    if (memory := submit_job_params.get("entrypoint_memory")) is not None:
        _set("entrypointMemory", memory)

    if (resources := submit_job_params.get("entrypoint_resources")) is not None:
        _set("entrypointResources", resources)

    if (metadata := submit_job_params.get("metadata")) is not None:
        _set("metadata", metadata)

    if (submission_id := submit_job_params.get("submission_id")) is not None:
        _set("jobId", submission_id)

    return ray_job


class PipesKubeRayJobClient(dg.PipesClient, TreatAsResourceParam):
    """A pipes client for running ``RayJob`` on Kubernetes.

    Args:
        context_injector (Optional[PipesContextInjector]): A context injector to use to inject
            context into the ``RayJob``. Defaults to [`PipesEnvContextInjector`][dagster.PipesEnvContextInjector].
        message_reader (Optional[PipesMessageReader]): A message reader to use to read messages
            from the glue job run. Defaults to [`PipesRayJobMessageReader`][dagster_ray.core.pipes.PipesRayJobMessageReader].
        client (Optional[boto3.client]): The Kubernetes API client.
        forward_termination (bool): Whether to terminate the Ray job when the Dagster process receives a termination signal,
            or if the startup timeout is reached. Defaults to ``True``.
        timeout (int): Timeout for various internal interactions with the Kubernetes RayJob.
        poll_interval (int): Interval at which to poll Kubernetes for status updates.
        port_forward (bool): Whether to use Kubernetes port-forwarding to connect to the KubeRay cluster.
            Is useful when running in a local environment.

    Info:
        Image defaults to `dagster/image` run tag.

    Tip:
        Make sure `ray[full]` is available in the image.
    """

    def __init__(
        self,
        client: RayJobClient | None = None,
        context_injector: PipesContextInjector | None = None,
        message_reader: PipesMessageReader | None = None,
        forward_termination: bool = True,
        timeout: float = 600,
        poll_interval: float = 1,
        port_forward: bool = False,
    ):
        self.client: RayJobClient = client or RayJobClient()

        self._context_injector = context_injector or PipesEnvContextInjector()
        self._message_reader = message_reader or PipesRayJobMessageReader()

        self.forward_termination = check.bool_param(forward_termination, "forward_termination")
        self.timeout = check.numeric_param(timeout, "timeout")
        self.poll_interval = check.numeric_param(poll_interval, "poll_interval")
        self.port_forward = check.bool_param(port_forward, "port_forward")

        self._job_submission_client: JobSubmissionClient | None = None

    @property
    def job_submission_client(self) -> JobSubmissionClient:
        if self._job_submission_client is None:
            raise dg.DagsterInvariantViolationError("JobSubmissionClient is only available inside the run method.")
        else:
            return self._job_submission_client

    def run(  # type: ignore
        self,
        *,
        context: OpOrAssetExecutionContext,
        submit_job_params: SubmitJobParams,
        ray_job: dict[str, Any] | None = None,
        extras: PipesExtras | None = None,
    ) -> PipesClientCompletedInvocation:
        """Execute a RayJob, enriched with the Pipes protocol.

        Args:
            context: Current Dagster op or asset context.
            submit_job_params: Job submission parameters (entrypoint, runtime_env, etc.).
            ray_job: Optional KubeRay RayJob template dict. When provided, ``submit_job_params`` values are merged into it. When omitted, a minimal template is auto-generated using the ``dagster/image`` run tag.
            extras: Additional information to pass to the Pipes session.
        """
        if ray_job is not None:
            ray_job = _merge_submit_params_into_ray_job(ray_job, submit_job_params)
        else:
            ray_job = _ray_job_from_submit_params(context, submit_job_params)

        with open_pipes_session(
            context=context,
            message_reader=self._message_reader,
            context_injector=self._context_injector,
            extras=extras,
        ) as session:
            ray_job = self._enrich_ray_job(context, session, ray_job)
            start_response = self._start(context, session, ray_job)
            start_status = cast(RayJobStatus, start_response["status"])
            ray_job_id = start_status["jobId"]  # pyright: ignore[reportTypedDictNotRequiredAccess]

            name = ray_job["metadata"]["name"]
            namespace = ray_job["metadata"]["namespace"]

            with self.client.ray_cluster_client.job_submission_client(
                name=self.client.get_ray_cluster_name(
                    name=name, namespace=namespace, timeout=self.timeout, poll_interval=self.poll_interval
                ),
                namespace=namespace,
                port_forward=self.port_forward,
            ) as job_submission_client:
                self._job_submission_client = job_submission_client

                session.report_launched(
                    {
                        "extras": {
                            PIPES_LAUNCHED_EXTRAS_RAY_JOB_ID_KEY: ray_job_id,
                            PIPES_LAUNCHED_EXTRAS_RAY_ADDRESS_KEY: job_submission_client.get_address(),
                        }
                    }
                )

                try:
                    self._wait_for_completion(context, start_response)

                    if isinstance(self._message_reader, PipesRayJobMessageReader) and self.port_forward:
                        # in this case the message reader will fail once port forwarding is finished
                        # TODO: merge https://github.com/danielgafni/dagster-ray/pull/123
                        # to avoid this work-around
                        self._message_reader.thread_ready.wait()
                        context.log.debug(
                            "[pipes] waiting for PipesRayJobMessageReader to complete before stopping port-forwarding"
                        )
                        self._message_reader.session_closed.set()
                        self._message_reader.completed.wait()

                    return PipesClientCompletedInvocation(
                        session, metadata={"RayJob": f"{namespace}/{name}", "Ray Job ID": ray_job_id}
                    )

                except DagsterExecutionInterruptedError:
                    if self.forward_termination:
                        context.log.warning(
                            f"[pipes] Dagster process interrupted! Will terminate RayJob {namespace}/{name}."
                        )
                        self._terminate(context, start_response)
                    raise

    def get_dagster_tags(self, context: OpOrAssetExecutionContext) -> dict[str, str]:
        tags = get_dagster_tags(context)
        return tags

    def _enrich_ray_job(
        self, context: OpOrAssetExecutionContext, session: PipesSession, ray_job: dict[str, Any]
    ) -> dict[str, Any]:
        env_vars = session.get_bootstrap_env_vars()

        ray_job["metadata"] = ray_job.get("metadata", {})
        ray_job["metadata"]["labels"] = ray_job["metadata"].get("labels", {})

        ray_job["metadata"]["name"] = ray_job["metadata"].get("name", f"pipes-{generate_job_id()}")
        ray_job["metadata"]["labels"].update(normalize_k8s_label_values(self.get_dagster_tags(context)))

        # update env vars in runtimeEnv
        runtime_env_yaml = ray_job["spec"].get("runtimeEnvYAML", "{}")

        runtime_env = yaml.safe_load(runtime_env_yaml)
        runtime_env["env_vars"] = runtime_env.get("env_vars", {})
        runtime_env["env_vars"].update(env_vars)

        ray_job["spec"]["runtimeEnvYAML"] = yaml.safe_dump(runtime_env, default_style='"')

        image_from_run_tag = context.run.tags.get("dagster/image")

        for container in ray_job["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]["containers"]:
            container["image"] = container.get("image") or image_from_run_tag

        for worker_spec in ray_job["spec"]["rayClusterSpec"]["workerGroupSpecs"]:
            for container in worker_spec["template"]["spec"]["containers"]:
                container["image"] = container.get("image") or image_from_run_tag

        return ray_job

    def _start(
        self, context: OpOrAssetExecutionContext, session: PipesSession, ray_job: dict[str, Any]
    ) -> dict[str, Any]:
        name = ray_job["metadata"]["name"]
        namespace = ray_job["metadata"]["namespace"]

        context.log.info(f"[pipes] Starting RayJob {namespace}/{name}...")

        self.client.create(
            body=ray_job,
            namespace=ray_job["metadata"]["namespace"],
        )

        self.client.wait_until_running(
            name=name,
            namespace=namespace,
            timeout=self.timeout,
            poll_interval=self.poll_interval,
            terminate_on_timeout=self.forward_termination,
            port_forward=self.port_forward,
        )

        return self.client.get(
            namespace=ray_job["metadata"]["namespace"],
            name=ray_job["metadata"]["name"],
        )

    def _wait_for_completion(self, context: OpOrAssetExecutionContext, start_response: dict[str, Any]) -> RayJobStatus:
        context.log.info("[pipes] Waiting for RayJob to complete...")

        name = start_response["metadata"]["name"]
        namespace = start_response["metadata"]["namespace"]

        while True:
            status = self.client.get_status(
                name=name,
                namespace=namespace,
            )

            if job_status := status.get("jobStatus"):
                if job_status in ["PENDING", "RUNNING"]:
                    pass
                elif job_status == "SUCCEEDED":
                    context.log.info(f"[pipes] RayJob {namespace}/{name} succeeded!")
                    return status
                elif job_status in ["STOPPED", "FAILED"]:
                    raise RuntimeError(
                        f"RayJob {namespace}/{name} status is {job_status}. Message:\n{status.get('message')}"
                    )
                else:
                    raise RuntimeError(
                        f"RayJob {namespace}/{name} has an unknown status: {job_status}. Message:\n{status.get('message')}"
                    )

            time.sleep(self.poll_interval)

    def _terminate(self, context: OpOrAssetExecutionContext, start_response: dict[str, Any]) -> None:
        name = start_response["metadata"]["name"]
        namespace = start_response["metadata"]["namespace"]

        context.log.info(f"[pipes] Terminating RayJob {namespace}/{name} ...")
        self.client.terminate(name=name, namespace=namespace, port_forward=self.port_forward)
        context.log.info(f"[pipes] RayJob {namespace}/{name} terminated.")
