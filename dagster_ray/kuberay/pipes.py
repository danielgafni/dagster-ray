from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Union, cast

import dagster._check as check
import yaml
from dagster import AssetExecutionContext, DagsterInvariantViolationError, OpExecutionContext, PipesClient
from dagster._annotations import beta
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
from typing_extensions import TypeAlias

from dagster_ray._base.utils import get_dagster_tags
from dagster_ray.kuberay.client import RayJobClient
from dagster_ray.kuberay.client.rayjob.client import RayJobStatus
from dagster_ray.kuberay.utils import normalize_k8s_label_values
from dagster_ray.pipes import PipesRayJobMessageReader, generate_job_id

if TYPE_CHECKING:
    from ray.job_submission import JobSubmissionClient

OpOrAssetExecutionContext: TypeAlias = Union[OpExecutionContext, AssetExecutionContext]


@beta
class PipesKubeRayJobClient(PipesClient, TreatAsResourceParam):
    """A pipes client for running ``RayJob`` on Kubernetes.

    Args:
        context_injector (Optional[PipesContextInjector]): A context injector to use to inject
            context into the ``RayJob``. Defaults to :py:class:`PipesEnvContextInjector`.
        message_reader (Optional[PipesMessageReader]): A message reader to use to read messages
            from the glue job run. Defaults to :py:class:`PipesRayJobMessageReader`.
        client (Optional[boto3.client]): The Kubernetes API client.
        forward_termination (bool): Whether to terminate the Ray job when the Dagster process receives a termination signal,
            or if the startup timeout is reached. Defaults to ``True``.
        timeout (int): Timeout for various internal interactions with the Kubernetes RayJob.
        poll_interval (int): Interval at which to poll the Kubernetes for status updates.
        port_forward (bool): Whether to use Kubernetes port-forwarding to connect to the KubeRay cluster.
        Is useful when running in a local environment.
    """

    def __init__(
        self,
        client: RayJobClient | None = None,
        context_injector: PipesContextInjector | None = None,
        message_reader: PipesMessageReader | None = None,
        forward_termination: bool = True,
        timeout: int = 600,
        poll_interval: int = 5,
        port_forward: bool = False,
    ):
        self.client: RayJobClient = client or RayJobClient()

        self._context_injector = context_injector or PipesEnvContextInjector()
        self._message_reader = message_reader or PipesRayJobMessageReader()

        self.forward_termination = check.bool_param(forward_termination, "forward_termination")
        self.timeout = check.int_param(timeout, "timeout")
        self.poll_interval = check.int_param(poll_interval, "poll_interval")
        self.port_forward = check.bool_param(port_forward, "port_forward")

        self._job_submission_client: JobSubmissionClient | None = None

    @property
    def job_submission_client(self) -> JobSubmissionClient:
        if self._job_submission_client is None:
            raise DagsterInvariantViolationError("JobSubmissionClient is only available inside the run method.")
        else:
            return self._job_submission_client

    def run(  # type: ignore
        self,
        *,
        context: OpOrAssetExecutionContext,
        ray_job: dict[str, Any],
        extras: PipesExtras | None = None,
    ) -> PipesClientCompletedInvocation:
        """
        Execute a RayJob, enriched with the Pipes protocol.

        Args:
            context (OpExecutionContext): Current Dagster op or asset context.
            ray_job (Dict[str, Any]): RayJob specification. `API reference <https://ray-project.github.io/kuberay/reference/api/#rayjob>`_.
            extras (Optional[Dict[str, Any]]): Additional information to pass to the Pipes session.
        """
        with open_pipes_session(
            context=context,
            message_reader=self._message_reader,
            context_injector=self._context_injector,
            extras=extras,
        ) as session:
            ray_job = self._enrich_ray_job(context, session, ray_job)
            start_response = self._start(context, ray_job)

            name = ray_job["metadata"]["name"]
            namespace = ray_job["metadata"]["namespace"]

            with self.client.ray_cluster_client.job_submission_client(
                name=self.client.get_ray_cluster_name(name=name, namespace=namespace),
                namespace=namespace,
                port_forward=self.port_forward,
            ) as job_submission_client:
                self._job_submission_client = job_submission_client

                try:
                    self._read_messages(context, start_response)
                    self._wait_for_completion(context, start_response)
                    return PipesClientCompletedInvocation(session)

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

        ray_job["spec"]["runtimeEnvYAML"] = yaml.safe_dump(runtime_env)

        image_from_run_tag = context.run.tags.get("dagster/image")

        for container in ray_job["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]["containers"]:
            container["image"] = container.get("image") or image_from_run_tag

        for worker_spec in ray_job["spec"]["rayClusterSpec"]["workerGroupSpecs"]:
            for container in worker_spec["template"]["spec"]["containers"]:
                container["image"] = container.get("image") or image_from_run_tag

        return ray_job

    def _start(self, context: OpOrAssetExecutionContext, ray_job: dict[str, Any]) -> dict[str, Any]:
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

    def _read_messages(self, context: OpOrAssetExecutionContext, start_response: dict[str, Any]) -> None:
        status = cast(RayJobStatus, start_response["status"])

        if isinstance(self._message_reader, PipesRayJobMessageReader):
            # starts a thread
            self._message_reader.consume_job_logs(
                # TODO: investigate why some messages aren't being handled with blocking=False
                client=self.job_submission_client,
                job_id=status["jobId"],
                blocking=True,
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
                    context.log.info(f"[pipes] RayJob {namespace}/{name} is complete!")
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
