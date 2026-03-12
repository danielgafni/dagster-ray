from __future__ import annotations

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
    generate_job_id,
)
from dagster_ray.kuberay.client import RayJobClient
from dagster_ray.kuberay.client.rayjob.client import RayJobStatus
from dagster_ray.kuberay.utils import normalize_k8s_label_values
from dagster_ray.types import OpOrAssetExecutionContext

if TYPE_CHECKING:
    from ray.job_submission import JobSubmissionClient


class PipesKubeRayJobClient(dg.PipesClient, TreatAsResourceParam):
    """A pipes client for running ``RayJob`` on Kubernetes.

    Args:
        context_injector: A context injector to use to inject
            context into the ``RayJob``. Defaults to [`PipesEnvContextInjector`][dagster.PipesEnvContextInjector].
        message_reader: A message reader to use to read messages
            from the glue job run. Defaults to [`PipesRayJobMessageReader`][dagster_ray.core.pipes.PipesRayJobMessageReader].
        client: The Kubernetes API client.
        forward_termination: Whether to terminate the Ray job when the Dagster process receives a termination signal,
            or if the startup timeout is reached. Defaults to ``True``.
        timeout: Timeout for various internal interactions with the Kubernetes RayJob.
        poll_interval: Interval at which to poll Kubernetes for status updates.
        port_forward: Whether to use Kubernetes port-forwarding to connect to the KubeRay cluster.
            Is useful when running in a local environment.
        address: Ray dashboard address (e.g., "https://ray-cluster.example.com").
            When provided, connects directly to this address instead of using port-forwarding or in-cluster service IPs.
            Can be overridden per-job in the `run()` method.
        headers: HTTP headers for dashboard requests, e.g. ``{"Authorization": "Bearer token"}``.
            Can be overridden per-job in the `run()` method.
        verify: TLS certificate verification. ``True`` uses system certs, ``False`` disables
            verification, or a path to a CA bundle.
            Can be overridden per-job in the `run()` method.
        cookies: Arbitrary metadata stored alongside all submitted jobs.
            Can be overridden per-job in the `run()` method.
        metadata: Arbitrary metadata to store along with all jobs.
            Will be merged with per-job metadata. Can be overridden per-job in the `run()` method.

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
        address: str | None = None,
        headers: dict[str, Any] | None = None,
        verify: str | bool = True,
        cookies: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        self.client: RayJobClient = client or RayJobClient()

        self._context_injector = context_injector or PipesEnvContextInjector()

        self._message_reader = message_reader or PipesRayJobMessageReader(
            job_submission_client_kwargs={
                "headers": headers,
                "verify": verify,
                "cookies": cookies,
                "metadata": metadata,
            }
        )

        self.forward_termination = check.bool_param(forward_termination, "forward_termination")
        self.timeout = check.int_param(timeout, "timeout")
        self.poll_interval = check.int_param(poll_interval, "poll_interval")
        self.port_forward = check.bool_param(port_forward, "port_forward")
        self.address = address
        self.headers = headers
        self.verify = verify
        self.cookies = cookies
        self.metadata = metadata

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
        ray_job: dict[str, Any],
        extras: PipesExtras | None = None,
        address: str | None = None,
        headers: dict[str, Any] | None = None,
        verify: str | bool | None = None,
        cookies: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PipesClientCompletedInvocation:
        """
        Execute a RayJob, enriched with the Pipes protocol.

        Args:
            context: Current Dagster op or asset context.
            ray_job: RayJob specification. `API reference <https://ray-project.github.io/kuberay/reference/api/#rayjob>`_.
            extras: Additional information to pass to the Pipes session.
            address: Ray dashboard address override for this specific job.
            headers: HTTP headers override for this specific job.
            verify: TLS verification override for this specific job.
            cookies: Cookies override for this specific job.
            metadata: Metadata override for this specific job.
        """
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
                address=address or self.address,
                headers=headers or self.headers,
                verify=verify if verify is not None else self.verify,
                cookies=cookies or self.cookies,
                metadata=metadata or self.metadata,
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
                        self._terminate(
                            context,
                            start_response,
                            address=address,
                            headers=headers,
                            verify=verify,
                            cookies=cookies,
                            metadata=metadata,
                        )
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

    def _terminate(
        self,
        context: OpOrAssetExecutionContext,
        start_response: dict[str, Any],
        address: str | None = None,
        headers: dict[str, Any] | None = None,
        verify: str | bool | None = None,
        cookies: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        name = start_response["metadata"]["name"]
        namespace = start_response["metadata"]["namespace"]

        context.log.info(f"[pipes] Terminating RayJob {namespace}/{name} ...")
        self.client.terminate(
            name=name,
            namespace=namespace,
            port_forward=self.port_forward,
            address=address or self.address,
            headers=headers or self.headers,
            verify=verify if verify is not None else self.verify,
            cookies=cookies or self.cookies,
            metadata=metadata or self.metadata,
        )
        context.log.info(f"[pipes] RayJob {namespace}/{name} terminated.")
