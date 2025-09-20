from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster._config import UserConfigSchema
from dagster._core.launcher.base import LaunchRunContext, RunLauncher, WorkerStatus
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.storage.tags import DOCKER_IMAGE_TAG
from dagster._grpc.types import ExecuteRunArgs
from dagster._serdes import ConfigurableClass
from pydantic import Field

from dagster_ray._base.utils import get_dagster_tags
from dagster_ray.configs import RayExecutionConfig
from dagster_ray.kuberay.client import RayJobClient
from dagster_ray.kuberay.client.base import load_kubeconfig
from dagster_ray.kuberay.configs import RayJobConfig
from dagster_ray.kuberay.resources.base import BaseKubeRayResourceConfig
from dagster_ray.kuberay.utils import normalize_k8s_label_values

try:
    from dagster._core.remote_representation.origin import RemoteJobOrigin
except ImportError:
    # for new versions of dagster > 1.11.6
    from dagster._core.remote_origin import RemoteJobOrigin  # pyright: ignore[reportMissingImports]  # noqa: F401


def get_ray_job_id_from_run_id(run_id: str, resume_attempt_number=None):
    """Generate a KubeRay job name from Dagster run ID."""
    return f"dagster-run-{run_id}" + ("" if not resume_attempt_number else f"-{resume_attempt_number}")


class KubeRayRunLauncherConfig(BaseKubeRayResourceConfig, RayExecutionConfig):
    """Configuration for the KubeRay run launcher."""

    ray_job: RayJobConfig = Field(
        default_factory=RayJobConfig,
        description="Configuration for the Kubernetes `RayJob` CR",
    )

    kube_context: str | None = Field(
        default=None,
        description="Kubernetes context to use. If not specified, uses the current context.",
    )

    kube_config: str | None = Field(
        default=None,
        description="Path to the Kubernetes config file. If not specified, uses the default config.",
    )

    log_cluster_conditions: bool = Field(
        default=True,
        description="Whether to log `RayCluster` conditions while waiting for the RayCluster to become ready. Learn more: [KubeRay docs](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/observability.html#raycluster-status-conditions).",
    )

    timeout: float = Field(
        default=600.0,
        description="Timeout for various Kubernetes operations in seconds.",
    )

    @property
    def namespace(self) -> str:
        return self.ray_job.namespace


class KubeRayRunLauncher(RunLauncher, ConfigurableClass):
    """RunLauncher that submits Dagster runs as KubeRay jobs to a Kubernetes cluster.

    Each Dagster run is executed as a separate Ray Job on Kubernetes using the KubeRay operator.
    This provides automatic cluster management and cleanup through Kubernetes native resources.

    Configuration can be provided via `dagster.yaml` and individual runs can override
    settings using the `dagster-ray/config` tag.

    Example:
        Configure via `dagster.yaml`
        ```yaml
        run_launcher:
          module: dagster_ray.kuberay
          class: KubeRayRunLauncher
          config:
            ray_job:
              spec:
                rayClusterSpec:
                  headGroupSpec:
                    template:
                      spec:
                        containers:
                          - name: ray-head
                            image: "rayproject/ray:2.0.0"
        ```

    Example:
        Override settings per job
        ```python
        import dagster as dg

        @dg.job(
            tags={
                "dagster-ray/config": {
                    "num_cpus": 16,
                    "num_gpus": 1,
                    "runtime_env": {"pip": {"packages": ["torch"]}},
                }
            }
        )
        def my_job():
            return my_op()
        ```
    """

    def __init__(self, **config):
        self._config = KubeRayRunLauncherConfig(**config)

        # Initialize Kubernetes configuration
        load_kubeconfig(context=self._config.kube_context, config_file=self._config.kube_config)
        self._client = RayJobClient(kube_context=self._config.kube_context, kube_config=self._config.kube_config)

        super().__init__()

    @classmethod
    def config_type(cls) -> UserConfigSchema:
        return {"kuberay": KubeRayRunLauncherConfig.to_config_schema().as_field()}

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(**config_value)

    def _get_dagster_tags(self, context: LaunchRunContext) -> Mapping[str, str]:
        """Get standardized Dagster tags for labeling Kubernetes resources."""
        return get_dagster_tags(context)

    def _get_env_vars_to_inject(self, dagster_run: DagsterRun) -> dict[str, str]:
        """Get environment variables to inject into the Ray job."""
        env_vars = {
            "DAGSTER_RUN_JOB_NAME": dagster_run.job_name or "unknown",
        }

        return env_vars

    def _create_ray_job_spec(self, context: LaunchRunContext, command_args: list[str]) -> dict[str, Any]:
        """Create the RayJob specification for the run."""
        dagster_run = context.dagster_run

        # Get user-provided run configuration - allow full RayJobConfig override
        user_provided_config = {}
        if "dagster-ray/config" in dagster_run.tags:
            import json

            user_provided_config = json.loads(dagster_run.tags["dagster-ray/config"])

        # Create a merged config by overlaying user config on base config
        merged_ray_job_config = self._merge_ray_job_configs(self._config.ray_job, user_provided_config)

        # Generate job name
        ray_job_name = get_ray_job_id_from_run_id(dagster_run.run_id)

        # Create the RayJob spec with merged configuration
        ray_job_spec = merged_ray_job_config.to_k8s(
            image=(self._config.image or dagster_run.tags.get(DOCKER_IMAGE_TAG)),
            labels=normalize_k8s_label_values({**self._get_dagster_tags(context)}),
            env_vars=self._get_env_vars_to_inject(dagster_run),
        )

        ray_job_spec["metadata"]["name"] = ray_job_name

        # Add the Dagster command to the entrypoint
        ray_job_spec["spec"]["entrypoint"] = " ".join(command_args)

        return ray_job_spec

    def _merge_ray_job_configs(self, base_config: RayJobConfig, user_config: dict[str, Any]) -> RayJobConfig:
        """Merge user-provided configuration with base configuration.

        This follows the same pattern as dagster-k8s where users can override
        any field in the configuration via the dagster-ray/config tag.
        """
        from dagster._utils.merger import deep_merge_dicts

        # Convert base config to dict
        base_dict = base_config.model_dump()

        # Deep merge user config over base config
        merged_dict = deep_merge_dicts(base_dict, user_config)

        # Create new config instance with merged values
        return RayJobConfig(**merged_dict)  # pyright: ignore[reportArgumentType]

    def launch_run(self, context: LaunchRunContext) -> None:
        """Launch a Dagster run as a KubeRay job."""
        dagster_run = context.dagster_run
        ray_job_name = get_ray_job_id_from_run_id(dagster_run.run_id)
        namespace = self._config.namespace
        job_origin = dg._check.not_none(dagster_run.job_code_origin)

        args = list(
            ExecuteRunArgs(
                job_origin=job_origin,
                run_id=dagster_run.run_id,
                instance_ref=self._instance.get_ref(),
                set_exit_code_on_failure=True,
            ).get_command_args()
        )

        # wrap the json in quotes to prevent errors with shell commands
        args[-1] = "'" + args[-1] + "'"

        ray_job_spec = self._create_ray_job_spec(context, args)

        self._instance.report_engine_event(
            f"Launching run {dagster_run.run_id} as KubeRay job {namespace}/{ray_job_name}",
            dagster_run,
            cls=self.__class__,
        )

        try:
            # Create the RayJob
            self._client.create(
                body=ray_job_spec,
                namespace=namespace,
            )

            # Wait for the job to start running
            self._client.wait_until_running(
                name=ray_job_name,
                namespace=namespace,
                timeout=self._config.timeout,
                poll_interval=self._config.poll_interval,
                terminate_on_timeout=False,  # Don't terminate on timeout for runs
                port_forward=False,
                log_cluster_conditions=self._config.log_cluster_conditions,
            )

            self._instance.report_engine_event(
                f"KubeRay job {namespace}/{ray_job_name} started successfully",
                dagster_run,
                cls=self.__class__,
            )

        except Exception as e:
            self._instance.report_engine_event(
                f"Failed to launch KubeRay job {namespace}/{ray_job_name}: {str(e)}",
                dagster_run,
                cls=self.__class__,
            )
            raise

    def can_terminate(self, run_id: str) -> bool:
        """Check if a run can be terminated."""
        return True

    def terminate(self, run_id: str) -> bool:
        """Terminate a running KubeRay job."""
        ray_job_name = get_ray_job_id_from_run_id(run_id)
        namespace = self._config.ray_job.namespace

        try:
            self._client.terminate(
                name=ray_job_name,
                namespace=namespace,
                port_forward=False,
            )
            return True
        except Exception:
            # If termination fails, the job might already be completed or not exist
            return False

    def get_run_worker_debug_info(self, run: DagsterRun, include_container_logs: bool | None = True) -> str | None:
        """Get debug information for a run."""
        ray_job_name = get_ray_job_id_from_run_id(run.run_id)
        namespace = self._config.ray_job.namespace

        try:
            # Try to get the RayJob status
            ray_job = self._client.get(name=ray_job_name, namespace=namespace)
            if ray_job:
                status_info = f"RayJob {namespace}/{ray_job_name}:\n"
                status_info += f"Status: {ray_job.get('status', {})}\n"
                if include_container_logs:
                    # Could potentially include container logs here if needed
                    pass
                return status_info
        except Exception:
            pass

        return None

    def get_run_worker_status(self, run_id: str) -> WorkerStatus:
        """Get the status of a run worker."""
        ray_job_name = get_ray_job_id_from_run_id(run_id)
        namespace = self._config.ray_job.namespace

        try:
            status = self._client.get_status(name=ray_job_name, namespace=namespace, timeout=5.0)
            job_status = status.get("jobStatus")

            if job_status == "RUNNING":
                return WorkerStatus.RUNNING
            elif job_status == "SUCCEEDED":
                return WorkerStatus.SUCCESS
            elif job_status in ["FAILED", "STOPPED"]:
                return WorkerStatus.FAILED
            else:
                return WorkerStatus.UNKNOWN
        except TimeoutError:
            return WorkerStatus.NOT_FOUND
        except Exception:
            return WorkerStatus.UNKNOWN
