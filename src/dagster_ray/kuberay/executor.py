from collections.abc import Iterator, Mapping
from typing import Any, cast

import dagster as dg
from dagster._core.definitions.executor_definition import multiple_process_executor_requirements
from dagster._core.definitions.metadata import MetadataValue
from dagster._core.events import DagsterEvent, EngineEventData
from dagster._core.execution.retries import RetryMode, get_retries_config
from dagster._core.execution.tags import get_tag_concurrency_limits_config
from dagster._core.executor.base import Executor
from dagster._core.executor.init import InitExecutorContext
from dagster._core.executor.step_delegating import (
    CheckStepHealthResult,
    StepDelegatingExecutor,
    StepHandler,
    StepHandlerContext,
)

from dagster_ray.kuberay.configs import RayJobConfig

try:
    from dagster._core.remote_representation.origin import RemoteJobOrigin  # noqa: F401
except ImportError:
    # for new versions of dagster > 1.11.6
    from dagster._core.remote_origin import RemoteJobOrigin  # noqa: F401  # pyright: ignore[reportMissingImports]

from dagster._utils.merger import merge_dicts
from pydantic import Field

from dagster_ray.configs import RayExecutionConfig
from dagster_ray.kuberay.client import RayJobClient
from dagster_ray.kuberay.client.base import load_kubeconfig
from dagster_ray.kuberay.resources.base import BaseKubeRayResourceConfig
from dagster_ray.kuberay.utils import normalize_k8s_label_values
from dagster_ray.utils import get_k8s_object_name


class KubeRayExecutorConfig(BaseKubeRayResourceConfig, RayExecutionConfig):
    """Configuration for the KubeRay executor."""

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


_KUBERAY_CONFIG_SCHEMA = KubeRayExecutorConfig.to_config_schema().as_field()

_KUBERAY_EXECUTOR_CONFIG_SCHEMA = merge_dicts(
    {"kuberay": _KUBERAY_CONFIG_SCHEMA},  # type: ignore
    {"retries": get_retries_config(), "tag_concurrency_limits": get_tag_concurrency_limits_config()},
)


@dg.executor(
    name="kuberay",
    config_schema=_KUBERAY_EXECUTOR_CONFIG_SCHEMA,
    requirements=multiple_process_executor_requirements(),
)
def kuberay_executor(init_context: InitExecutorContext) -> Executor:
    """Executes steps by submitting them as KubeRay jobs.

    Each step is executed as a separate Ray Job on a Kubernetes cluster using the KubeRay operator.
    This executor provides automatic cluster management and cleanup through Kubernetes native resources.

    Example:
        Use `kuberay_executor` for the entire code location
        ```python
        import dagster as dg
        from dagster_ray import kuberay_executor

        kuberay_executor = kuberay_executor.configured(
            {
                "kuberay": {
                    "ray_job": {
                        "spec": {
                            "rayClusterSpec": {
                                "headGroupSpec": {
                                    "template": {
                                        "spec": {
                                            "containers": [
                                                {
                                                    "name": "ray-head",
                                                    "image": "rayproject/ray:2.0.0",
                                                }
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )

        defs = dg.Definitions(..., executor=kuberay_executor)
        ```

    Example:
        Override configuration for a specific asset
        ```python
        import dagster as dg

        @dg.asset(
            op_tags={"dagster-ray/config": {"num_cpus": 2}}
        )
        def my_asset(): ...
        ```
    """
    exc_cfg = init_context.executor_config
    kuberay_cfg = KubeRayExecutorConfig(**exc_cfg["kuberay"])  # type: ignore

    load_kubeconfig(context=kuberay_cfg.kube_context, config_file=kuberay_cfg.kube_config)
    client = RayJobClient(kube_context=kuberay_cfg.kube_context, kube_config=kuberay_cfg.kube_config)

    return StepDelegatingExecutor(
        KubeRayStepHandler(
            client=client,
            config=kuberay_cfg,
        ),
        retries=RetryMode.from_config(exc_cfg["retries"]),  # type: ignore
        max_concurrent=dg._check.opt_int_elem(exc_cfg, "max_concurrent"),
        tag_concurrency_limits=dg._check.opt_list_elem(exc_cfg, "tag_concurrency_limits"),
        should_verify_step=True,
    )


class KubeRayStepHandler(StepHandler):
    @property
    def name(self):
        return "KubeRayStepHandler"

    def __init__(
        self,
        client: RayJobClient,
        config: KubeRayExecutorConfig,
    ):
        super().__init__()
        self.client = client
        self.config = config

    def _get_step_key(self, step_handler_context: StepHandlerContext) -> str:
        step_keys_to_execute = cast(list[str], step_handler_context.execute_step_args.step_keys_to_execute)
        assert len(step_keys_to_execute) == 1, "Launching multiple steps is not currently supported"
        return step_keys_to_execute[0]

    def _get_ray_job_name(self, step_handler_context: StepHandlerContext) -> str:
        step_key = self._get_step_key(step_handler_context)

        name_key = get_k8s_object_name(
            step_handler_context.execute_step_args.run_id,
            step_key,
        )

        if step_handler_context.execute_step_args.known_state:
            retry_state = step_handler_context.execute_step_args.known_state.get_retry_state()
            if retry_state.get_attempt_count(step_key):
                return f"dagster-step-{name_key}-{retry_state.get_attempt_count(step_key)}"

        return f"dagster-step-{name_key}"

    def _get_dagster_tags(self, step_handler_context: StepHandlerContext) -> Mapping[str, str]:
        """Get standardized Dagster tags for labeling Kubernetes resources."""
        return step_handler_context.dagster_run.dagster_execution_info

    def _create_ray_job_spec(self, step_handler_context: StepHandlerContext) -> dict[str, Any]:
        """Create the RayJob specification for the step."""
        run = step_handler_context.dagster_run
        step_key = self._get_step_key(step_handler_context)

        # Get user-provided step configuration - allow full RayJobConfig override
        user_provided_config = {}
        if "dagster-ray/config" in step_handler_context.step_tags[step_key]:
            import json

            user_provided_config = json.loads(step_handler_context.step_tags[step_key]["dagster-ray/config"])

        # Create a merged config by overlaying user config on base config
        merged_ray_job_config = self._merge_ray_job_configs(self.config.ray_job, user_provided_config)

        # Create the RayJob spec
        ray_job_name = self._get_ray_job_name(step_handler_context)

        ray_job_spec = merged_ray_job_config.to_k8s(
            image=(self.config.image or run.tags.get("dagster/image")),
            labels=normalize_k8s_label_values({**self._get_dagster_tags(step_handler_context)}),
            env_vars=self._get_env_vars_to_inject(step_handler_context),
        )

        ray_job_spec["metadata"]["name"] = ray_job_name

        # Add the Dagster command to the entrypoint
        command_args = step_handler_context.execute_step_args.get_command_args(skip_serialized_namedtuple=True)
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

    def _get_env_vars_to_inject(self, step_handler_context: StepHandlerContext) -> dict[str, str]:
        """Get environment variables to inject into the Ray job."""
        run = step_handler_context.dagster_run
        step_key = self._get_step_key(step_handler_context)

        env_vars = {
            "DAGSTER_RUN_JOB_NAME": run.job_name,
            "DAGSTER_RUN_STEP_KEY": step_key,
            **{env["name"]: env["value"] for env in step_handler_context.execute_step_args.get_command_env()},
        }

        return env_vars

    def launch_step(self, step_handler_context: StepHandlerContext) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)
        ray_job_name = self._get_ray_job_name(step_handler_context)
        namespace = self.config.ray_job.namespace

        yield DagsterEvent.step_worker_starting(
            step_handler_context.get_step_context(step_key),
            message=f'Executing step "{step_key}" in KubeRay job {namespace}/{ray_job_name}.',
            metadata={
                "RayJob Name": MetadataValue.text(ray_job_name),
                "Namespace": MetadataValue.text(namespace),
            },
        )

        ray_job_spec = self._create_ray_job_spec(step_handler_context)

        self.client.create(
            body=ray_job_spec,
            namespace=namespace,
        )

        # Wait for the job to start running
        self.client.wait_until_running(
            name=ray_job_name,
            namespace=namespace,
            timeout=self.config.timeout,
            poll_interval=self.config.poll_interval,
            terminate_on_timeout=True,
            port_forward=False,
            log_cluster_conditions=self.config.log_cluster_conditions,
        )

    def check_step_health(self, step_handler_context: StepHandlerContext) -> CheckStepHealthResult:
        step_key = self._get_step_key(step_handler_context)
        ray_job_name = self._get_ray_job_name(step_handler_context)
        namespace = self.config.ray_job.namespace

        try:
            status = self.client.get_status(name=ray_job_name, namespace=namespace)
        except Exception as e:
            return CheckStepHealthResult.unhealthy(
                reason=f"KubeRay job {namespace}/{ray_job_name} for step {step_key} could not be found: {e}"
            )

        job_status = status.get("jobStatus")

        if job_status in ["FAILED", "STOPPED"]:
            message = status.get("message", "No message provided")
            return CheckStepHealthResult.unhealthy(
                reason=f"Discovered failed KubeRay job {namespace}/{ray_job_name} for step {step_key}. Status: {job_status}. Message: {message}"
            )

        return CheckStepHealthResult.healthy()

    def terminate_step(self, step_handler_context: StepHandlerContext) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)
        ray_job_name = self._get_ray_job_name(step_handler_context)
        namespace = self.config.ray_job.namespace

        yield DagsterEvent.engine_event(
            step_handler_context.get_step_context(step_key),
            message=f"Stopping KubeRay job {namespace}/{ray_job_name} for step",
            event_specific_data=EngineEventData(),
        )

        self.client.terminate(name=ray_job_name, namespace=namespace, port_forward=False)
