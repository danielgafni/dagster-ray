from __future__ import annotations

import copy
from typing import Any
from unittest.mock import MagicMock

import dagster as dg
import pytest
import yaml

from dagster_ray.core.pipes import SubmitJobParams
from dagster_ray.kuberay.pipes import (
    PipesKubeRayJobClient,
    _merge_submit_params_into_ray_job,
    _ray_job_from_submit_params,
    _submit_params_from_ray_job,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_submit_job_params() -> SubmitJobParams:
    return SubmitJobParams(entrypoint="python main.py")


@pytest.fixture
def full_submit_job_params() -> SubmitJobParams:
    return SubmitJobParams(
        entrypoint="python train.py --epochs 10",
        runtime_env={"pip": ["torch", "numpy"], "env_vars": {"WANDB_KEY": "secret"}},
        entrypoint_num_cpus=2.0,
        entrypoint_num_gpus=1.0,
    )


@pytest.fixture
def minimal_ray_job_spec() -> dict[str, Any]:
    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayJob",
        "metadata": {"namespace": "default"},
        "spec": {
            "entrypoint": "python old_script.py",
            "shutdownAfterJobFinishes": True,
            "rayClusterSpec": {
                "headGroupSpec": {
                    "template": {"spec": {"containers": [{"name": "head", "image": "ray:latest"}]}},
                },
                "workerGroupSpecs": [
                    {
                        "template": {"spec": {"containers": [{"name": "worker", "image": "ray:latest"}]}},
                    }
                ],
            },
        },
    }


@pytest.fixture
def ray_job_spec_with_runtime_env(minimal_ray_job_spec: dict[str, Any]) -> dict[str, Any]:
    spec = copy.deepcopy(minimal_ray_job_spec)
    spec["spec"]["runtimeEnvYAML"] = yaml.safe_dump({"pip": ["pandas"]})
    spec["spec"]["entrypointNumCpus"] = 4
    spec["spec"]["entrypointNumGpus"] = 2
    return spec


@pytest.fixture
def mock_context() -> MagicMock:
    """A mock Dagster execution context with configurable run tags."""
    context = MagicMock(spec=dg.AssetExecutionContext)
    run = MagicMock()
    run.tags = {"dagster/image": "my-registry/my-image:v1"}
    context.run = run
    return context


@pytest.fixture
def mock_context_no_image() -> MagicMock:
    """A mock Dagster execution context without the dagster/image tag."""
    context = MagicMock(spec=dg.AssetExecutionContext)
    run = MagicMock()
    run.tags = {}
    context.run = run
    return context


# ---------------------------------------------------------------------------
# _submit_params_from_ray_job
# ---------------------------------------------------------------------------


class TestExtractSubmitJobParamsFromRayJob:
    def test_extracts_entrypoint(self, minimal_ray_job_spec: dict[str, Any]) -> None:
        params = _submit_params_from_ray_job(minimal_ray_job_spec)
        assert params["entrypoint"] == "python old_script.py"

    def test_extracts_runtime_env_yaml(self, ray_job_spec_with_runtime_env: dict[str, Any]) -> None:
        params = _submit_params_from_ray_job(ray_job_spec_with_runtime_env)
        assert params["runtime_env"] == {"pip": ["pandas"]}

    def test_extracts_num_cpus_and_gpus(self, ray_job_spec_with_runtime_env: dict[str, Any]) -> None:
        params = _submit_params_from_ray_job(ray_job_spec_with_runtime_env)
        assert params["entrypoint_num_cpus"] == 4.0
        assert params["entrypoint_num_gpus"] == 2.0

    def test_handles_missing_optional_fields(self, minimal_ray_job_spec: dict[str, Any]) -> None:
        params = _submit_params_from_ray_job(minimal_ray_job_spec)
        assert "runtime_env" not in params
        assert "entrypoint_num_cpus" not in params
        assert "entrypoint_num_gpus" not in params

    def test_minimal_spec_only_entrypoint(self) -> None:
        spec = {"spec": {"entrypoint": "echo hello"}}
        params = _submit_params_from_ray_job(spec)
        assert params == {"entrypoint": "echo hello"}


# ---------------------------------------------------------------------------
# _merge_submit_params_into_ray_job
# ---------------------------------------------------------------------------


class TestApplySubmitJobParamsToRayJob:
    def test_overrides_entrypoint(
        self,
        minimal_ray_job_spec: dict[str, Any],
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _merge_submit_params_into_ray_job(minimal_ray_job_spec, minimal_submit_job_params)
        assert result["spec"]["entrypoint"] == "python main.py"

    def test_overrides_runtime_env_yaml(
        self,
        minimal_ray_job_spec: dict[str, Any],
        full_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _merge_submit_params_into_ray_job(minimal_ray_job_spec, full_submit_job_params)
        parsed = yaml.safe_load(result["spec"]["runtimeEnvYAML"])
        assert parsed["pip"] == ["torch", "numpy"]
        assert parsed["env_vars"] == {"WANDB_KEY": "secret"}

    def test_sets_num_cpus_and_gpus(
        self,
        minimal_ray_job_spec: dict[str, Any],
        full_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _merge_submit_params_into_ray_job(minimal_ray_job_spec, full_submit_job_params)
        assert result["spec"]["entrypointNumCpus"] == 2.0
        assert result["spec"]["entrypointNumGpus"] == 1.0

    def test_does_not_modify_original(
        self,
        minimal_ray_job_spec: dict[str, Any],
        full_submit_job_params: SubmitJobParams,
    ) -> None:
        original_entrypoint = minimal_ray_job_spec["spec"]["entrypoint"]
        _merge_submit_params_into_ray_job(minimal_ray_job_spec, full_submit_job_params)
        assert minimal_ray_job_spec["spec"]["entrypoint"] == original_entrypoint
        assert "runtimeEnvYAML" not in minimal_ray_job_spec["spec"]

    def test_partial_params_only_entrypoint(
        self,
        minimal_ray_job_spec: dict[str, Any],
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _merge_submit_params_into_ray_job(minimal_ray_job_spec, minimal_submit_job_params)
        assert result["spec"]["entrypoint"] == "python main.py"
        assert "runtimeEnvYAML" not in result["spec"]
        assert "entrypointNumCpus" not in result["spec"]
        assert "entrypointNumGpus" not in result["spec"]

    def test_preserves_other_spec_fields(
        self,
        minimal_ray_job_spec: dict[str, Any],
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _merge_submit_params_into_ray_job(minimal_ray_job_spec, minimal_submit_job_params)
        assert result["spec"]["shutdownAfterJobFinishes"] is True
        assert "rayClusterSpec" in result["spec"]


# ---------------------------------------------------------------------------
# _ray_job_from_submit_params
# ---------------------------------------------------------------------------


class TestBuildMinimalRayJob:
    def test_generates_valid_manifest_structure(
        self,
        mock_context: MagicMock,
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _ray_job_from_submit_params(mock_context, minimal_submit_job_params)
        assert result["apiVersion"] == "ray.io/v1"
        assert result["kind"] == "RayJob"
        assert "metadata" in result
        assert "spec" in result

    def test_sets_entrypoint(
        self,
        mock_context: MagicMock,
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _ray_job_from_submit_params(mock_context, minimal_submit_job_params)
        assert result["spec"]["entrypoint"] == "python main.py"

    def test_sets_runtime_env_yaml(
        self,
        mock_context: MagicMock,
        full_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _ray_job_from_submit_params(mock_context, full_submit_job_params)
        parsed = yaml.safe_load(result["spec"]["runtimeEnvYAML"])
        assert parsed["pip"] == ["torch", "numpy"]
        assert parsed["env_vars"] == {"WANDB_KEY": "secret"}

    def test_sets_num_cpus_and_gpus(
        self,
        mock_context: MagicMock,
        full_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _ray_job_from_submit_params(mock_context, full_submit_job_params)
        assert result["spec"]["entrypointNumCpus"] == 2.0
        assert result["spec"]["entrypointNumGpus"] == 1.0

    def test_omits_runtime_env_when_not_provided(
        self,
        mock_context: MagicMock,
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _ray_job_from_submit_params(mock_context, minimal_submit_job_params)
        assert "runtimeEnvYAML" not in result["spec"]

    def test_omits_num_cpus_and_gpus_when_not_provided(
        self,
        mock_context: MagicMock,
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _ray_job_from_submit_params(mock_context, minimal_submit_job_params)
        assert "entrypointNumCpus" not in result["spec"]
        assert "entrypointNumGpus" not in result["spec"]

    def test_uses_dagster_image_tag(
        self,
        mock_context: MagicMock,
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _ray_job_from_submit_params(mock_context, minimal_submit_job_params)
        head_image = result["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]["containers"][0]["image"]
        assert head_image == "my-registry/my-image:v1"
        for worker_spec in result["spec"]["rayClusterSpec"]["workerGroupSpecs"]:
            worker_image = worker_spec["template"]["spec"]["containers"][0]["image"]
            assert worker_image == "my-registry/my-image:v1"

    def test_no_image_tag_raises_error(
        self,
        mock_context_no_image: MagicMock,
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        with pytest.raises(ValueError, match="Image is missing"):
            _ray_job_from_submit_params(mock_context_no_image, minimal_submit_job_params)

    def test_uses_custom_namespace(
        self,
        mock_context: MagicMock,
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _ray_job_from_submit_params(mock_context, minimal_submit_job_params, namespace="ml-jobs")
        assert result["metadata"]["namespace"] == "ml-jobs"

    def test_default_namespace(
        self,
        mock_context: MagicMock,
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _ray_job_from_submit_params(mock_context, minimal_submit_job_params)
        assert result["metadata"]["namespace"] == "default"

    def test_spec_has_shutdown_after_job_finishes(
        self,
        mock_context: MagicMock,
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _ray_job_from_submit_params(mock_context, minimal_submit_job_params)
        assert result["spec"]["shutdownAfterJobFinishes"] is True

    def test_includes_ray_cluster_spec(
        self,
        mock_context: MagicMock,
        minimal_submit_job_params: SubmitJobParams,
    ) -> None:
        result = _ray_job_from_submit_params(mock_context, minimal_submit_job_params)
        cluster_spec = result["spec"]["rayClusterSpec"]
        assert "headGroupSpec" in cluster_spec
        assert "workerGroupSpecs" in cluster_spec


# ---------------------------------------------------------------------------
# PipesKubeRayJobClient.run() validation
# ---------------------------------------------------------------------------


class TestPipesKubeRayJobClientRunValidation:
    def test_raises_when_neither_params_nor_ray_job_provided(self) -> None:
        client = PipesKubeRayJobClient.__new__(PipesKubeRayJobClient)
        mock_context = MagicMock(spec=dg.AssetExecutionContext)

        with pytest.raises(
            dg.DagsterInvariantViolationError,
            match="Either `submit_job_params` or `ray_job` must be provided",
        ):
            client.run(context=mock_context)
