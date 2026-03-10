from dagster._core.libraries import DagsterLibraryRegistry

from dagster_ray._base.resources import RayResource
from dagster_ray._version import __version__
from dagster_ray.configs import Lifecycle
from dagster_ray.core.executor import ray_executor
from dagster_ray.core.io_manager import RayIOManager
from dagster_ray.core.pipes import PipesRayJobClient, PipesRayJobMessageReader
from dagster_ray.core.resources import LocalRay
from dagster_ray.core.run_launcher import RayRunLauncher

DagsterLibraryRegistry.register("dagster-ray", __version__, is_dagster_package=False)

__all__ = [
    "Lifecycle",
    "RayResource",
    "RayRunLauncher",
    "RayIOManager",
    "ray_executor",
    "PipesRayJobMessageReader",
    "PipesRayJobClient",
    "LocalRay",
    "__version__",
]
