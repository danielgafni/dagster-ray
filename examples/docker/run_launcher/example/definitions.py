from dagster import Definitions, job

from dagster_ray import RayIOManager, ray_executor
from example.ops import inspect_files, run_ray_compute, sum_two_values

configured_ray_executor = ray_executor.configured(
    {"ray": {"env_vars": ["DAGSTER_POSTGRES_USER", "DAGSTER_POSTGRES_PASSWORD", "DAGSTER_POSTGRES_DB"]}}
)


@job(tags={"dagster-ray/config": {"num_cpus": 0.5}}, executor_def=configured_ray_executor)
def my_job():
    sum_two_values(run_ray_compute(), inspect_files())


definitions = Definitions(jobs=[my_job], resources={"io_manager": RayIOManager()}, executor=configured_ray_executor)
