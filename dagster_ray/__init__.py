from dagster_ray._base.resources import BaseRayResource
from dagster_ray.executor import ray_executor

RayResource = BaseRayResource


__all__ = ["RayResource", "ray_executor"]
