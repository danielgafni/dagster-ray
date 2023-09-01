# from pathlib import Path
#
# import pytest
# from dagster import materialize, asset
# import ray
# from ray import ObjectRef
#
# from dagster_ray.resource import RayResource
# from dagster_ray.io_managers import RayIOManager, RayRefIOManager
#
#
# @pytest.fixture(scope="session")
# def ray_refs_dir(tmp_path_factory) -> Path:
#     return tmp_path_factory.mktemp("ray_refs_store")
#
#
# @pytest.fixture(scope="session")
# def ray_io_manager(ray_refs_dir: Path) -> RayIOManager:
#     return RayIOManager(
#         base_dir=str(ray_refs_dir),
#         ray=RayResource()
# )
#
#
# @pytest.fixture(scope="session")
# def ray_ref_io_manager(ray_refs_dir) -> RayRefIOManager:
#     return RayRefIOManager(
#         base_dir=str(ray_refs_dir),
#         ray=RayResource()
# )
#
#
# def test_ray_io_manager(ray_io_manager: RayResource):
#     @asset(
#         io_manager_def=ray_io_manager,
#     )
#     def upstream() -> int:
#         return 0
#
#
#     @asset
#     def downstream(upstream: int) -> None:
#         assert upstream == 0
#
#     materialize([upstream, downstream])
#
#
# def test_ray_ref_io_manager(ray_ref_io_manager: RayRefIOManager):
#     @asset(
#         io_manager_def=ray_ref_io_manager,
#     )
#     def upstream() -> int:
#         return 0
#
#     @asset
#     def downstream(upstream: ObjectRef) -> None:
#         assert isinstance(upstream, ObjectRef)
#         assert ray.get(upstream) == 0
