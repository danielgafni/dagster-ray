# import pickle
# from typing import Any
#
# import ray
#
# from dagster import UPathIOManager, InputContext, OutputContext, InitResourceContext, ConfigurableIOManager
# from pydantic import PrivateAttr
# from upath import UPath
#
# from dagster_ray.resource import RayResource
#
#
# class BaseRayIOManager(ConfigurableIOManager, UPathIOManager):
#     base_dir: str
#     extension: str = ".ray"
#     ray: RayResource
#
#     _base_path: UPath = PrivateAttr()
#
#     def setup_for_execution(self, context: InitResourceContext) -> None:
#         self._base_path = UPath(self.base_dir)
#
#     def dump_to_path(self, context: OutputContext, obj: Any, path: "UPath"):
#         ref = ray.put(obj)
#         path.write_bytes(pickle.dumps(ref))
#
#
# class RayRefIOManager(BaseRayIOManager):
#     def load_from_path(self, context: InputContext, path: "UPath") -> ray.ObjectRef:
#         ref = pickle.loads(path.read_bytes())
#         assert isinstance(ref, ray.ObjectRef)
#         return ref
#
# #
# # class RayIOManager(BaseRayIOManager):
# #     def load_from_path(self, context: InputContext, path: "UPath") -> Any:
# #         ref = pickle.loads(path.read_bytes())
# #         return ray.get(ref)
