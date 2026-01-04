from __future__ import annotations

from typing import TYPE_CHECKING

import dagster as dg

DAGSTER_RAY_OBJECT_MAP_NAME = "DagsterRayObjectMap"
DAGSTER_RAY_NAMESPACE = "dagster-ray"

# we need to create a global Ray actor which will store all the refs to all objcets

if TYPE_CHECKING:
    import ray


class RayObjectMap:
    # TODO: implement some eventual cleanup mechanism
    # idea: save creation timestamp and periodically check for old refs
    # or add some integration with the RunLauncher/Executor
    def __init__(self):
        self._object_map: dict[str, ray.ObjectRef] = {}

    def set(self, key: str, ref: ray.ObjectRef):
        self._object_map[key] = ref

    def get(self, key: str) -> ray.ObjectRef | None:
        return self._object_map.get(key)

    def delete(self, key: str):
        if key in self._object_map:
            del self._object_map[key]

    def keys(self):
        return self._object_map.keys()

    def ping(self):
        return "pong"

    @staticmethod
    def get_or_create():
        import ray

        actor = (
            ray.remote(RayObjectMap)
            .options(  # type: ignore
                name=DAGSTER_RAY_OBJECT_MAP_NAME,
                namespace=DAGSTER_RAY_NAMESPACE,
                get_if_exists=True,
                lifetime="detached",
                # max_restarts=-1,
                max_concurrency=1000,  # TODO: make this configurable,
                runtime_env={"RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": "1"},
            )
            .remote()
        )

        # make sure the actor is created
        ray.get(actor.ping.remote())  # type: ignore

        return actor


class RayIOManager(dg.ConfigurableIOManager):
    """IO Manager that stores intermediate values in Ray's object store.

    The RayIOManager allows storing and retrieving intermediate values in Ray's distributed
    object store, making it ideal for use with RayRunLauncher and ray_executor. It works by
    storing Dagster step keys in a global Ray actor that maintains a mapping between step
    keys and Ray ObjectRefs.

    Attributes:
        address (str | None): Ray cluster address. If provided, will initialize Ray connection.
            If None, assumes Ray is already initialized.

    Example:
        Basic usage
        ```python
        import dagster as dg
        from dagster_ray import RayIOManager

        @dg.asset(io_manager_key="ray_io_manager")
        def upstream() -> int:
            return 42

        @dg.asset
        def downstream(upstream: int):
            return upstream * 2

        definitions = dg.Definitions(
            assets=[upstream, downstream],
            resources={"ray_io_manager": RayIOManager()}
        )
        ```

    Example:
        With Ray cluster address
        ```python
        ray_io_manager = RayIOManager(address="ray://head-node:10001")
        ```

    Info:
        - Works with picklable Python objects
        - Supports partitioned assets and partition mappings
        - Uses Ray's automatic object movement for fault tolerance
        - Objects are stored with the Ray actor as owner for lifecycle management
    """

    address: str | None = None

    def handle_output(self, context: dg.OutputContext, obj):
        import ray

        # Only call ray.init() if an explicit address is provided.
        # When running inside a Ray job (via ray_executor), Ray is already
        # connected and we shouldn't try to reinitialize.
        if self.address:
            ray.init(self.address, ignore_reinit_error=True)

        object_map = RayObjectMap.get_or_create()

        storage_key = self._get_single_key(context)

        # TODO: understand if Ray will automatically move the object from dying nodes
        # what if not?

        ref = ray.put(obj, _owner=object_map)

        object_map.set.remote(storage_key, ref)  # type: ignore

        context.log.debug(f"[RayIOManager] Stored object with key {storage_key} as {ref}")

    def load_input(self, context: dg.InputContext):
        import ray

        # Only call ray.init() if an explicit address is provided.
        # When running inside a Ray job (via ray_executor), Ray is already
        # connected and we shouldn't try to reinitialize.
        if self.address:
            ray.init(self.address, ignore_reinit_error=True)

        object_map = RayObjectMap.get_or_create()

        if context.has_asset_partitions and len(context.asset_partition_keys) > 1:
            # load multiple partitions as once
            # first, get the refs

            storage_keys = self._get_multiple_keys(context)
            refs = [object_map.get.remote(key) for key in storage_keys.values()]  # type: ignore
            values = ray.get(refs)
            return {partition_key: value for partition_key, value in zip(storage_keys.keys(), values)}

        else:
            storage_key = self._get_single_key(context)

        context.log.debug(f"[RayIOManager] Loading object with key {storage_key}")

        ref = object_map.get.remote(storage_key)  # type: ignore

        assert ref is not None, f"[RayIOManager] Object with key {storage_key} not found in RayObjectMap"

        return ray.get(ref)

    def _get_single_key(self, context: dg.InputContext | dg.OutputContext) -> str:
        identifier = context.get_identifier() if not context.has_asset_key else context.get_asset_identifier()
        return "/".join(identifier)

    def _get_multiple_keys(self, context: dg.InputContext) -> dict[str, str]:
        if context.has_asset_key:
            asset_path = list(context.asset_key.path)

            return {
                partition_key: "/".join(asset_path + [partition_key]) for partition_key in context.asset_partition_keys
            }
        else:
            raise RuntimeError(
                "[RayIOManager] This method can only be called with an InputContext that has multiple partitions"
            )
