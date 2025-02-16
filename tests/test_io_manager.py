from dagster import AssetExecutionContext, StaticPartitionsDefinition, asset, materialize

from dagster_ray import RayIOManager


def test_ray_io_manager():
    @asset
    def upstream():
        return 1

    @asset
    def downstream(upstream) -> None:
        assert upstream == 1

    materialize(
        [upstream, downstream],
        resources={"io_manager": RayIOManager()},
    )


def test_ray_io_manager_partitioned():
    partitions_def = StaticPartitionsDefinition(partition_keys=["A", "B", "C"])

    @asset(partitions_def=partitions_def)
    def upsteram_partitioned(context: AssetExecutionContext) -> str:
        return context.partition_key.lower()

    @asset(partitions_def=partitions_def)
    def downstream_partitioned(context: AssetExecutionContext, upsteram_partitioned: str) -> None:
        assert upsteram_partitioned == context.partition_key.lower()

    for partition_key in ["A", "B", "C"]:
        materialize(
            [upsteram_partitioned, downstream_partitioned],
            resources={"io_manager": RayIOManager()},
            partition_key=partition_key,
        )


def test_ray_io_manager_partition_mapping():
    partitions_def = StaticPartitionsDefinition(partition_keys=["A", "B", "C"])

    @asset(partitions_def=partitions_def)
    def upsteram_partitioned(context: AssetExecutionContext) -> str:
        return context.partition_key.lower()

    @asset
    def downstream_non_partitioned(upsteram_partitioned: dict[str, str]) -> None:
        assert upsteram_partitioned == {"A": "a", "B": "b", "C": "c"}

    for partition_key in ["A", "B", "C"]:
        materialize([upsteram_partitioned], resources={"io_manager": RayIOManager()}, partition_key=partition_key)

    materialize(
        [upsteram_partitioned.to_source_asset(), downstream_non_partitioned], resources={"io_manager": RayIOManager()}
    )
