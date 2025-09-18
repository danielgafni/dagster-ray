import dagster as dg

from dagster_ray import RayIOManager


def test_ray_io_manager():
    @dg.asset
    def upstream():
        return 1

    @dg.asset
    def downstream(upstream) -> None:
        assert upstream == 1

    dg.materialize(
        [upstream, downstream],
        resources={"io_manager": RayIOManager()},
    )


def test_ray_io_manager_partitioned():
    partitions_def = dg.StaticPartitionsDefinition(partition_keys=["A", "B", "C"])

    @dg.asset(partitions_def=partitions_def)
    def upsteram_partitioned(context: dg.AssetExecutionContext) -> str:
        return context.partition_key.lower()

    @dg.asset(partitions_def=partitions_def)
    def downstream_partitioned(context: dg.AssetExecutionContext, upsteram_partitioned: str) -> None:
        assert upsteram_partitioned == context.partition_key.lower()

    for partition_key in ["A", "B", "C"]:
        dg.materialize(
            [upsteram_partitioned, downstream_partitioned],
            resources={"io_manager": RayIOManager()},
            partition_key=partition_key,
        )


def test_ray_io_manager_partition_mapping():
    partitions_def = dg.StaticPartitionsDefinition(partition_keys=["A", "B", "C"])

    @dg.asset(partitions_def=partitions_def)
    def upsteram_partitioned(context: dg.AssetExecutionContext) -> str:
        return context.partition_key.lower()

    @dg.asset
    def downstream_non_partitioned(upsteram_partitioned: dict[str, str]) -> None:
        assert upsteram_partitioned == {"A": "a", "B": "b", "C": "c"}

    for partition_key in ["A", "B", "C"]:
        dg.materialize([upsteram_partitioned], resources={"io_manager": RayIOManager()}, partition_key=partition_key)

    dg.materialize(
        [upsteram_partitioned.to_source_asset(), downstream_non_partitioned], resources={"io_manager": RayIOManager()}
    )
