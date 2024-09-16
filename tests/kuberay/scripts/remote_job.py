from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as pipes:
    pipes.log.info("Hello from Ray!")
    assert pipes.get_extra("foo") == "bar"
    pipes.report_asset_materialization(
        metadata={"some_metric": {"raw_value": 0, "type": "int"}},
        data_version="alpha",
    )