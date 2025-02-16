from dagster import ExecuteInProcessResult


def get_saved_path(result: ExecuteInProcessResult, asset_name: str) -> str:
    path = (
        next(filter(lambda evt: evt.is_handled_output, result.events_for_node(asset_name)))
        .event_specific_data.metadata["path"]  # type: ignore
        .value
    )  # type: ignore[index,union-attr]
    assert isinstance(path, str)
    return path
