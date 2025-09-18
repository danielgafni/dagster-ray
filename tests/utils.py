import dagster as dg


def get_saved_path(result: dg.ExecuteInProcessResult, asset_name: str) -> str:
    path = (
        list(filter(lambda evt: evt.is_handled_output, result.events_for_node(asset_name)))[0]
        .event_specific_data.metadata["path"]  # type: ignore
        .value
    )  # type: ignore[index,union-attr]
    assert isinstance(path, str)
    return path
