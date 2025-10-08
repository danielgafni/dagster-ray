import dagster as dg
from packaging.version import Version

if Version(dg.__version__) < Version("1.11.6"):
    from dagster._core.remote_representation import CodeLocationOrigin, InProcessCodeLocationOrigin
else:
    from dagster._core.remote_origin import InProcessCodeLocationOrigin, CodeLocationOrigin  # pyright: ignore[reportMissingImports,reportUnusedImport]  # noqa: F401,I001

assert InProcessCodeLocationOrigin is not None
assert CodeLocationOrigin is not None


def get_saved_path(result: dg.ExecuteInProcessResult, asset_name: str) -> str:
    path = (
        list(filter(lambda evt: evt.is_handled_output, result.events_for_node(asset_name)))[0]
        .event_specific_data.metadata["path"]  # type: ignore
        .value
    )  # type: ignore[index,union-attr]
    assert isinstance(path, str)
    return path


__all__ = ["CodeLocationOrigin", "InProcessCodeLocationOrigin", "get_saved_path"]
