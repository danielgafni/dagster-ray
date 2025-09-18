from typing import Union

import dagster as dg
from typing_extensions import TypeAlias

OpOrAssetExecutionContext: TypeAlias = Union[dg.OpExecutionContext, dg.AssetExecutionContext]

AnyDagsterContext: TypeAlias = Union[dg.OpExecutionContext, dg.AssetExecutionContext, dg.InitResourceContext]
