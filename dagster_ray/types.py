from typing import Union

import dagster as dg
from typing_extensions import TypeAlias

AnyDagsterContext: TypeAlias = Union[dg.OpExecutionContext, dg.AssetExecutionContext, dg.InitResourceContext]
