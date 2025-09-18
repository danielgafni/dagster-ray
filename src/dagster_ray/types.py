from typing import TypeAlias

import dagster as dg

OpOrAssetExecutionContext: TypeAlias = dg.OpExecutionContext | dg.AssetExecutionContext

AnyDagsterContext: TypeAlias = dg.OpExecutionContext | dg.AssetExecutionContext | dg.InitResourceContext
