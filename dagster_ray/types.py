from typing import Union

from dagster import AssetExecutionContext, InitResourceContext, OpExecutionContext
from typing_extensions import TypeAlias

DagsterExecutionContext: TypeAlias = Union[OpExecutionContext, AssetExecutionContext]

AnyDagsterContext: TypeAlias = Union[InitResourceContext, DagsterExecutionContext]
