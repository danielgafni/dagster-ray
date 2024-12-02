import os

import ray  # noqa
from dagster import OpExecutionContext, op


@ray.remote(num_cpus=4)
def my_func():
    print("hello from a remote function!")
    return 1


@op
def run_ray_compute(context: OpExecutionContext) -> int:
    result = ray.get(my_func.remote())

    context.log.info(f"result: {result}")

    return result


@op
def inspect_files(context: OpExecutionContext) -> int:
    context.log.info(f"Contents of example: {os.listdir('example')}")

    return len(os.listdir("example"))


@op
def sum_two_values(context: OpExecutionContext, a: int, b: int) -> int:
    res = a + b

    context.log.info(f"Sum is: {res}")

    return res
