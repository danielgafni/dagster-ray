import time

import dagster as dg

from dagster_ray.core.executor import ray_executor


class MyConfig(dg.Config):
    sleep_for: int = 3


@dg.op
def return_one(context: dg.OpExecutionContext, config: MyConfig) -> int:
    context.log.debug(f"sleeping for {config.sleep_for} seconds...")
    time.sleep(config.sleep_for)

    return 1


@dg.op
def return_two(context: dg.OpExecutionContext, config: MyConfig) -> int:
    context.log.info(f"sleeping for {config.sleep_for} seconds...")
    time.sleep(config.sleep_for)
    context.log.info("Waking up!")

    return 2


@dg.op
def sum_one_and_two(a: int, b: int) -> int:
    res = a + b

    assert res == 3

    return res


@dg.job(executor_def=ray_executor)
def my_job():
    return_two_result = return_two()
    return_one_result = return_one()
    sum_one_and_two(return_one_result, return_two_result)


definitions = dg.Definitions(
    jobs=[my_job],
)
