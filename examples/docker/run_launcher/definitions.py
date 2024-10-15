import time

from dagster import Config, Definitions, OpExecutionContext, job, op

from dagster_ray import RayIOManager


class MyConfig(Config):
    sleep_for: int = 3


@op
def return_one(context: OpExecutionContext, config: MyConfig) -> int:
    context.log.info(f"sleeping for {config.sleep_for} seconds...")
    time.sleep(config.sleep_for)
    context.log.info("Waking up!")

    return 1


@op
def return_two(context: OpExecutionContext, config: MyConfig) -> int:
    context.log.info(f"sleeping for {config.sleep_for} seconds...")
    time.sleep(config.sleep_for)
    context.log.info("Waking up!")

    import os

    context.log.info(str(os.listdir(".")))

    return 2


@op
def sum_one_and_two(a: int, b: int) -> int:
    res = a + b

    assert res == 3

    return res


@job(tags={"dagster-ray/config": {"num_cpus": 0.5}})
def my_job():
    return_two_result = return_two()
    return_one_result = return_one()
    sum_one_and_two(return_one_result, return_two_result)


definitions = Definitions(jobs=[my_job], resources={"io_manager": RayIOManager()})
