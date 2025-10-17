#!/usr/bin/env python3
"""
Script for testing Ray executor termination behavior.
This script creates a long-running job using the Ray executor,
allowing the parent test to interrupt it and verify termination behavior.
"""

import argparse
import time

import dagster as dg

from dagster_ray.core.executor import ray_executor


def parse_args():
    parser = argparse.ArgumentParser(description="Test script for Ray executor termination")
    parser.add_argument("--ray-address", required=True, help="Ray cluster address")
    parser.add_argument("--temp-dir", required=True, help="Temporary directory for DagsterInstance")
    parser.add_argument("--signal-file", required=True, help="File to create when job starts running")
    return parser.parse_args()


@dg.op
def long_running_op(context: dg.OpExecutionContext) -> None:
    """Op that runs for a long time to allow termination testing."""
    import os

    # Create signal file to notify parent process that job has started
    signal_file = os.environ.get("SIGNAL_FILE")
    if signal_file:
        with open(signal_file, "w") as f:
            f.write("started")
        context.log.info(f"Created signal file: {signal_file}")

    # Run for a long time to ensure parent process can send termination signal
    for i in range(300):  # 5 minutes
        time.sleep(1)
        if i % 10 == 0:
            context.log.info(f"Still running... iteration {i}")


@dg.job(executor_def=ray_executor)
def long_running_job():
    long_running_op()


def main():
    args = parse_args()

    import os

    # Set DAGSTER_HOME so DagsterInstance.get() will create a persistent instance
    os.environ["DAGSTER_HOME"] = args.temp_dir

    # Create a temporary non-ephemeral instance for multi-process execution
    with dg.DagsterInstance.get() as instance:
        # Execute the job - pass signal file via runtime_env so it reaches the Ray job
        dg.execute_job(
            job=dg.reconstructable(long_running_job),
            instance=instance,
            run_config={
                "execution": {
                    "config": {
                        "ray": {
                            "address": args.ray_address,
                            "runtime_env": {
                                "env_vars": {
                                    "SIGNAL_FILE": args.signal_file,
                                }
                            },
                        },
                    }
                }
            },
            raise_on_error=False,
        )


if __name__ == "__main__":
    main()
