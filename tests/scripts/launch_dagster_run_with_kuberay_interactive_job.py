#!/usr/bin/env python3
"""
Script for testing process interrupt cleanup behavior.
This script creates a KubeRayInteractiveJob that will hang due to invalid image,
allowing the parent test to interrupt it and verify cleanup behavior.
"""

import argparse
import json
import sys

import dagster as dg

from dagster_ray._base.resources import Lifecycle
from dagster_ray.kuberay import KubeRayInteractiveJob
from dagster_ray.kuberay.client.rayjob.client import RayJobClient
from dagster_ray.kuberay.configs import RayClusterSpec
from dagster_ray.kuberay.resources.rayjob import InteractiveRayJobConfig, InteractiveRayJobSpec


def parse_args():
    parser = argparse.ArgumentParser(description="Test script for process interrupt cleanup")
    parser.add_argument("--config-file", required=True, help="Path to kubeconfig file")
    parser.add_argument("--context", required=True, help="Kubernetes context name")
    parser.add_argument("--image", required=True, help="Docker image name")
    parser.add_argument("--cleanup", required=True, help="Cleanup behavior")
    parser.add_argument("--redis-port", type=int, required=True, help="Redis port number")
    parser.add_argument("--namespace", required=True, help="Kubernetes namespace")
    parser.add_argument("--head-group-spec", required=True, help="Head group spec as JSON string")
    parser.add_argument("--worker-group-specs", required=True, help="Worker group specs as JSON string")
    return parser.parse_args()


def main():
    args = parse_args()

    # Parse JSON arguments
    try:
        head_group_spec = json.loads(args.head_group_spec)
        worker_group_specs = json.loads(args.worker_group_specs)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON arguments: {e}", file=sys.stderr)
        sys.exit(1)

    # Initialize the client and resource
    client = RayJobClient(config_file=args.config_file, context=args.context)

    interactive_rayjob = KubeRayInteractiveJob(
        image=args.image,  # Invalid image that will cause hanging
        client=client,
        redis_port=args.redis_port,
        ray_job=InteractiveRayJobConfig(
            metadata={"namespace": args.namespace},
            spec=InteractiveRayJobSpec(
                ray_cluster_spec=RayClusterSpec(head_group_spec=head_group_spec, worker_group_specs=worker_group_specs),
            ),
        ),
        lifecycle=Lifecycle(create=True, wait=True, connect=False, cleanup=args.cleanup),
        timeout=300,  # Large timeout
    )

    @dg.asset
    def hanging_asset(interactive_rayjob: KubeRayInteractiveJob) -> None:
        return

    res = dg.materialize(
        assets=[hanging_asset],
        resources={"interactive_rayjob": interactive_rayjob},
        raise_on_error=False,
    )

    # we print the run id so the caller can use it to filter by RayJob `dagster/run-id` label in kubernetes
    print(res.run_id)


if __name__ == "__main__":
    main()
