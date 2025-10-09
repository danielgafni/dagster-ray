# KubeRay API Reference

KubeRay integration components for running Ray on Kubernetes.  Learn how to use it [here](../tutorial/kuberay.md).

---

## Client Mode Resources

These resources initialize Ray client connection with a remote cluster.

::: dagster_ray.kuberay.KubeRayInteractiveJob
    options:
      members:
        - "__init__"
        - "lifecycle"
        - "ray_job"
        - "client"
        - "failure_tolerance_timeout"
        - "log_cluster_conditions"


::: dagster_ray.kuberay.KubeRayCluster
    options:
      members:
        - "__init__"
        - "cluster_sharing"
        - "lifecycle"
        - "ray_cluster"
        - "client"
        - "failure_tolerance_timeout"
        - "log_cluster_conditions"

---

## Pipes

::: dagster_ray.kuberay.PipesKubeRayJobClient
    options:
      members:
        - "__init__"
        - "run"

---

## Configuration and Types

::: dagster_ray.kuberay.configs.RayJobConfig
    options:
      members:
        - "metadata"
        - "spec"
        - "to_k8s"

::: dagster_ray.kuberay.configs.RayJobSpec
    options:
      members:
        - "entrypoint"
        - "runtime_env"
        - "ray_cluster_spec"
        - "entrypoint_num_cpus"
        - "entrypoint_num_gpus"
        - "entrypoint_memory"
        - "entrypoint_resources"
        - "to_k8s"

---

::: dagster_ray.kuberay.resources.rayjob.InteractiveRayJobConfig
    options:
      members:
        - "metadata"
        - "spec"
        - "to_k8s"

::: dagster_ray.kuberay.resources.rayjob.InteractiveRayJobSpec
    options:
      inherited_members: true
      members: true

---

::: dagster_ray.kuberay.configs.RayClusterConfig
    options:
      members:
        - "metadata"
        - "spec"
        - "to_k8s"

::: dagster_ray.kuberay.configs.RayClusterSpec
    options:
      inherited_members: true
      members: true

::: dagster_ray.kuberay.configs.MatchDagsterLabels
    options:
      inherited_members: true
      members: true

::: dagster_ray.kuberay.configs.ClusterSharing
    options:
      inherited_members: true
      members: true

--

::: dagster_ray.kuberay.resources.base.BaseKubeRayResourceConfig
    options:
      members:
        - "image"
        - "deployment_name"
        - "poll_interval"

---

## Resources

::: dagster_ray.kuberay.KubeRayJobClientResource
    options:
      members:
        - "__init__"

::: dagster_ray.kuberay.KubeRayClusterClientResource
    options:
      members:
        - "__init__"

---

## Sensors

::: dagster_ray.kuberay.sensors.cleanup_expired_kuberay_clusters

A Dagster sensor that monitors shared `RayCluster` resources created by the current code location and submits jobs to delete clusters that have expired.

Selects clusters based on the following labels:
    - `dagster/cluster-sharing=true`
    - `dagster/code-location=<current-code-location>`

By default it monitors the `ray` namespace. This can be configured by setting `DAGSTER_RAY_NAMESPACES` (accepts a comma-separated list of namespaces).

---

## Kubernetes API Clients

::: dagster_ray.kuberay.client.RayClusterClient
    options:
      members:
        - "__init__"
        - "create"
        - "delete"
        - "get"
        - "list"
        - "update"

::: dagster_ray.kuberay.client.RayJobClient
    options:
      members:
        - "__init__"
        - "create"
        - "delete"
        - "get"
        - "list"
        - "update"
