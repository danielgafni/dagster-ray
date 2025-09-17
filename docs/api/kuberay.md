# KubeRay API Reference

KubeRay integration components for running Ray on Kubernetes.

::: dagster_ray.kuberay
    options:
      show_docstring_classes: true

## Client Mode Resources

These resources initialize Ray client connection with a remote cluster.

::: dagster_ray.kuberay.KubeRayInteractiveJob
    options:
      members:
        - "__init__"
        - "lifecycle"
        - "ray_job"
        - "client"
        - "log_cluster_conditions"


::: dagster_ray.kuberay.KubeRayCluster
    options:
      members:
        - "__init__"
        - "lifecycle"
        - "ray_cluster"
        - "client"
        - "log_cluster_conditions"

## Job Submission Resources

These resources submit Ray jobs to a remote cluster.

::: dagster_ray.kuberay.PipesKubeRayJobClient
    options:
      members:
        - "__init__"
        - "run"

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

--

::: dagster_ray.kuberay.resources.base.BaseKubeRayResourceConfig
    options:
      members:
        - "image"
        - "deployment_name"
        - "poll_interval"

## Resources

::: dagster_ray.kuberay.KubeRayJobClientResource
    options:
      members:
        - "__init__"

::: dagster_ray.kuberay.KubeRayClusterClientResource
    options:
      members:
        - "__init__"

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
