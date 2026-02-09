# Changelog

All notable user-facing changes to `dagster-ray` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Added

- `worker_process_setup_hook` parameter of `RuntimeEnv` is now supported (only as a string module path)

#### Fixes

- fixed `env_vars` not being used by `KubeRayInteractiveJob`

## 0.4.1

### Added

- `RayCluster`'s head pod logs are now displayed on startup timeout or failure

### Fixes

- Prevent the `RayCluster` cleanup sensor from targeting clusters with `.metadata.ownerReferences` set.
- `address` config value can now be omitted for `ray_executor`, making it use Ray's default cluster address resolution. Thanks @cornettew!
- Fixed race condition with cluster sharing: previously multiple steps running in parallel could create different `RayCluster` instances at the same time (that were supposed to be shared). `dagster-ray` now uses Kubernetes [Lease](https://kubernetes.io/docs/concepts/architecture/leases/)-based leader election to coordinate shared cluster creation, which guarantees that only one of the running steps creates the shared `RayCluster`.
- `runtimeEnvYAML` now has all strings fully quoted which fixes passing values such as `1e-5` as `runtime_env` values. Thanks @JosefNagelschmidt!
- `ray_address` is now optional for `RunLauncherConfig`. Thanks @cornettew!

## 0.4.0

This release introduces a new feature that is very useful in dev environments: **Cluster Sharing**. Cluster sharing allows reusing existing `RayCluster` resources created by previous Dagster steps. It's implemented for `KubeRayCluster` Dagster resource. This feature enables faster iteration speed and reduced infrastructure costs (at the expense of job isolation). Therefore `KubeRayCluster` is now recommended over `KubeRayInteractiveJob` for use in **dev** environments.

Learn more in [Cluster Sharing docs](tutorial/kuberay.md/#cluster-sharing).

### Added
- `KubeRayCluster.cluster_sharing` parameter that controls cluster sharing behavior.
- `dagster_ray.kuberay.sensors.cleanup_expired_kuberay_clusters` sensor that cleans up expired clusters (both shared and non-shared). Learn more in [docs](api/kuberay.md#dagster_ray.kuberay.sensors.cleanup_expired_kuberay_clusters).
- `dagster-ray` entry now appears in the Dagster libraries list in the web UI.

### Changed
- [:bomb: breaking] - removed `cleanup_kuberay_clusters_op` and other associated definitions in favor of `dagster_ray.kuberay.sensors.cleanup_expired_kuberay_clusters` sensor that is more flexible.

## 0.3.1

### Added
- `failure_tolerance_timeout` configuration parameter for `KubeRayInteractiveJob` and `KubeRayCluster`. It can be set to a positive value to give the cluster some time to transition out of `failed` state (which can be transient in some scenarios) before raising an error.

### Fixes
- ensure both `.head.serviceIP` and `.head.serviceName` are set on the `RayCluster` while waiting for cluster readiness.

## 0.3.0

This release includes massive docs improvements and drops support for Python 3.9.

### Changes

- [:bomb: breaking] dropped Python 3.9 support (EOL October 2025).
- [internal] most of the general, backend-agnostic code has been moved to `dagster_ray.core` (top-level imports still work).

## 0.2.1

### Fixes

- Fixed broken wheel on PyPI.

## 0.2.0

### Changed
- `KubeRayInteractiveJob.deletion_strategy` now defaults to `DeleteCluster` for both successful and failed executions. This is a reasonable default for the use case.
- `KubeRayInteractiveJob.ttl_seconds_after_finished` now defaults to `600` seconds.
- `KubeRayCluster.lifecycle.cleanup` now defaults to `always`.
- [:bomb: breaking] `RayJob` and `RayCluster` clients and resources Kubernetes init parameters have been renamed to `kube_config` and `kube_context`.

### Added
- `enable_legacy_debugger` configuration parameter to subclasses of `RayResource`
- `on_exception` option for `lifecycle.cleanup` policy. It's triggered during resource setup/cleanup (including `KeyboardInterrupt`), but not by user `@op`/`@asset` code.
- `KubeRayInteractiveJob` now respects `lifecycle.cleanup`. It defaults to `on_exception`. Users are advised to rely on built-in `RayJob` cleanup mechanisms, such as `ttlSecondsAfterFinished` and `deletionStrategy`.

### Fixes
- removed `ignore_reinit_error` from `RayResource` init options: it's potentially dangerous, for example in case the user has accidentally connected to another Ray cluster (including local ray) before initializing the resource.

## 0.1.0

### Changed
- [:bomb: breaking] `RayResource`: top-level `skip_init` and `skip_setup` configuration parameters have been removed. The `lifecycle` field is the new way of configuring steps performed during resource initialization. `KubeRayCluster`'s `skip_cleanup` has been moved to `lifecycle` as well.
- [:bomb: breaking] injected `dagster.io/run_id` Kubernetes label has been renamed to `dagster/run-id`. Keys starting with `dagster.io/` have been converted to `dagster/` to match how `dagster-k8s` does it.
- [:bomb: breaking] `dagster_ray.kuberay` Configurations have been unified with KubeRay APIs.
- `dagster-ray` now populates Kubernetes labels with more values (including some useful Dagster Cloud values such as `git-sha`).

### Added
- `KubeRayInteractiveJob` -- a resource that utilizes the new `InteractiveMode` for `RayJob`. It can be used to connect to Ray in Client mode -- like `KubeRayCluster` -- but gives access to `RayJob` features, such as automatic cleanup (`ttlSecondsAfterFinished`), retries (`backoffLimit`) and timeouts (`activeDeadlineSeconds`).
- `RayResource` setup lifecycle has been overhauled: resources now has an `actions` parameter with 3 configuration options: `create`, `wait` and `connect`. The user can disable them and run `.create()`, `.wait()` and `.connect()` manually if needed.
