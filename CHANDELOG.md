# Changelog

All notable user-facing changes to `dagster-ray` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.2.1

### Fixes

- Fixed broken wheel on PyPI

## 0.2.0

### Changed
- `KubeRayInteractiveJob.deletion_strategy` now defaults to `DeleteCluster` for both successful and failed executions. This is a reasonable default for the use case.
- `KubeRayInteractiveJob.ttl_seconds_after_finished` now defaults to `600` seconds.
- `KubeRayCluster.lifecycle.cleanup` now defaults to `always`
- [:bomb: breaking] `RayJob` and `RayCluster` clients and resources Kubernetes init parameters have been renamed to `kube_config` and `kube_context`.

### Added
- new `enable_legacy_debugger` configuration parameter to subclasses of `RayResource`
- new `on_exception` option for `lifecycle.cleanup` policy. It's triggered during resource setup/cleanup (including `KeyboardInterrupt`), but not by user `@op`/`@asset` code.
- `KubeRayInteractiveJob` now respects `lifecycle.cleanup`. It defaults to `on_exception`. Users are advised to rely on built-in `RayJob` cleanup mechanisms, such as `ttlSecondsAfterFinished` and `deletionStrategy`.

### Fixes
- removed `ignore_reinit_error` from `RayResource` init options: it's potentially dangerous, for example in case the user has accidentally connected to another Ray cluster (including local ray) before initializing the resource.

## 0.1.0

### Changed
- [:bomb: breaking] `RayResource`: top-level `skip_init` and `skip_setup` configuration parameters have been removed. The `lifecycle` field is the new way of configuring steps performed during resource initialization. `KubeRayCluster`'s `skip_cleanup` has been moved to `lifecycle` as well.
- [:bomb: breaking] injected `dagster.io/run_id` Kubernetes label has been renamed to `dagster/run-id`. Keys starting with `dagster.io/` have been converted to `dagster/` to match how `dagster-k8s` does it.
- [:bomb: breaking] `dagster_ray.kuberay` Configurations have been unified with KubeRay APIs.
- `dagster-ray` now populates Kubernetes labels with more values (including some useful Dagster Cloud values such as `git-sha`)

### Added
- `KubeRayInteractiveJob` -- a new resource that utililizes the new `InteractiveMode` for `RayJob`. It can be used to connect to Ray in Client mode -- like `KubeRayCluster` -- but gives access to `RayJob` features, such as automatic cleanup (`ttlSecondsAfterFinished`), retries (`backoffLimit`) and timeouts (`activeDeadlineSeconds`).
- `RayResource` setup lifecycle has been overhauled: resources now has an `actions` parameter with 3 configuration options: `create`, `wait` and `connect`. The user can disable them and run `.create()`, `.wait()` and `.connect()` manually if needed.
