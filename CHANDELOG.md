# Changelog

All notable user-facing changes to `dagster-ray` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

- added `enable_legacy_debugger` configuration parameter to subclasses of `RayResource`

## Fixes
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
