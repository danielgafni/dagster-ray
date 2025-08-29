# Changelog

All notable user-facing changes to `dagster-ray` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- [:bomb: breaking] injected `dagster.io/run_id` Kubernetes label has been renamed to `dagster.io/run-id`. Keys starting with `dagster/` have been converted to `dagster.io/`.
- [:bomb: breaking] `dagster_ray.kuberay` Configurations have been unified with KubeRay APIs.
- `dagster-ray` now populates Kubernetes labels with more values (including some useful Dagster Cloud values such as `git-sha`)

### Added
- `KubeRayInteractiveJob` -- a new resource that utililizes the new `InteractiveMode` for `RayJob`. It can be used to connect to Ray in Client mode -- like `KubeRayCluster` -- but gives access to `RayJob` features, such as automatic cleanup (`ttlSecondsAfterFinished`), retries (`backoffLimit`) and timeouts (`activeDeadlineSeconds`).
- `RayResource` resources now have a `skip_setup` parameter that can be used to lazily postpone creation of ray clusters. The user can manually create the ray cluster inside the Dagster op when (if) needed.
