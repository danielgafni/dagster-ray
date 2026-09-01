# Changelog

All notable user-facing changes to `dagster-ray` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## v0.4.7 (01-09-2026)

### :sparkles: Features

- support kuberay 1.7.0 ([#382](https://github.com/danielgafni/dagster-ray/pull/382) by [@danielgafni](https://github.com/danielgafni))

## v0.4.6 (29-07-2026)

### :sparkles: Features

- support `RayCluster.authOptions (KubeRay 1.6.0)` ([#372](https://github.com/danielgafni/dagster-ray/pull/372) by [@danielgafni](https://github.com/danielgafni))
- support `RayCluster.upgradeStrategy` (KubeRay 1.6.0) ([#371](https://github.com/danielgafni/dagster-ray/pull/371) by [@danielgafni](https://github.com/danielgafni))
- support `RayJob.preRunningDeadlineSeconds` (KubeRay 1.6.0) ([#369](https://github.com/danielgafni/dagster-ray/pull/369) by [@danielgafni](https://github.com/danielgafni))
- support Python 3.13 ([#366](https://github.com/danielgafni/dagster-ray/pull/366) by [@danielgafni](https://github.com/danielgafni))
- publish heartbeats for Dagster steps running on shared Ray clusters ([#364](https://github.com/danielgafni/dagster-ray/pull/364) by [@danielgafni](https://github.com/danielgafni))

### :bug: Bug Fixes

- avoid setting a default for `RayJob.deletionStrategy` ([#370](https://github.com/danielgafni/dagster-ray/pull/370) by [@danielgafni](https://github.com/danielgafni))
- swallow more transient connection errors ([#374](https://github.com/danielgafni/dagster-ray/pull/374) by [@danielgafni](https://github.com/danielgafni))
- correctly pass through extra manifest keys ([#368](https://github.com/danielgafni/dagster-ray/pull/368) by [@danielgafni](https://github.com/danielgafni))

### :hammer_and_wrench: Other Improvements

- release v0.4.6 ([#376](https://github.com/danielgafni/dagster-ray/pull/376) by [@danielgafni](https://github.com/danielgafni))
- add devenv profiles for different Python versions ([#367](https://github.com/danielgafni/dagster-ray/pull/367) by [@danielgafni](https://github.com/danielgafni))
- init dagger ([#365](https://github.com/danielgafni/dagster-ray/pull/365) by [@danielgafni](https://github.com/danielgafni))
- update ghcr.io/astral-sh/uv docker tag to v0.11.31 ([#361](https://github.com/danielgafni/dagster-ray/pull/361) by [@renovate[bot]](https://github.com/renovate[bot]))
- update dependency astral-sh/uv to v0.11.28 ([#360](https://github.com/danielgafni/dagster-ray/pull/360) by [@renovate[bot]](https://github.com/renovate[bot]))
- upgrade zensical to 0.0.37 ([#363](https://github.com/danielgafni/dagster-ray/pull/363) by [@danielgafni](https://github.com/danielgafni))
- enable uv integration in devenv ([#362](https://github.com/danielgafni/dagster-ray/pull/362) by [@danielgafni](https://github.com/danielgafni))

## v0.4.5 (29-05-2026)

### :bug: Bug Fixes

- preserve useful characters when truncating K8s label values ([#356](https://github.com/danielgafni/dagster-ray/pull/356) by [@peterroelants](https://github.com/peterroelants))

### :book: Docs

- add IPRally to happy users in README ([#351](https://github.com/danielgafni/dagster-ray/pull/351) by [@jrosti](https://github.com/jrosti))

### :hammer_and_wrench: Other Improvements

- release v0.4.5 ([#359](https://github.com/danielgafni/dagster-ray/pull/359) by [@danielgafni](https://github.com/danielgafni))
- update dependency astral-sh/uv to v0.11.16 ([#352](https://github.com/danielgafni/dagster-ray/pull/352) by [@renovate[bot]](https://github.com/renovate[bot]))
- update ghcr.io/astral-sh/uv docker tag to v0.11.8 ([#357](https://github.com/danielgafni/dagster-ray/pull/357) by [@renovate[bot]](https://github.com/renovate[bot]))
- update softprops/action-gh-release action to v2.6.2 ([#358](https://github.com/danielgafni/dagster-ray/pull/358) by [@renovate[bot]](https://github.com/renovate[bot]))
- update ghcr.io/astral-sh/uv docker tag to v0.11.6 ([#353](https://github.com/danielgafni/dagster-ray/pull/353) by [@renovate[bot]](https://github.com/renovate[bot]))
- switch to devenv ([#350](https://github.com/danielgafni/dagster-ray/pull/350) by [@danielgafni](https://github.com/danielgafni))
- change default branch to `main` (by [@danielgafni](https://github.com/danielgafni))

### :heart: New Contributors

- [@peterroelants](https://github.com/peterroelants) made their first contribution in [#356](https://github.com/danielgafni/dagster-ray/pull/356)
- [@jrosti](https://github.com/jrosti) made their first contribution in [#351](https://github.com/danielgafni/dagster-ray/pull/351)

## v0.4.4 (27-03-2026)

### :sparkles: Features

- add `create_cluster_if_needed` constructor argument to `PipesRayJobClient` ([#324](https://github.com/danielgafni/dagster-ray/pull/324) by [@ashutosh1807](https://github.com/ashutosh1807))
- add `submit_job_params` argument to `PipesKubeRayJobClient.run` ([#322](https://github.com/danielgafni/dagster-ray/pull/322) by [@ashutosh1807](https://github.com/ashutosh1807))

### :bug: Bug Fixes

- add _force_create_local_cluster workaround for broken create_cluster_if_needed ([#346](https://github.com/danielgafni/dagster-ray/pull/346) by [@ashutosh1807](https://github.com/ashutosh1807))
- ignore more k8s errors during RayCluster startup ([#344](https://github.com/danielgafni/dagster-ray/pull/344) by [@danielgafni](https://github.com/danielgafni))

### :hammer_and_wrench: Other Improvements

- release 0.4.4 ([#348](https://github.com/danielgafni/dagster-ray/pull/348) by [@danielgafni](https://github.com/danielgafni))
- add a section for other improvements to changelog ([#347](https://github.com/danielgafni/dagster-ray/pull/347) by [@danielgafni](https://github.com/danielgafni))
- improve Pipes imports and docs references ([#343](https://github.com/danielgafni/dagster-ray/pull/343) by [@danielgafni](https://github.com/danielgafni))
- upgrade zensical to 0.0.28 ([#342](https://github.com/danielgafni/dagster-ray/pull/342) by [@danielgafni](https://github.com/danielgafni))
- add minimal Claude Code config ([#341](https://github.com/danielgafni/dagster-ray/pull/341) by [@danielgafni](https://github.com/danielgafni))

## v0.4.3 (18-03-2026)

This release is focused on improving support for authentication and cross-cluster KubeRay workloads.

!!! note
    If you or your company are using `dagster-ray` in production, please consider adding yourself to the list [here](https://github.com/danielgafni/dagster-ray/blob/main/README.md#-who-is-using-dagster-ray) :)


### :sparkles: Features

- support `resolve_hostname` hook in `PipesKubeRayJobClient` ([#340](https://github.com/danielgafni/dagster-ray/pull/340) by [@danielgafni](https://github.com/danielgafni))
- add resolve_hostname hook to `KubeRayResource` ([#335](https://github.com/danielgafni/dagster-ray/pull/335) by [@danielgafni](https://github.com/danielgafni))
- support authOptions in RayClusterSpec ([#314](https://github.com/danielgafni/dagster-ray/pull/314) by [@danielgafni](https://github.com/danielgafni))
- add support for custom Ray dashboard address with authentication ([#315](https://github.com/danielgafni/dagster-ray/pull/315) by [@JosefNagelschmidt](https://github.com/JosefNagelschmidt))
- add missing fields to RayJobSpec ([#321](https://github.com/danielgafni/dagster-ray/pull/321) by [@danielgafni](https://github.com/danielgafni))

### :bug: Bug Fixes

- use serviceName FQDN instead of serviceIP for cross-cluster connectivity ([#319](https://github.com/danielgafni/dagster-ray/pull/319) by [@pythonmonty](https://github.com/pythonmonty))

### :book: Docs

- add kaiko to the list of users ([#338](https://github.com/danielgafni/dagster-ray/pull/338) by [@pythonmonty](https://github.com/pythonmonty))
- add Sanas to the list of users ([#339](https://github.com/danielgafni/dagster-ray/pull/339) by [@ashutosh1807](https://github.com/ashutosh1807))
- add cross-cluster & authentication docs ([#336](https://github.com/danielgafni/dagster-ray/pull/336) by [@danielgafni](https://github.com/danielgafni))
- update Pipes docs with better Ray API references ([#329](https://github.com/danielgafni/dagster-ray/pull/329) by [@danielgafni](https://github.com/danielgafni))
- switch to Zensical ([#327](https://github.com/danielgafni/dagster-ray/pull/327) by [@danielgafni](https://github.com/danielgafni))

### :hammer_and_wrench: Other Improvements

- release 0.4.3 ([#337](https://github.com/danielgafni/dagster-ray/pull/337) by [@danielgafni](https://github.com/danielgafni))
- add production users list ([#334](https://github.com/danielgafni/dagster-ray/pull/334) by [@danielgafni](https://github.com/danielgafni))
- git-cliff & packaging tweaks ([#333](https://github.com/danielgafni/dagster-ray/pull/333) by [@danielgafni](https://github.com/danielgafni))
- adopt git-cliff ([#332](https://github.com/danielgafni/dagster-ray/pull/332) by [@danielgafni](https://github.com/danielgafni))
- replace pre-commit with prek ([#331](https://github.com/danielgafni/dagster-ray/pull/331) by [@danielgafni](https://github.com/danielgafni))
- adopt Conventional Commits ([#330](https://github.com/danielgafni/dagster-ray/pull/330) by [@danielgafni](https://github.com/danielgafni))
- fix main docs publishing CI ([#328](https://github.com/danielgafni/dagster-ray/pull/328) by [@danielgafni](https://github.com/danielgafni))

### :heart: New Contributors

- [@pythonmonty](https://github.com/pythonmonty) made their first contribution in [#319](https://github.com/danielgafni/dagster-ray/pull/319)
- [@ashutosh1807](https://github.com/ashutosh1807) made their first contribution in [#339](https://github.com/danielgafni/dagster-ray/pull/339)


## 0.4.2 (20-02-2026)

### Added

- `RayResource` now has new lifecycle hook methods for customizing startup messages (and potentially doing something else):
    - `on_create`
    - `on_ready`
    - `on_connect`
    - `on_cleanup`

### Fixes

- fixed duplicated startup log message for `LocalRay`
- fixed the top-level `env_vars` Dagster config field not taking effect for `KubeRayInteractiveJob`

## 0.4.1 (25-01-2026)

### Added

- `RayCluster`'s head pod logs are now displayed on startup timeout or failure

### Fixes

- Prevent the `RayCluster` cleanup sensor from targeting clusters with `.metadata.ownerReferences` set.
- `address` config value can now be omitted for `ray_executor`, making it use Ray's default cluster address resolution. Thanks @cornettew!
- Fixed race condition with cluster sharing: previously multiple steps running in parallel could create different `RayCluster` instances at the same time (that were supposed to be shared). `dagster-ray` now uses Kubernetes [Lease](https://kubernetes.io/docs/concepts/architecture/leases/)-based leader election to coordinate shared cluster creation, which guarantees that only one of the running steps creates the shared `RayCluster`.
- `runtimeEnvYAML` now has all strings fully quoted which fixes passing values such as `1e-5` as `runtime_env` values. Thanks @JosefNagelschmidt!
- `ray_address` is now optional for `RunLauncherConfig`. Thanks @cornettew!

## 0.4.0 (10-10-2025)

This release introduces a new feature that is very useful in dev environments: **Cluster Sharing**. Cluster sharing allows reusing existing `RayCluster` resources created by previous Dagster steps. It's implemented for `KubeRayCluster` Dagster resource. This feature enables faster iteration speed and reduced infrastructure costs (at the expense of job isolation). Therefore `KubeRayCluster` is now recommended over `KubeRayInteractiveJob` for use in **dev** environments.

Learn more in [Cluster Sharing docs](tutorial/kuberay.md#cluster-sharing).

### Added
- `KubeRayCluster.cluster_sharing` parameter that controls cluster sharing behavior.
- `dagster_ray.kuberay.sensors.cleanup_expired_kuberay_clusters` sensor that cleans up expired clusters (both shared and non-shared). Learn more in [docs](api/kuberay.md#dagster_ray.kuberay.sensors.cleanup_expired_kuberay_clusters).
- `dagster-ray` entry now appears in the Dagster libraries list in the web UI.

### Changed
- [:bomb: breaking] - removed `cleanup_kuberay_clusters_op` and other associated definitions in favor of `dagster_ray.kuberay.sensors.cleanup_expired_kuberay_clusters` sensor that is more flexible.

## 0.3.1 (02-10-2025)

### Added
- `failure_tolerance_timeout` configuration parameter for `KubeRayInteractiveJob` and `KubeRayCluster`. It can be set to a positive value to give the cluster some time to transition out of `failed` state (which can be transient in some scenarios) before raising an error.

### Fixes
- ensure both `.head.serviceIP` and `.head.serviceName` are set on the `RayCluster` while waiting for cluster readiness.

## 0.3.0 (19-09-2025)

This release includes massive docs improvements and drops support for Python 3.9.

### Changes

- [:bomb: breaking] dropped Python 3.9 support (EOL October 2025).
- [internal] most of the general, backend-agnostic code has been moved to `dagster_ray.core` (top-level imports still work).

## 0.2.1 (18-09-2025)

### Fixes

- Fixed broken wheel on PyPI.

## 0.2.0 (18-09-2025)

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

## 0.1.0 (05-09-2025)

### Changed
- [:bomb: breaking] `RayResource`: top-level `skip_init` and `skip_setup` configuration parameters have been removed. The `lifecycle` field is the new way of configuring steps performed during resource initialization. `KubeRayCluster`'s `skip_cleanup` has been moved to `lifecycle` as well.
- [:bomb: breaking] injected `dagster.io/run_id` Kubernetes label has been renamed to `dagster/run-id`. Keys starting with `dagster.io/` have been converted to `dagster/` to match how `dagster-k8s` does it.
- [:bomb: breaking] `dagster_ray.kuberay` Configurations have been unified with KubeRay APIs.
- `dagster-ray` now populates Kubernetes labels with more values (including some useful Dagster Cloud values such as `git-sha`).

### Added
- `KubeRayInteractiveJob` -- a resource that utilizes the new `InteractiveMode` for `RayJob`. It can be used to connect to Ray in Client mode -- like `KubeRayCluster` -- but gives access to `RayJob` features, such as automatic cleanup (`ttlSecondsAfterFinished`), retries (`backoffLimit`) and timeouts (`activeDeadlineSeconds`).
- `RayResource` setup lifecycle has been overhauled: resources now has an `actions` parameter with 3 configuration options: `create`, `wait` and `connect`. The user can disable them and run `.create()`, `.wait()` and `.connect()` manually if needed.

