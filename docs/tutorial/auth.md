# Authentication

## Ray Token Authentication

Ray supports [token authentication](https://docs.ray.io/en/latest/ray-core/internals/token-authentication.html) (v2.52.0+) which protects both gRPC (used by `ray.init()`) and HTTP (used by job submission) endpoints.

The client process needs two environment variables to be set:

- `RAY_AUTH_MODE=token`
- `RAY_AUTH_TOKEN=<token>` (or `RAY_AUTH_TOKEN_PATH`)

### Authenticating Against Proxies

Pipes clients such as [`PipesKubeRayJobClient`][dagster_ray.kuberay.PipesKubeRayJobClient] or [`PipesRayJobClient`][dagster_ray.core.pipes.PipesRayJobClient] communicate with the Ray dashboard over HTTP. They support `headers`, `cookies`, and `verify` parameters for authentication:

```python
PipesKubeRayJobClient(
    headers={"Authorization": "Bearer <token>"},
)
```

!!! tip

    These parameters can also be overridden per-invocation in the [`PipesKubeRayJobClient.run`][dagster_ray.kuberay.PipesKubeRayJobClient.run] method.

This is useful when submitting jobs across Kubernetes clusters, where the traffic may be flowing through a proxy requiring additional authentication.
