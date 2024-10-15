# `RayRunLauncher`

The `RunLauncher` is configured in [dagster.yaml](dagster.yaml).

1. Start a local Ray cluster:

```shell
ray start --head
```

2. Start Dagster in the context of this example directory:

```shell
cd examples/local/run_launcher
dagster dev
```

3. From the UI, run the example job and observe how the steps are executed in a Ray job.

Note that this example doens't have the `ray_executor` configured, so steps will be executed in the same Ray job using the default `multiprocess_executor`.

To see an example of how to use both `RayRunLauncher` and `ray_executor`, see the [RunLauncher and Executor example](../run_launcher_and_executor/README.md).
