# `RayRunLauncher` + `ray_executor`

The `RunLauncher` is configured in [dagster.yaml](dagster.yaml).

1. Start a local Ray cluster:

```shell
ray start --head
```

2. Start Dagster:

```shell
dagster dev -w examples/run_launcher/workspace.yaml
```

3. From the UI, run the example job and observe how:
- The run is launched via a Ray job.
- The run steps in the run are executed in separate Ray jobs in parallel.
