# `ray_executor`

1. Start a local Ray cluster:

```shell
ray start --head
```

2. Start Dagster in the context of this example directory:

```shell
cd examples/local/executor
dagster dev
```

3. From the UI, run the example job and observe how the steps are executed in separate Ray jobs in parallel.
