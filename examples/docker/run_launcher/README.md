# Example Dagster + Ray deployment with Docker

1. Start all services with Docker Compose:

```shell
docker compose up --build
```

2. In your browser, open `localhost:3000` to access the Dagster UI and `localhost:8265` to access the Ray dashboard.

3. Launch a job. Observe how the steps are executed in separate Ray jobs in parallel. Inspect the launched Ray jobs in the Ray dashboard.

4. Make changes to files in `example` package.

5. Re-run the job and observe how your changes are immidiately picked up.
