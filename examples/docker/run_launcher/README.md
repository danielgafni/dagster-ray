# Example Dagster + Ray deployment with Docker

1. Start all services with Docker Compose:

```shell
docker compose up --build
```

2. In your browser, open `localhost:3000` to access the Dagster UI and `localhost:8265` to access the Ray dashboard.

3. Launch a job. Observe how the steps are executed in separate Ray jobs in parallel.

4. Make changes to `src/definitions.py`.
