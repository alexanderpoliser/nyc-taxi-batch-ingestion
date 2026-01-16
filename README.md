# NYC Taxi Batch Ingestion

Batch ingestion service that reads [NYC Yellow Taxi](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)*) CSV data and loads it into Postgres using Spring Batch. It exposes HTTP endpoints to start, stop, and monitor the job, and publishes metrics via Micrometer/Prometheus.

## What it does
- Reads `data/yellow_tripdata.csv` (configurable) in chunks
- Validates core fields (timestamps, passenger count, distance)
- Writes to `ingestion.taxi_trip_raw` with de-duplication on `(source_file, line_number)`
- Exposes job control endpoints under `/jobs/*`
- Emits batch metrics to `/actuator/prometheus`

## Prerequisites
- JDK 25 (see `pom.xml`)
- Docker (recommended for Postgres, optional for Prometheus/Grafana)
- Maven is optional because the repo includes the Maven Wrapper

## Quick start
1) Start Postgres with the provided schema:
```bash
docker compose -f docker/postgres.yml up -d
```

2) Ensure the CSV exists at `data/yellow_tripdata.csv` (or override via config, below).

3) Run the app:
```bash
./mvnw spring-boot:run
```
On Windows:
```powershell
.\mvnw.cmd spring-boot:run
```

4) Kick off the batch job:
```bash
curl -X POST http://localhost:8080/jobs/run
```

5) Check job status:
```bash
curl http://localhost:8080/jobs/status/{executionId}
```

## Configuration
Default settings live in `src/main/resources/application.yml`:
- Postgres: `jdbc:postgresql://localhost:5432/nyc_taxi`
- User: `nyc_user`
- Password: `nyc_pass`
- Source file: `taxi.ingestion.source-file` (defaults to `data/yellow_tripdata.csv`)

Override the source file at runtime:
```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments=--taxi.ingestion.source-file=/path/to/file.csv
```

## Database schema
The app expects tables created by the SQL in `docker/init/`:
- `docker/init/01-schema.sql` creates `ingestion.taxi_trip_raw`
- `docker/init/02-batch-tables.sql` creates Spring Batch metadata tables in schema `batch`

Spring Batch schema auto-init is disabled, so you must provision these tables.

## Job control endpoints
- `POST /jobs/run` start a new execution
- `POST /jobs/stop/{executionId}` request a stop
- `POST /jobs/restart/{executionId}` restart a failed/stopped execution
- `GET /jobs/status/{executionId}` execution status and counters

## Observability (optional)
Actuator endpoints are exposed for `health`, `info`, and `prometheus`.

Start Prometheus + Grafana:
```bash
docker compose -f docker/observability.yml up -d
```

Prometheus is configured to scrape `http://host.docker.internal:8080/actuator/prometheus`.

## Tests
```bash
./mvnw test
```
