# loans-analytics-delta-pipeline

Daily snapshot CSV ingest (11 parts, ~300k rows each) -> set-based delta feed generation for a downstream system.

**Stack:** Java 21, Spring Boot 3.5.9, PostgreSQL, Flyway, Gradle, Lombok.

## What it does

For a given `asOf` date:
1. Discovers **exactly 11** snapshot CSV parts (shards) in an input dir.
2. Loads each file into PostgreSQL using **COPY** (fast path).
3. Upserts all staged rows into `loan_snapshot` for that day.
4. Generates a **delta** vs `asOf - 1 day` (I/U/D) using a **FULL OUTER JOIN** on `loan_id`.
5. Exposes an endpoint to download the delta as CSV.

This design is optimized for large daily volumes (millions of rows): **COPY + SQL set operations** instead of row-by-row Java diffs.

## CSV format

All parts share the same schema and include a header:

```csv
loan_id,customer_id,product_code,principal,currency,status,opened_at,maturity_at,interest_rate,updated_at
L-1001,C-42,HOME,100000.00,EUR,OPEN,2024-01-01,2034-01-01,0.035000,2026-01-17T00:00:00Z
```

### File naming

Default pattern (configurable):

- `loans_snapshot_%s_part*.csv` where `%s` is `yyyyMMdd`
- Expected parts: **11**

Example for `2026-01-17`:

- `loans_snapshot_20260117_part01.csv`
- ...
- `loans_snapshot_20260117_part11.csv`

## Run locally

### 1) Start Postgres

```bash
docker compose up -d
```

### 2) Configure input dir

Put the 11 CSV parts into `./data` (default), or change:

```yaml
loans:
  ingestion:
    input-dir: ./data
```

### 3) Trigger ingestion + delta generation

Option A (REST):

```bash
curl -X POST "http://localhost:8080/api/admin/ingest?asOf=2026-01-17"
```

Option B (CLI):

```bash
./gradlew bootRun --args='--loans.ingestion.cli-enabled=true --loans.ingestion.as-of=2026-01-17'
```

### 4) Download delta feed

```bash
curl -o delta.csv "http://localhost:8080/api/delta?asOf=2026-01-17"
```

## Database model (high level)

- `loan_snapshot_staging` — raw COPY destination per run
- `loan_snapshot` — immutable daily snapshot keyed by `(as_of_date, loan_id)`
- `loan_delta` — delta rows for downstream keyed by `(job_run_id, loan_id, change_type)`
- `job_run` — run audit (status, timestamps)

## Production-grade considerations included

- **PostgreSQL COPY** for ingestion throughput
- **Idempotent** run re-execution (upsert snapshot; delta regeneration deletes by run_id)
- **Set-based delta generation** (SQL FULL OUTER JOIN)
- Staging is kept on failure to support investigation
- Integration tests with **Testcontainers Postgres**

## GitHub

Suggested repository name: **`loans-analytics-delta-pipeline`**

Suggested description:
> Java 21 + Spring Boot pipeline that ingests daily loan snapshot CSV parts via PostgreSQL COPY and produces an I/U/D delta feed for downstream systems.


## Postman

Import from `postman/`:
- `Loans-Analytics.postman_collection.json`
- `Loans-Analytics.local.postman_environment.json`

Main flow:
1) Health
2) List feeds
3) Run ingest
4) List runs / Get run feeds
5) Download delta CSV


---

## 📜 License

MIT

---

## Contact

**Dimitry Ivaniuta** — [dzmitry.ivaniuta.services@gmail.com](mailto:dzmitry.ivaniuta.services@gmail.com) — [GitHub](https://github.com/DimitryIvaniuta)

