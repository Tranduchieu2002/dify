# Local Development Guide

This guide covers two modes of running Dify locally:

- **Dev mode** — run API and web as host processes (fast iteration, hot-reload)
- **Docker test mode** — build from source inside Docker (mirrors production, validates the build)

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Docker + Docker Compose | v2.20+ | https://docs.docker.com/get-docker/ |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Node.js | 22+ | `nvm install 22` |
| pnpm | 11.5+ | `corepack enable` (after Node 22 install) |

Check versions:

```bash
docker compose version
uv --version
node --version   # must be v22+
pnpm --version
```

---

## Dev Mode (host processes)

Dev mode runs PostgreSQL, Redis, Weaviate, Sandbox, and the observability stack in Docker, while API and web run directly on your machine for fast iteration.

### 1. Start middleware

```bash
cd docker
cp middleware.env.example middleware.env   # first time only
docker compose -f docker-compose.middleware.yaml --env-file middleware.env \
  -p dify-middleware up -d
```

> **Port conflicts**: if ports 5432 or 6379 are in use, set `EXPOSE_POSTGRES_PORT=5433` and `EXPOSE_REDIS_PORT=6380` in `middleware.env`, then update `api/.env` with those ports.

### 2. Configure the API

```bash
cd api
cp .env.example .env
```

Edit `.env` — minimum changes required:

```env
SECRET_KEY=<generate with: openssl rand -base64 42>
DB_HOST=localhost
DB_PORT=5432        # or 5433 if remapped
DB_PASSWORD=difyai123456
REDIS_HOST=localhost
REDIS_PORT=6379     # or 6380 if remapped
REDIS_PASSWORD=difyai123456
WEAVIATE_ENDPOINT=http://localhost:8080
CODE_EXECUTION_ENDPOINT=http://localhost:8194
```

Run DB migrations:

```bash
cd api
uv run flask db upgrade
```

### 3. Start the API

```bash
# In the api/ directory
LOG_OUTPUT_FORMAT=json LOG_LEVEL=DEBUG \
uv run flask run --host 0.0.0.0 --port 5001 --debug
```

> `LOG_OUTPUT_FORMAT=json` must be set on the CLI — uv does not auto-load `.env` for env var overrides.

### 4. Start the Celery worker

```bash
# In the api/ directory
LOG_OUTPUT_FORMAT=json LOG_LEVEL=INFO \
uv run celery -A app.celery worker -P gevent -c 1 --loglevel=INFO -Q dataset,generation,mail,ops_trace,app_deletion
```

### 5. Start the web frontend

```bash
cd web
pnpm install
pnpm dev
```

Web is available at `http://localhost:3000`.

### 6. Start observability (optional)

```bash
./dev/start-logging
```

This starts Alloy, Loki, Tempo, Prometheus, and Grafana. Dashboard is at `http://localhost:4000` (admin/admin).

---

## Docker Test Mode (full stack from source)

This mode builds API and web from your local source code and runs everything in Docker. Use it to:
- Validate a build before pushing
- Test configuration changes
- Reproduce production-like behaviour

### Start

```bash
cd docker
docker compose -f docker-compose.test.yaml -p dify-test up --build -d
```

On first run, Docker builds the API (~3 min) and web (~5 min) images. Subsequent starts with `--build` only rebuild if source files changed.

### Access

| Service | URL |
|---------|-----|
| Dify web app | http://localhost:8080 |
| API | http://localhost:8080/v1 |
| Grafana | http://localhost:4000 (admin/admin) |

### Logs

```bash
# All services
docker compose -f docker-compose.test.yaml -p dify-test logs -f

# Specific service
docker compose -f docker-compose.test.yaml -p dify-test logs -f api
docker compose -f docker-compose.test.yaml -p dify-test logs -f worker
```

### Rebuild a single service

```bash
docker compose -f docker-compose.test.yaml -p dify-test build api
docker compose -f docker-compose.test.yaml -p dify-test up -d api
```

### Stop and clean up

```bash
# Stop containers, keep volumes
docker compose -f docker-compose.test.yaml -p dify-test down

# Stop and remove all volumes (full reset)
docker compose -f docker-compose.test.yaml -p dify-test down -v
```

### Environment variables

All app services share `docker/envs/test/shared.env`. Override values there — do not commit secrets to this file.

Key values in `shared.env`:

| Variable | Value | Notes |
|----------|-------|-------|
| `DB_HOST` | `db_postgres` | Docker internal name |
| `REDIS_HOST` | `redis` | Docker internal name |
| `WEAVIATE_ENDPOINT` | `http://weaviate:8080` | Docker internal name |
| `OTLP_BASE_ENDPOINT` | `http://alloy:4318` | Traces → Alloy → Tempo |
| `LOG_OUTPUT_FORMAT` | `json` | Required for Loki parsing |
| `ENABLE_OTEL` | `true` | OpenTelemetry traces enabled |

---

## Observability

The observability stack (Alloy, Loki, Tempo, Prometheus, Grafana) runs in both modes.

### Stack

```
Dify API/Worker
  │
  ├── OTLP HTTP (traces + metrics) → Alloy :4318
  └── stdout JSON logs ──────────── Alloy (Docker log discovery)

Alloy
  ├── traces  → Tempo      :4317
  ├── metrics → Prometheus :9090 (remote_write)
  └── logs    → Loki       :3100

Grafana → queries Loki, Tempo, Prometheus
```

### Grafana dashboards

Open `http://localhost:4000`, go to **Dashboards → Dify Dev Logs**.

Variables:

| Variable | Type | Usage |
|----------|------|-------|
| Service | Multi-select | Filter by api, worker, beat, etc. |
| Severity | Single-select | DEBUG / INFO / WARNING / ERROR / CRITICAL |
| Trace ID | Text box | Paste a trace_id to see all log lines for that trace |
| Workflow ID | Text box | Filter logs for a specific workflow execution |
| Tenant ID | Text box | Filter by tenant |

Leave text boxes at `.*` to show everything.

Clicking a trace_id value in a log line opens the trace in Tempo (cross-linking is configured automatically).

### LogQL quick reference

```logql
# All logs from the api service
{service="api"}

# Filter to a specific trace
{service=~"api|worker"} | json | trace_id="<32-char hex>"

# Filter to a workflow execution
{service=~"api|worker"} | json workflow_id="attributes.workflow_id" | workflow_id="<uuid>"

# Filter by tenant
{service="api"} | json tenant_id="identity.tenant_id" | tenant_id="<uuid>"

# Errors only (last 15 minutes)
{service=~"api|worker|beat"} | json | severity=~"ERROR|CRITICAL"

# Count errors per service
sum by (service) (rate({service=~"api|worker|beat"} | json | severity="ERROR" [5m]))
```

### TraceQL quick reference (Tempo)

```traceql
# Slow requests
{ traceDuration > 500ms }

# By service
{ resource.service.name = "langgenius/dify" }

# By workflow span attribute
{ span.workflow_id = "<uuid>" }

# Errors
{ status = error }
```

---

## JSON log format

When `LOG_OUTPUT_FORMAT=json`, each log line is a JSON object:

```json
{
  "ts": "2024-01-01T00:00:00.000Z",
  "severity": "INFO",
  "service": "api",
  "caller": "core/workflow/workflow_engine.py:123",
  "message": "Workflow started",
  "trace_id": "a1b2c3d4e5f6...",
  "identity": {
    "tenant_id": "uuid",
    "user_id": "uuid"
  },
  "attributes": {
    "workflow_id": "uuid",
    "app_id": "uuid",
    "run_id": "uuid"
  }
}
```

---

## Common issues

### Port conflict on 5432 or 6379

```bash
# Find what's using the port
sudo ss -tlnp | grep 5432

# Use alternate ports in middleware.env
EXPOSE_POSTGRES_PORT=5433
EXPOSE_REDIS_PORT=6380
```

Then update `api/.env` with the new ports.

### DB migration fails (relation already exists)

The `init` service runs `flask db upgrade` before API starts. If migrations fail on a fresh volume, check the logs:

```bash
docker compose -f docker-compose.test.yaml -p dify-test logs init
```

If the volume is corrupted, reset it:

```bash
docker compose -f docker-compose.test.yaml -p dify-test down -v
docker compose -f docker-compose.test.yaml -p dify-test up --build -d
```

### No logs in Grafana

1. Verify `LOG_OUTPUT_FORMAT=json` is set (check with `docker compose logs api | head -5` — should show JSON)
2. Verify Alloy is running: `docker compose logs alloy`
3. Verify Loki is receiving: open `http://localhost:3100/ready` — should return `ready`
4. In Grafana Explore → Loki, run `{service="api"}` (no extra filters)

### Web build OOM in Docker

The Next.js build is memory-intensive. Increase Docker memory limit to at least 4 GB in Docker Desktop settings, or pass a build arg:

```bash
docker compose -f docker-compose.test.yaml -p dify-test build \
  --build-arg NODE_OPTIONS="--max-old-space-size=4096" web
```

### uv not found

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
```

Add the export to `~/.zshrc` or `~/.bashrc` for persistence.
