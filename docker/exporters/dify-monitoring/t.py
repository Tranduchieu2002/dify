#!/usr/bin/env python3
"""
dify_exporter.py — Prometheus exporter cho Dify 1.11 (app mode: workflow).

Nguyên tắc thiết kế:
  * Đọc Postgres của Dify theo CURSOR TĂNG DẦN, không bao giờ SELECT COUNT(*).
    -> Retention của Dify xoá log cũ cũng không làm counter tụt.
  * Counter/Histogram được cộng dồn trong bộ nhớ và persist ra state file,
    nên restart exporter không reset chuỗi số liệu.
  * Có safety-lag để tránh bỏ sót row commit muộn (transaction skew).
  * Có cardinality guard: label value vượt ngưỡng bị gộp vào "__other__".

Chạy: python3 dify_exporter.py   (đọc cấu hình từ biến môi trường, xem .env.example)
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg
from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import (
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
)

LOG = logging.getLogger("dify_exporter")

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #


def _env_bool(key: str, default: bool) -> bool:
    return os.getenv(key, str(default)).strip().lower() in ("1", "true", "yes", "on")


def _env_float_list(key: str, default: List[float]) -> List[float]:
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    return sorted(float(x) for x in raw.split(",") if x.strip())


@dataclass
class Config:
    dsn: str = os.getenv(
        "DIFY_DB_DSN",
        "postgresql://postgres:difyai123456@db:5432/dify",
    )
    listen_port: int = int(os.getenv("EXPORTER_PORT", "9192"))
    listen_addr: str = os.getenv("EXPORTER_ADDR", "0.0.0.0")

    poll_interval: float = float(os.getenv("POLL_INTERVAL_SECONDS", "30"))
    # Chỉ đọc row có event_time cũ hơn ngần này giây -> tránh miss row commit muộn.
    safety_lag: float = float(os.getenv("SAFETY_LAG_SECONDS", "10"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "5000"))
    # Số vòng lặp fetch tối đa mỗi chu kỳ (chống kẹt khi backfill lượng lớn).
    max_batches_per_cycle: int = int(os.getenv("MAX_BATCHES_PER_CYCLE", "20"))
    backfill_days: float = float(os.getenv("BACKFILL_DAYS", "0"))

    state_path: str = os.getenv("STATE_PATH", "/var/lib/dify-exporter/state.json")

    # Cửa sổ quét cho gauge "running" (giới hạn để dùng được index, tránh seq scan).
    running_window_days: int = int(os.getenv("RUNNING_WINDOW_DAYS", "7"))

    # Label controls
    include_workflow_id: bool = _env_bool("INCLUDE_WORKFLOW_ID_LABEL", True)
    include_node_title: bool = _env_bool("INCLUDE_NODE_TITLE_LABEL", True)
    node_title_max_len: int = int(os.getenv("NODE_TITLE_MAX_LEN", "48"))

    # Cardinality guard: số label-set tối đa cho mỗi metric family.
    max_series_per_metric: int = int(os.getenv("MAX_SERIES_PER_METRIC", "5000"))

    # Lọc
    node_triggered_from: str = os.getenv("NODE_TRIGGERED_FROM", "workflow-run")
    exclude_triggered_from: Tuple[str, ...] = tuple(
        x.strip() for x in os.getenv("EXCLUDE_TRIGGERED_FROM", "").split(",") if x.strip()
    )
    # Chỉ lấy app có mode nằm trong danh sách này (rỗng = lấy hết).
    app_modes: Tuple[str, ...] = tuple(
        x.strip() for x in os.getenv("APP_MODES", "workflow").split(",") if x.strip()
    )

    run_buckets: List[float] = field(
        default_factory=lambda: _env_float_list(
            "RUN_DURATION_BUCKETS",
            [0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600],
        )
    )
    node_buckets: List[float] = field(
        default_factory=lambda: _env_float_list(
            "NODE_DURATION_BUCKETS",
            [0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120],
        )
    )


CFG = Config()

# --------------------------------------------------------------------------- #
# Phân loại lỗi
# --------------------------------------------------------------------------- #

_ERROR_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("timeout", re.compile(r"timed?\s*out|timeout|deadline exceeded|read timed", re.I)),
    ("rate_limit", re.compile(r"rate.?limit|429|too many requests|tpm|rpm limit", re.I)),
    ("quota", re.compile(r"quota|insufficient.?(balance|credit)|billing|payment", re.I)),
    ("auth", re.compile(r"401|403|unauthorized|forbidden|invalid.?api.?key|authentication", re.I)),
    ("connection", re.compile(r"connection|econnrefused|dns|unreachable|network|ssl|socket", re.I)),
    ("upstream_5xx", re.compile(r"\b5\d{2}\b|internal server error|bad gateway|service unavailable", re.I)),
    ("json_parse", re.compile(r"json|expecting value|deserializ|parse error|invalid syntax", re.I)),
    ("validation", re.compile(r"validat|required|missing|invalid (input|parameter|variable)|schema", re.I)),
    ("context_length", re.compile(r"context.?length|max.?tokens|too long|maximum context", re.I)),
    ("content_filter", re.compile(r"content.?filter|moderation|safety|blocked by", re.I)),
    ("code_error", re.compile(r"traceback|nameerror|typeerror|keyerror|indexerror|referenceerror", re.I)),
]


def classify_error(text: Optional[str]) -> str:
    if not text:
        return "unknown"
    for name, pat in _ERROR_PATTERNS:
        if pat.search(text):
            return name
    return "other"


# --------------------------------------------------------------------------- #
# Cardinality guard
# --------------------------------------------------------------------------- #


class CardinalityGuard:
    """Giới hạn số label-set mỗi metric. Vượt ngưỡng -> gộp vào __other__."""

    OTHER = "__other__"

    def __init__(self, limit: int):
        self.limit = limit
        self._seen: Dict[str, set] = {}
        self.dropped: Dict[str, int] = {}

    def admit(self, metric: str, labels: Tuple[str, ...]) -> Tuple[str, ...]:
        seen = self._seen.setdefault(metric, set())
        if labels in seen:
            return labels
        if len(seen) < self.limit:
            seen.add(labels)
            return labels
        self.dropped[metric] = self.dropped.get(metric, 0) + 1
        # Giữ lại 2 label đầu (app_id, app_name) để vẫn quy được về app.
        return labels[:2] + tuple(self.OTHER for _ in labels[2:])

    def size(self, metric: str) -> int:
        return len(self._seen.get(metric, ()))


# --------------------------------------------------------------------------- #
# Kho số liệu (thread-safe)
# --------------------------------------------------------------------------- #


class MetricStore:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.lock = threading.Lock()
        self.guard = CardinalityGuard(cfg.max_series_per_metric)

        # counters: metric -> labels tuple -> value
        self.counters: Dict[str, Dict[Tuple[str, ...], float]] = {}
        # histograms: metric -> labels tuple -> {"buckets": [...], "sum": f, "count": n}
        self.histograms: Dict[str, Dict[Tuple[str, ...], Dict[str, Any]]] = {}
        # gauges snapshot (ghi đè mỗi chu kỳ)
        self.gauges: Dict[str, Dict[Tuple[str, ...], float]] = {}

        # cursors: (event_time_iso, id)
        self.cursor_runs: Optional[Tuple[str, str]] = None
        self.cursor_nodes: Optional[Tuple[str, str]] = None

        # self-metrics
        self.scrape_duration = 0.0
        self.last_success_ts = 0.0
        self.db_errors = 0.0
        self.rows_processed: Dict[str, float] = {"workflow_runs": 0.0, "workflow_node_executions": 0.0}

    # -- mutators ---------------------------------------------------------- #

    def inc(self, metric: str, labels: Tuple[str, ...], value: float = 1.0) -> None:
        labels = self.guard.admit(metric, labels)
        d = self.counters.setdefault(metric, {})
        d[labels] = d.get(labels, 0.0) + value

    def observe(self, metric: str, labels: Tuple[str, ...], value: float, buckets: List[float]) -> None:
        labels = self.guard.admit(metric, labels)
        d = self.histograms.setdefault(metric, {})
        h = d.get(labels)
        if h is None:
            h = {"buckets": [0.0] * (len(buckets) + 1), "sum": 0.0, "count": 0.0}
            d[labels] = h
        for i, ub in enumerate(buckets):
            if value <= ub:
                h["buckets"][i] += 1
        h["buckets"][-1] += 1  # +Inf
        h["sum"] += value
        h["count"] += 1

    def set_gauge_family(self, metric: str, values: Dict[Tuple[str, ...], float]) -> None:
        self.gauges[metric] = values

    def max_gauge(self, metric: str, labels: Tuple[str, ...], value: float) -> None:
        """Gauge kiểu 'giá trị lớn nhất từng thấy' — dùng cho last_success_timestamp."""
        labels = self.guard.admit(metric, labels)
        d = self.gauges.setdefault(metric, {})
        if value > d.get(labels, 0.0):
            d[labels] = value

    # -- persistence ------------------------------------------------------- #

    def dump(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "version": 1,
                "cursor_runs": self.cursor_runs,
                "cursor_nodes": self.cursor_nodes,
                "counters": {
                    m: [[list(k), v] for k, v in d.items()] for m, d in self.counters.items()
                },
                "histograms": {
                    m: [[list(k), v] for k, v in d.items()] for m, d in self.histograms.items()
                },
                "last_success_gauges": [
                    [list(k), v]
                    for k, v in self.gauges.get("dify_workflow_run_last_success_timestamp", {}).items()
                ],
                "rows_processed": self.rows_processed,
                "db_errors": self.db_errors,
            }

    def load(self, data: Dict[str, Any]) -> None:
        with self.lock:
            self.cursor_runs = tuple(data["cursor_runs"]) if data.get("cursor_runs") else None
            self.cursor_nodes = tuple(data["cursor_nodes"]) if data.get("cursor_nodes") else None
            for m, items in (data.get("counters") or {}).items():
                self.counters[m] = {tuple(k): v for k, v in items}
                self.guard._seen.setdefault(m, set()).update(self.counters[m].keys())
            for m, items in (data.get("histograms") or {}).items():
                self.histograms[m] = {tuple(k): v for k, v in items}
                self.guard._seen.setdefault(m, set()).update(self.histograms[m].keys())
            lsg = {tuple(k): v for k, v in (data.get("last_success_gauges") or [])}
            if lsg:
                self.gauges["dify_workflow_run_last_success_timestamp"] = lsg
            self.rows_processed.update(data.get("rows_processed") or {})
            self.db_errors = data.get("db_errors", 0.0)

    def save_to_disk(self) -> None:
        path = self.cfg.state_path
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            tmp = f"{path}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(self.dump(), fh)
            os.replace(tmp, path)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Không ghi được state file %s: %s", path, exc)

    def load_from_disk(self) -> None:
        path = self.cfg.state_path
        if not os.path.exists(path):
            LOG.info("Chưa có state file, khởi động mới.")
            return
        try:
            with open(path, encoding="utf-8") as fh:
                self.load(json.load(fh))
            LOG.info("Đã nạp state từ %s (cursor_runs=%s)", path, self.cursor_runs)
        except Exception as exc:  # noqa: BLE001
            LOG.error("State file hỏng (%s), bỏ qua: %s", path, exc)


STORE = MetricStore(CFG)

# --------------------------------------------------------------------------- #
# SQL
# --------------------------------------------------------------------------- #

RUNS_SQL = """
SELECT
    wr.id::text                                        AS id,
    wr.app_id::text                                    AS app_id,
    COALESCE(a.name, 'unknown')                        AS app_name,
    wr.workflow_id::text                               AS workflow_id,
    COALESCE(wr.status, 'unknown')                     AS status,
    COALESCE(wr.triggered_from, 'unknown')             AS triggered_from,
    COALESCE(wr.elapsed_time, 0)::float8               AS elapsed_time,
    COALESCE(wr.total_steps, 0)::bigint                AS total_steps,
    COALESCE(wr.exceptions_count, 0)::bigint           AS exceptions_count,
    COALESCE(wr.finished_at, wr.created_at)            AS event_time
FROM workflow_runs wr
LEFT JOIN apps a ON a.id = wr.app_id
WHERE wr.status <> 'running'
  AND COALESCE(wr.finished_at, wr.created_at) < %(upper)s
  AND (COALESCE(wr.finished_at, wr.created_at), wr.id) > (%(cur_ts)s, %(cur_id)s::uuid)
  {app_mode_filter}
ORDER BY COALESCE(wr.finished_at, wr.created_at), wr.id
LIMIT %(limit)s
"""

NODES_SQL = """
SELECT
    wne.id::text                                       AS id,
    wne.app_id::text                                   AS app_id,
    COALESCE(a.name, 'unknown')                        AS app_name,
    COALESCE(wne.node_type, 'unknown')                 AS node_type,
    COALESCE(NULLIF(wne.title, ''), 'untitled')        AS node_title,
    COALESCE(wne.status, 'unknown')                    AS status,
    COALESCE(wne.elapsed_time, 0)::float8              AS elapsed_time,
    LEFT(COALESCE(wne.error, ''), 500)                 AS error,
    COALESCE(wne.finished_at, wne.created_at)          AS event_time
FROM workflow_node_executions wne
LEFT JOIN apps a ON a.id = wne.app_id
WHERE wne.status <> 'running'
  AND COALESCE(wne.finished_at, wne.created_at) < %(upper)s
  AND (COALESCE(wne.finished_at, wne.created_at), wne.id) > (%(cur_ts)s, %(cur_id)s::uuid)
  {node_from_filter}
  {app_mode_filter}
ORDER BY COALESCE(wne.finished_at, wne.created_at), wne.id
LIMIT %(limit)s
"""

RUNNING_SQL = """
SELECT
    wr.app_id::text                        AS app_id,
    COALESCE(a.name, 'unknown')            AS app_name,
    COUNT(*)::bigint                       AS running_count,
    COALESCE(MAX(EXTRACT(EPOCH FROM (%(now)s - wr.created_at))), 0)::float8 AS oldest_age
FROM workflow_runs wr
LEFT JOIN apps a ON a.id = wr.app_id
WHERE wr.status = 'running'
  AND wr.created_at > %(window_start)s
  {app_mode_filter}
GROUP BY 1, 2
"""

TERMINAL_OK = {"succeeded", "partial-succeeded"}

# --------------------------------------------------------------------------- #
# Poller
# --------------------------------------------------------------------------- #

ZERO_UUID = "00000000-0000-0000-0000-000000000000"


def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class DifyPoller:
    def __init__(self, cfg: Config, store: MetricStore):
        self.cfg = cfg
        self.store = store
        self._app_mode_filter_runs = ""
        self._app_mode_filter_nodes = ""
        if cfg.app_modes:
            modes = ", ".join(f"'{m}'" for m in cfg.app_modes)
            self._app_mode_filter_runs = f"AND a.mode IN ({modes})"
            self._app_mode_filter_nodes = f"AND a.mode IN ({modes})"
        self._node_from_filter = (
            f"AND wne.triggered_from = '{cfg.node_triggered_from}'"
            if cfg.node_triggered_from
            else ""
        )

    # -- schema check ------------------------------------------------------ #

    def verify_schema(self, conn: psycopg.Connection) -> None:
        required = {
            "workflow_runs": [
                "id", "app_id", "workflow_id", "status", "triggered_from",
                "elapsed_time", "total_steps", "exceptions_count", "created_at", "finished_at",
            ],
            "workflow_node_executions": [
                "id", "app_id", "node_type", "title", "status", "elapsed_time",
                "error", "triggered_from", "created_at", "finished_at",
            ],
            "apps": ["id", "name", "mode"],
        }
        with conn.cursor() as cur:
            for table, cols in required.items():
                cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    (table,),
                )
                have = {r[0] for r in cur.fetchall()}
                if not have:
                    raise RuntimeError(f"Không tìm thấy bảng '{table}' — kiểm tra DSN/database.")
                missing = [c for c in cols if c not in have]
                if missing:
                    raise RuntimeError(
                        f"Bảng '{table}' thiếu cột {missing}. "
                        f"Schema Dify của bạn khác bản 1.11 — cần sửa SQL trong exporter."
                    )
        LOG.info("Schema check OK.")

    # -- cursor init ------------------------------------------------------- #

    def _initial_cursor(self) -> Tuple[str, str]:
        start = utcnow_naive() - timedelta(days=self.cfg.backfill_days)
        return (start.isoformat(sep=" "), ZERO_UUID)

    # -- fetch loops ------------------------------------------------------- #

    def _fetch_runs(self, conn: psycopg.Connection, upper: datetime) -> int:
        cur_ts, cur_id = self.store.cursor_runs or self._initial_cursor()
        sql = RUNS_SQL.format(app_mode_filter=self._app_mode_filter_runs)
        total = 0
        for _ in range(self.cfg.max_batches_per_cycle):
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    sql,
                    {
                        "upper": upper,
                        "cur_ts": cur_ts,
                        "cur_id": cur_id,
                        "limit": self.cfg.batch_size,
                    },
                )
                rows = cur.fetchall()
            if not rows:
                break
            for row in rows:
                self._handle_run(row)
            last = rows[-1]
            cur_ts, cur_id = last["event_time"].isoformat(sep=" "), last["id"]
            with self.store.lock:
                self.store.cursor_runs = (cur_ts, cur_id)
                self.store.rows_processed["workflow_runs"] += len(rows)
            total += len(rows)
            if len(rows) < self.cfg.batch_size:
                break
        return total

    def _fetch_nodes(self, conn: psycopg.Connection, upper: datetime) -> int:
        cur_ts, cur_id = self.store.cursor_nodes or self._initial_cursor()
        sql = NODES_SQL.format(
            node_from_filter=self._node_from_filter,
            app_mode_filter=self._app_mode_filter_nodes,
        )
        total = 0
        for _ in range(self.cfg.max_batches_per_cycle):
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    sql,
                    {
                        "upper": upper,
                        "cur_ts": cur_ts,
                        "cur_id": cur_id,
                        "limit": self.cfg.batch_size,
                    },
                )
                rows = cur.fetchall()
            if not rows:
                break
            for row in rows:
                self._handle_node(row)
            last = rows[-1]
            cur_ts, cur_id = last["event_time"].isoformat(sep=" "), last["id"]
            with self.store.lock:
                self.store.cursor_nodes = (cur_ts, cur_id)
                self.store.rows_processed["workflow_node_executions"] += len(rows)
            total += len(rows)
            if len(rows) < self.cfg.batch_size:
                break
        return total

    # -- row handlers ------------------------------------------------------ #

    def _wf(self, workflow_id: str) -> Tuple[str, ...]:
        return (workflow_id,) if self.cfg.include_workflow_id else ()

    def _handle_run(self, row: Dict[str, Any]) -> None:
        if row["triggered_from"] in self.cfg.exclude_triggered_from:
            return
        app_id, app_name = row["app_id"], row["app_name"]
        wf = self._wf(row["workflow_id"])
        status = row["status"]

        self.store.inc(
            "dify_workflow_runs_total",
            (app_id, app_name) + wf + (status, row["triggered_from"]),
        )
        self.store.observe(
            "dify_workflow_run_duration_seconds",
            (app_id, app_name) + wf + (status,),
            float(row["elapsed_time"]),
            self.cfg.run_buckets,
        )
        if row["total_steps"]:
            self.store.inc(
                "dify_workflow_run_steps_total",
                (app_id, app_name) + wf,
                float(row["total_steps"]),
            )
        if row["exceptions_count"]:
            self.store.inc(
                "dify_workflow_exceptions_total",
                (app_id, app_name) + wf,
                float(row["exceptions_count"]),
            )
        if status in TERMINAL_OK:
            ts = row["event_time"].replace(tzinfo=timezone.utc).timestamp()
            self.store.max_gauge(
                "dify_workflow_run_last_success_timestamp",
                (app_id, app_name) + wf,
                ts,
            )

    def _title(self, title: str) -> Tuple[str, ...]:
        if not self.cfg.include_node_title:
            return ()
        t = title.strip().replace("\n", " ")
        if len(t) > self.cfg.node_title_max_len:
            t = t[: self.cfg.node_title_max_len] + "…"
        return (t,)

    def _handle_node(self, row: Dict[str, Any]) -> None:
        app_id, app_name = row["app_id"], row["app_name"]
        ntype = row["node_type"]
        title = self._title(row["node_title"])
        status = row["status"]

        self.store.inc(
            "dify_workflow_node_executions_total",
            (app_id, app_name, ntype) + title + (status,),
        )
        self.store.observe(
            "dify_workflow_node_duration_seconds",
            (app_id, app_name, ntype) + title,
            float(row["elapsed_time"]),
            self.cfg.node_buckets,
        )
        if status in ("failed", "exception"):
            self.store.inc(
                "dify_workflow_node_errors_total",
                (app_id, app_name, ntype) + title + (classify_error(row["error"]), status),
            )

    # -- gauges ------------------------------------------------------------ #

    def _refresh_running(self, conn: psycopg.Connection, now: datetime) -> None:
        sql = RUNNING_SQL.format(app_mode_filter=self._app_mode_filter_runs)
        window_start = now - timedelta(days=self.cfg.running_window_days)
        with conn.cursor() as cur:
            cur.execute(sql, {"now": now, "window_start": window_start})
            rows = cur.fetchall()
        running: Dict[Tuple[str, ...], float] = {}
        oldest: Dict[Tuple[str, ...], float] = {}
        for app_id, app_name, count, age in rows:
            running[(app_id, app_name)] = float(count)
            oldest[(app_id,)] = float(age)
        self.store.set_gauge_family("dify_workflow_runs_running", running)
        self.store.set_gauge_family("dify_workflow_oldest_running_age_seconds", oldest)

    # -- main cycle -------------------------------------------------------- #

    def poll_once(self) -> None:
        t0 = time.monotonic()
        now = utcnow_naive()
        upper = now - timedelta(seconds=self.cfg.safety_lag)
        with psycopg.connect(self.cfg.dsn, connect_timeout=10, application_name="dify_exporter") as conn:
            conn.read_only = True
            n_runs = self._fetch_runs(conn, upper)
            n_nodes = self._fetch_nodes(conn, upper)
            self._refresh_running(conn, now)
        self.store.scrape_duration = time.monotonic() - t0
        self.store.last_success_ts = time.time()
        self.store.save_to_disk()
        LOG.info(
            "Poll xong trong %.2fs — runs=%d nodes=%d",
            self.store.scrape_duration, n_runs, n_nodes,
        )


# --------------------------------------------------------------------------- #
# Collector
# --------------------------------------------------------------------------- #


def _labels(base: List[str], wf: bool, tail: List[str]) -> List[str]:
    return base + (["workflow_id"] if wf else []) + tail


class DifyCollector:
    def __init__(self, cfg: Config, store: MetricStore):
        self.cfg = cfg
        self.store = store

    def _counter(self, name: str, doc: str, labels: List[str]) -> Iterable:
        data = self.store.counters.get(name, {})
        fam = CounterMetricFamily(name, doc, labels=labels)
        for lbls, val in data.items():
            if len(lbls) == len(labels):
                fam.add_metric(list(lbls), val)
        yield fam

    def _histogram(self, name: str, doc: str, labels: List[str], buckets: List[float]) -> Iterable:
        data = self.store.histograms.get(name, {})
        fam = HistogramMetricFamily(name, doc, labels=labels)
        for lbls, h in data.items():
            if len(lbls) != len(labels):
                continue
            bucket_pairs = [[str(ub), h["buckets"][i]] for i, ub in enumerate(buckets)]
            bucket_pairs.append(["+Inf", h["buckets"][-1]])
            fam.add_metric(list(lbls), bucket_pairs, sum_value=h["sum"])
        yield fam

    def _gauge(self, name: str, doc: str, labels: List[str]) -> Iterable:
        data = self.store.gauges.get(name, {})
        fam = GaugeMetricFamily(name, doc, labels=labels)
        for lbls, val in data.items():
            if len(lbls) == len(labels):
                fam.add_metric(list(lbls), val)
        yield fam

    def collect(self):
        cfg, store = self.cfg, self.store
        wf = cfg.include_workflow_id
        nt = cfg.include_node_title
        base = ["app_id", "app_name"]

        with store.lock:
            # --- 1. Workflow run core ---
            yield from self._counter(
                "dify_workflow_runs_total",
                "Tổng số workflow run đã kết thúc, theo status và nguồn kích hoạt.",
                _labels(base, wf, ["status", "triggered_from"]),
            )
            yield from self._histogram(
                "dify_workflow_run_duration_seconds",
                "Phân bố elapsed_time của workflow run (giây).",
                _labels(base, wf, ["status"]),
                cfg.run_buckets,
            )
            yield from self._counter(
                "dify_workflow_run_steps_total",
                "Tổng số step đã thực thi (cộng dồn total_steps).",
                _labels(base, wf, []),
            )
            yield from self._counter(
                "dify_workflow_exceptions_total",
                "Tổng số exception ở node nhưng workflow vẫn tiếp tục qua fail-branch.",
                _labels(base, wf, []),
            )
            yield from self._gauge(
                "dify_workflow_runs_running",
                "Số workflow run đang ở trạng thái running.",
                base,
            )
            yield from self._gauge(
                "dify_workflow_oldest_running_age_seconds",
                "Tuổi (giây) của workflow run đang chạy lâu nhất.",
                ["app_id"],
            )
            yield from self._gauge(
                "dify_workflow_run_last_success_timestamp",
                "Unix timestamp của run thành công gần nhất.",
                _labels(base, wf, []),
            )

            # --- 2. Node level ---
            node_base = ["app_id", "app_name", "node_type"] + (["node_title"] if nt else [])
            yield from self._counter(
                "dify_workflow_node_executions_total",
                "Tổng số lần thực thi node, theo loại node và status.",
                node_base + ["status"],
            )
            yield from self._histogram(
                "dify_workflow_node_duration_seconds",
                "Phân bố elapsed_time của từng node (giây).",
                node_base,
                cfg.node_buckets,
            )
            yield from self._counter(
                "dify_workflow_node_errors_total",
                "Số node thất bại, đã phân loại theo error_class.",
                node_base + ["error_class", "status"],
            )

            # --- self metrics ---
            g = GaugeMetricFamily(
                "dify_exporter_scrape_duration_seconds",
                "Thời gian chu kỳ poll DB gần nhất.",
            )
            g.add_metric([], store.scrape_duration)
            yield g

            g = GaugeMetricFamily(
                "dify_exporter_last_success_timestamp",
                "Unix timestamp lần poll DB thành công gần nhất.",
            )
            g.add_metric([], store.last_success_ts)
            yield g

            c = CounterMetricFamily(
                "dify_exporter_db_errors_total", "Số lần lỗi kết nối/truy vấn DB."
            )
            c.add_metric([], store.db_errors)
            yield c

            c = CounterMetricFamily(
                "dify_exporter_rows_processed_total",
                "Số dòng đã xử lý theo bảng.",
                labels=["table"],
            )
            for table, val in store.rows_processed.items():
                c.add_metric([table], val)
            yield c

            g = GaugeMetricFamily(
                "dify_exporter_series_count",
                "Số label-set đang giữ cho mỗi metric family.",
                labels=["metric"],
            )
            for m in list(store.counters) + list(store.histograms):
                g.add_metric([m], store.guard.size(m))
            yield g

            c = CounterMetricFamily(
                "dify_exporter_series_dropped_total",
                "Số label-set bị gộp vào __other__ do vượt trần cardinality.",
                labels=["metric"],
            )
            for m, v in store.guard.dropped.items():
                c.add_metric([m], v)
            yield c


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

_stop = threading.Event()


def _handle_signal(signum, _frame):
    LOG.info("Nhận tín hiệu %s, đang thoát...", signum)
    _stop.set()


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    STORE.load_from_disk()
    poller = DifyPoller(CFG, STORE)

    # Schema check trước khi mở cổng — fail fast nếu DSN/schema sai.
    try:
        with psycopg.connect(CFG.dsn, connect_timeout=10, application_name="dify_exporter") as conn:
            poller.verify_schema(conn)
    except Exception as exc:  # noqa: BLE001
        LOG.error("Khởi động thất bại: %s", exc)
        return 1

    REGISTRY.register(DifyCollector(CFG, STORE))
    start_http_server(CFG.listen_port, addr=CFG.listen_addr)
    LOG.info("Exporter đang lắng nghe %s:%d/metrics", CFG.listen_addr, CFG.listen_port)

    backoff = 1.0
    while not _stop.is_set():
        try:
            poller.poll_once()
            backoff = 1.0
            _stop.wait(CFG.poll_interval)
        except Exception as exc:  # noqa: BLE001
            STORE.db_errors += 1
            LOG.error("Lỗi khi poll: %s", exc, exc_info=LOG.isEnabledFor(logging.DEBUG))
            _stop.wait(min(backoff, 60))
            backoff = min(backoff * 2, 60)

    STORE.save_to_disk()
    LOG.info("Đã lưu state, thoát.")
    return 0


if __name__ == "__main__":
    sys.exit(main())