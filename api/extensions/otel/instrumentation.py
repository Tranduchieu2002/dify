import contextlib
import logging
from collections.abc import Callable
from typing import Protocol, cast, override

import flask
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.metrics import Observation, get_meter, get_meter_provider
from opentelemetry.semconv.attributes.http_attributes import (  # type: ignore[import-untyped]
    HTTP_REQUEST_METHOD,
    HTTP_ROUTE,
)
from opentelemetry.trace import Span, get_tracer_provider
from opentelemetry.trace.status import StatusCode

from configs import dify_config
from dify_app import DifyApp
from extensions.otel.runtime import is_celery_worker

logger = logging.getLogger(__name__)


class SupportsInstrument(Protocol):
    def instrument(self, **kwargs: object) -> None: ...


class SupportsFlaskInstrumentor(Protocol):
    def instrument_app(
        self, app: DifyApp, response_hook: Callable[[Span, str, list], None] | None = None, **kwargs: object
    ) -> None: ...


# Some OpenTelemetry instrumentor constructors are typed loosely enough that
# pyrefly infers `NoneType`. Narrow the instances to just the methods we use
# while leaving runtime behavior unchanged.
def _new_celery_instrumentor() -> SupportsInstrument:
    return cast(
        SupportsInstrument,
        CeleryInstrumentor(tracer_provider=get_tracer_provider(), meter_provider=get_meter_provider()),
    )


def _new_httpx_instrumentor() -> SupportsInstrument:
    return cast(SupportsInstrument, HTTPXClientInstrumentor())


def _new_redis_instrumentor() -> SupportsInstrument:
    return cast(SupportsInstrument, RedisInstrumentor())


def _new_sqlalchemy_instrumentor() -> SupportsInstrument:
    return cast(SupportsInstrument, SQLAlchemyInstrumentor())


class ExceptionLoggingHandler(logging.Handler):
    """
    Handler that records exceptions to the current OpenTelemetry span.

    Unlike creating a new span, this records exceptions on the existing span
    to maintain trace context consistency throughout the request lifecycle.
    """

    @override
    def emit(self, record: logging.LogRecord) -> None:
        with contextlib.suppress(Exception):
            if not record.exc_info:
                return

            from opentelemetry.trace import get_current_span

            span = get_current_span()
            if not span or not span.is_recording():
                return

            # Record exception on the current span instead of creating a new one
            span.set_status(StatusCode.ERROR, record.getMessage())

            # Add log context as span events/attributes
            span.add_event(
                "log.exception",
                attributes={
                    "log.level": record.levelname,
                    "log.message": record.getMessage(),
                    "log.logger": record.name,
                    "log.file.path": record.pathname,
                    "log.file.line": record.lineno,
                },
            )

            if record.exc_info[1]:
                span.record_exception(record.exc_info[1])
            if record.exc_info[0]:
                span.set_attribute("exception.type", record.exc_info[0].__name__)


def instrument_exception_logging() -> None:
    exception_handler = ExceptionLoggingHandler()
    logging.getLogger().addHandler(exception_handler)


def init_flask_instrumentor(app: DifyApp) -> None:
    meter = get_meter("http_metrics", version=dify_config.project.version)
    _http_response_counter = meter.create_counter(
        "http.server.response.count",
        description="Total number of HTTP responses by status code, method and target",
        unit="{response}",
    )

    def response_hook(span: Span, status: str, response_headers: list) -> None:
        if span and span.is_recording():
            try:
                if status.startswith("2"):
                    span.set_status(StatusCode.OK)
                else:
                    span.set_status(StatusCode.ERROR, status)

                status = status.split(" ")[0]
                status_code = int(status)
                status_class = f"{status_code // 100}xx"
                attributes: dict[str, str | int] = {"status_code": status_code, "status_class": status_class}
                request = flask.request
                if request and request.url_rule:
                    attributes[HTTP_ROUTE] = str(request.url_rule.rule)
                if request and request.method:
                    attributes[HTTP_REQUEST_METHOD] = str(request.method)
                _http_response_counter.add(1, attributes)
            except Exception:
                logger.exception("Error setting status and attributes")

    from opentelemetry.instrumentation.flask import FlaskInstrumentor

    instrumentor = cast(SupportsFlaskInstrumentor, FlaskInstrumentor())
    if dify_config.DEBUG:
        logger.info("Initializing Flask instrumentor")
    instrumentor.instrument_app(app, response_hook=response_hook)


def init_sqlalchemy_instrumentor(app: DifyApp) -> None:
    with app.app_context():
        engines = list(app.extensions["sqlalchemy"].engines.values())
        _new_sqlalchemy_instrumentor().instrument(enable_commenter=True, engines=engines)

        meter = get_meter("db_pool_metrics", version=dify_config.project.version)

        def _checked_out(opts: object) -> list:
            with contextlib.suppress(Exception):
                with app.app_context():
                    from extensions.ext_database import db

                    return [Observation(db.engine.pool.checkedout())]
            return [Observation(0)]

        def _checked_in(opts: object) -> list:
            with contextlib.suppress(Exception):
                with app.app_context():
                    from extensions.ext_database import db

                    return [Observation(db.engine.pool.checkedin())]
            return [Observation(0)]

        def _overflow(opts: object) -> list:
            with contextlib.suppress(Exception):
                with app.app_context():
                    from extensions.ext_database import db

                    return [Observation(db.engine.pool.overflow())]
            return [Observation(0)]

        meter.create_observable_gauge(
            "db.pool.checked_out",
            callbacks=[_checked_out],
            description="Number of connections currently checked out from the pool",
            unit="{connection}",
        )
        meter.create_observable_gauge(
            "db.pool.checked_in",
            callbacks=[_checked_in],
            description="Number of idle connections in the pool",
            unit="{connection}",
        )
        meter.create_observable_gauge(
            "db.pool.overflow",
            callbacks=[_overflow],
            description="Number of overflow connections currently open beyond pool_size",
            unit="{connection}",
        )


_MONITORED_QUEUES = [
    "workflow_professional",
    "workflow_team",
    "workflow_sandbox",
    "schedule_executor",
    "schedule_poller",
    "dataset",
    "priority_dataset",
    "pipeline",
    "monitor",
    "mail",
    "conversation",
    "plugin",
    "app_deletion",
    "workflow_draft_var",
    "workflow_storage",
]


def init_celery_queue_metrics(app: DifyApp) -> None:
    from kombu.utils.url import parse_url  # type: ignore[import-untyped]
    from redis import Redis

    redis_config = parse_url(dify_config.CELERY_BROKER_URL)
    celery_redis = Redis(
        host=str(redis_config.get("hostname") or "localhost"),
        port=int(redis_config.get("port") or 6379),
        password=str(pwd) if (pwd := redis_config.get("password")) is not None else None,
        db=int(redis_config.get("virtual_host")) if redis_config.get("virtual_host") else 1,
        ssl=dify_config.BROKER_USE_SSL,
        socket_timeout=5,
        socket_connect_timeout=5,
        health_check_interval=30,
    )

    meter = get_meter("celery_queue_metrics", version=dify_config.project.version)

    def _queue_depth(opts: object) -> list:
        results = []
        with contextlib.suppress(Exception):
            prefix = dify_config.REDIS_KEY_PREFIX
            key_prefix = f"{prefix}:" if prefix else ""
            for q in _MONITORED_QUEUES:
                with contextlib.suppress(Exception):
                    depth = celery_redis.llen(f"{key_prefix}{q}") or 0
                    results.append(Observation(int(depth), {"queue": q}))
        return results

    def _task_status_count(opts: object) -> list:
        results = []
        with contextlib.suppress(Exception):
            with app.app_context():
                from sqlalchemy import text

                from extensions.ext_database import db

                rows = db.session.execute(
                    text(
                        "SELECT queue_name, status, COUNT(*) AS cnt"
                        " FROM workflow_trigger_logs"
                        " WHERE created_at >= NOW() - INTERVAL '1 hour'"
                        " GROUP BY queue_name, status"
                    )
                ).fetchall()
                for row in rows:
                    results.append(Observation(int(row.cnt), {"queue": row.queue_name, "status": row.status}))
        return results

    def _running_elapsed_seconds(opts: object) -> list:
        results = []
        with contextlib.suppress(Exception):
            with app.app_context():
                from sqlalchemy import text

                from extensions.ext_database import db

                rows = db.session.execute(
                    text(
                        "SELECT queue_name,"
                        " EXTRACT(EPOCH FROM (NOW() - triggered_at))::int AS elapsed_sec"
                        " FROM workflow_trigger_logs"
                        " WHERE status = 'running' AND triggered_at IS NOT NULL"
                    )
                ).fetchall()
                for row in rows:
                    results.append(Observation(int(row.elapsed_sec), {"queue": row.queue_name}))
        return results

    def _make_depth_cb():
        def cb(opts: object):
            return _queue_depth(opts)

        return cb

    def _make_status_cb(target_status: str):
        def cb(opts: object):
            all_rows = _task_status_count(opts)
            return [obs for obs in all_rows if obs.attributes.get("status") == target_status]

        return cb

    def _make_elapsed_cb():
        def cb(opts: object):
            return _running_elapsed_seconds(opts)

        return cb

    meter.create_observable_gauge(
        "celery.queue.depth",
        callbacks=[_make_depth_cb()],
        description="Number of pending tasks in each Celery queue (Redis LLEN)",
        unit="{task}",
    )

    for status in ("pending", "queued", "running", "failed", "rate_limited"):
        meter.create_observable_gauge(
            f"celery.task.{status}",
            callbacks=[_make_status_cb(status)],
            description=f"Number of workflow trigger tasks with status={status} in the last hour",
            unit="{task}",
        )

    meter.create_observable_gauge(
        "celery.task.running_elapsed_seconds",
        callbacks=[_make_elapsed_cb()],
        description="Elapsed seconds for each currently running workflow trigger task",
        unit="s",
    )


def init_redis_instrumentor() -> None:
    _new_redis_instrumentor().instrument()


def init_httpx_instrumentor() -> None:
    _new_httpx_instrumentor().instrument()


def init_instruments(app: DifyApp) -> None:
    if not is_celery_worker():
        init_flask_instrumentor(app)
        _new_celery_instrumentor().instrument()

    instrument_exception_logging()
    init_sqlalchemy_instrumentor(app)
    init_redis_instrumentor()
    init_httpx_instrumentor()
    init_celery_queue_metrics(app)
