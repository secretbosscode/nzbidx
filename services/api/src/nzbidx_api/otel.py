from __future__ import annotations

import logging
import os
from contextlib import contextmanager

try:  # pragma: no cover - optional dependency
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
except Exception:  # pragma: no cover - no otel installed
    trace = None  # type: ignore

logger = logging.getLogger(__name__)


def setup_tracing() -> None:
    """Configure OpenTelemetry if the required env vars are set."""
    if trace is None:
        return
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return
    service = os.getenv("OTEL_SERVICE_NAME", "nzbidx")
    provider = TracerProvider(resource=Resource.create({"service.name": service}))
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)


def current_trace_id() -> str:
    if trace is None:
        return ""
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if not ctx or ctx.trace_id == 0:
        return ""
    return f"{ctx.trace_id:032x}"


@contextmanager
def start_span(name: str):
    if trace is None:
        yield
    else:  # pragma: no cover - span creation
        with trace.get_tracer("nzbidx").start_as_current_span(name):
            yield
