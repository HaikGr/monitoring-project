import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Your UI polls these every 700ms; tracing them would drown out real traffic.
EXCLUDED = "metrics,health,api/messages,api/typing"


def setup_tracing() -> None:
    if os.getenv("OTEL_SDK_DISABLED", "false").lower() == "true":
        return
    provider = TracerProvider(
        resource=Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", "chat")})
    )
    # Endpoint is read from OTEL_EXPORTER_OTLP_ENDPOINT
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    PsycopgInstrumentor().instrument()


def instrument_app(app) -> None:
    FastAPIInstrumentor.instrument_app(app, excluded_urls=EXCLUDED)