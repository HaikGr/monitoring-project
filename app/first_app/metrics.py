"""Prometheus metrics shared by app1 and app2.

Drop this file next to main.py in BOTH apps.
"""

from prometheus_client import Counter, Histogram

HTTP_REQUESTS = Counter(
    "chat_http_requests_total",
    "Total HTTP requests handled.",
    ["method", "route", "status"],
)

HTTP_ERRORS = Counter(
    "chat_http_errors_total",
    "HTTP responses with status >= 400 (or unhandled exceptions).",
    ["method", "route", "status"],
)

HTTP_REQUEST_DURATION = Histogram(
    "chat_http_request_duration_seconds",
    "Wall-clock duration of HTTP requests.",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# created_at in Postgres -> record arriving in the consumer.
# Covers the whole Postgres -> Debezium -> Kafka -> consumer path.
MESSAGE_CDC_LATENCY = Histogram(
    "chat_message_cdc_latency_seconds",
    "Latency from row commit in Postgres to the CDC event reaching the consumer.",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

# Time spent decoding/merging a single Kafka record into local state.
MESSAGE_CONSUMER_LATENCY = Histogram(
    "chat_message_consumer_processing_seconds",
    "Time spent processing one Kafka record inside the consumer loop.",
    buckets=(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
)

# Time spent writing a message to Postgres via POST /messages.
MESSAGE_DB_LATENCY = Histogram(
    "chat_message_db_insert_seconds",
    "Time spent inserting a message into Postgres.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# Consumer receipt -> first time the message is served over /api/messages.
MESSAGE_HTTP_DELIVERY_LATENCY = Histogram(
    "chat_message_http_delivery_seconds",
    "Latency from consumer receipt to first delivery over /api/messages.",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

__all__ = [
    "HTTP_REQUESTS",
    "HTTP_ERRORS",
    "HTTP_REQUEST_DURATION",
    "MESSAGE_CDC_LATENCY",
    "MESSAGE_CONSUMER_LATENCY",
    "MESSAGE_DB_LATENCY",
    "MESSAGE_HTTP_DELIVERY_LATENCY",
]