from prometheus_client import (
    Counter,
    Histogram
)

from prometheus_client import Histogram

HTTP_REQUESTS = Counter(
    "http_requests_total",
    "Total number of HTTP requests",
    ["method", "route", "status"],
)

HTTP_ERRORS = Counter(
    "http_errors_total",
    "Total number of HTTP errors",
    ["method", "route", "status"],
)

HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "route"],
)


MESSAGE_DB_LATENCY = Histogram(
    "message_db_latency_seconds",
    "Time spent creating a chat message in PostgreSQL",
    buckets=(
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
    ),
)


MESSAGE_CDC_LATENCY = Histogram(
    "message_cdc_latency_seconds",
    "Time from PostgreSQL message creation to Debezium Kafka delivery",
    buckets=(
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        30.0,
    ),
)


MESSAGE_CONSUMER_LATENCY = Histogram(
    "message_consumer_latency_seconds",
    "Time spent processing a Kafka chat message inside the application",
    buckets=(
        0.0005,
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
    ),
)


MESSAGE_HTTP_DELIVERY_LATENCY = Histogram(
    "message_http_delivery_latency_seconds",
    "Time from message becoming available in app state to HTTP delivery",
    buckets=(
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
    ),
)