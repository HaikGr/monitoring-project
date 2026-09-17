import json
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    generate_latest,
)

from chat_postgres import create_message, save_messages

from metrics import (
    HTTP_ERRORS,
    HTTP_REQUESTS,
    HTTP_REQUEST_DURATION,
    MESSAGE_CDC_LATENCY,
    MESSAGE_CONSUMER_LATENCY,
    MESSAGE_DB_LATENCY,
    MESSAGE_HTTP_DELIVERY_LATENCY,
)


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "my-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "chat-messages",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "app2-chat",
)

KAFKA_TYPING_TOPIC = os.getenv(
    "KAFKA_TYPING_TOPIC",
    "chat-typing",
)

APP_ID = os.getenv(
    "APP_ID",
    "app2",
)

MAX_MESSAGES = int(
    os.getenv(
        "MAX_MESSAGES",
        "100",
    )
)


# ============================================================
# Instance identity
# ============================================================

# Kubernetes automatically provides HOSTNAME.
#
# Example:
#
# chat-second-app-7d9f6c87d4-abc12
#
# Every replica therefore gets a unique Kafka consumer group.
INSTANCE_ID = os.getenv(
    "HOSTNAME",
    APP_ID,
)


# ============================================================
# IMPORTANT KAFKA DESIGN
#
# Each application replica gets its own consumer group.
#
# This means:
#
# app2 pod A -> app2-chat-pod-A
# app2 pod B -> app2-chat-pod-B
#
# Each replica receives the COMPLETE chat stream.
#
# This is necessary because the application keeps the current
# chat state in local memory.
# ============================================================

CHAT_CONSUMER_GROUP = (
    f"{KAFKA_GROUP_ID}-{INSTANCE_ID}"
)

TYPING_CONSUMER_GROUP = (
    f"{KAFKA_GROUP_ID}-typing-{INSTANCE_ID}"
)


# ============================================================
# In-memory state
# ============================================================

messages: deque[
    dict[str, Any]
] = deque(
    maxlen=MAX_MESSAGES
)


# Instead of:
#
# typing_users["app1"] = True
#
# we keep the timestamp too:
#
# typing_users["app1"] = {
#     "is_typing": True,
#     "timestamp_ms": 123456789
# }
#
# This prevents an older event from overwriting a newer event.

typing_users: dict[
    str,
    dict[str, Any],
] = {}


state_lock = threading.Lock()


# ============================================================
# Kafka consumers
# ============================================================

consumer = Consumer(
    {
        "bootstrap.servers":
            KAFKA_BOOTSTRAP_SERVERS,

        "group.id":
            CHAT_CONSUMER_GROUP,

        "auto.offset.reset":
            "earliest",

        "enable.auto.commit":
            True,
    }
)


typing_consumer = Consumer(
    {
        "bootstrap.servers":
            KAFKA_BOOTSTRAP_SERVERS,

        "group.id":
            TYPING_CONSUMER_GROUP,

        "auto.offset.reset":
            "latest",

        "enable.auto.commit":
            True,
    }
)


# ============================================================
# Kafka producer
# ============================================================

typing_producer = Producer(
    {
        "bootstrap.servers":
            KAFKA_BOOTSTRAP_SERVERS,
    }
)


# ============================================================
# Consumer lifecycle
# ============================================================

stop_event = threading.Event()

consumer_thread: threading.Thread | None = None

typing_consumer_thread: threading.Thread | None = None


# ============================================================
# Utility functions
# ============================================================

def now_ms() -> int:
    """
    Current UTC time in milliseconds.
    """

    return int(
        datetime.now(
            timezone.utc
        ).timestamp() * 1000
    )


def parse_timestamp_seconds(
    value: Any,
) -> float | None:
    """
    Convert PostgreSQL/Debezium ISO timestamp
    into Unix seconds.

    Supports values such as:

        2026-09-16T00:30:10.123Z

    or:

        2026-09-16T00:30:10.123+00:00
    """

    if not value:
        return None

    try:

        text = str(value)

        text = text.replace(
            "Z",
            "+00:00",
        )

        dt = datetime.fromisoformat(
            text
        )

        if dt.tzinfo is None:

            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.timestamp()

    except Exception as exc:

        print(
            "Failed to parse timestamp "
            f"{value!r}: {exc}",
            flush=True,
        )

        return None


def message_sort_key(
    message: dict[str, Any],
) -> tuple[str, int]:
    """
    Deterministic ordering for messages.

    Kafka guarantees ordering inside a partition, but not
    a single global ordering across multiple partitions.

    Therefore we order the chat state using:

        created_at
        +
        database message ID
    """

    created_at = (
        message.get(
            "created_at"
        )
        or ""
    )

    try:

        message_id = int(
            message.get("id")
            or 0
        )

    except (
        TypeError,
        ValueError,
    ):

        message_id = 0

    return (
        str(created_at),
        message_id,
    )


# ============================================================
# FastAPI lifespan
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    global consumer_thread
    global typing_consumer_thread

    stop_event.clear()

    consumer_thread = threading.Thread(
        target=consume_messages,
        name=(
            f"chat-consumer-{INSTANCE_ID}"
        ),
        daemon=True,
    )

    typing_consumer_thread = threading.Thread(
        target=consume_typing_events,
        name=(
            f"typing-consumer-{INSTANCE_ID}"
        ),
        daemon=True,
    )

    consumer_thread.start()

    typing_consumer_thread.start()

    print(
        "Application started:"
        f" app={APP_ID}"
        f" instance={INSTANCE_ID}"
        f" chat_group={CHAT_CONSUMER_GROUP}"
        f" typing_group={TYPING_CONSUMER_GROUP}",
        flush=True,
    )

    try:

        yield

    finally:

        print(
            "Application shutting down:"
            f" instance={INSTANCE_ID}",
            flush=True,
        )

        stop_event.set()

        try:

            typing_producer.flush(
                5
            )

        except Exception:

            pass

        if consumer_thread:

            consumer_thread.join(
                timeout=5
            )

        if typing_consumer_thread:

            typing_consumer_thread.join(
                timeout=5
            )


# ============================================================
# FastAPI application
# ============================================================

app = FastAPI(
    title="Kafka Chat - App 2",
    description=(
        "PostgreSQL + Debezium + Kafka "
        "chat application"
    ),
    lifespan=lifespan,
)


# ============================================================
# Request models
# ============================================================

class MessageRequest(BaseModel):
    conversation_id: str
    content: str


class TypingRequest(BaseModel):
    conversation_id: str
    is_typing: bool


# ============================================================
# HTTP metrics middleware
# ============================================================

@app.middleware("http")
async def metrics_middleware(
    request,
    call_next,
):

    start_time = time.perf_counter()

    response = await call_next(
        request
    )

    duration = (
        time.perf_counter()
        - start_time
    )

    route = request.url.path

    status = str(
        response.status_code
    )

    HTTP_REQUESTS.labels(
        method=request.method,
        route=route,
        status=status,
    ).inc()

    HTTP_REQUEST_DURATION.labels(
        method=request.method,
        route=route,
    ).observe(
        duration
    )

    if response.status_code >= 400:

        HTTP_ERRORS.labels(
            method=request.method,
            route=route,
            status=status,
        ).inc()

    return response


# ============================================================
# Prometheus endpoint
# ============================================================

@app.get("/metrics")
def metrics():

    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


# ============================================================
# Health
# ============================================================

@app.get("/health")
def health() -> dict[str, str]:

    return {
        "status": "ok",
        "app_id": APP_ID,
        "instance_id": INSTANCE_ID,
    }


# Postgre

@app.post("/export-messages")
def export_messages():
    save_messages()
    return {"status": "export started"}

# ============================================================
# Debug
# ============================================================

@app.get("/api/debug")
def debug() -> dict[str, Any]:

    with state_lock:

        return {
            "app_id":
                APP_ID,

            "instance_id":
                INSTANCE_ID,

            "chat_consumer_group":
                CHAT_CONSUMER_GROUP,

            "typing_consumer_group":
                TYPING_CONSUMER_GROUP,

            "cached_messages":
                len(messages),

            "typing_users":
                dict(typing_users),
        }


# ============================================================
# Send typing event
# ============================================================

@app.post("/typing")
def update_typing(
    typing: TypingRequest,
):

    event = {
        "event_type":
            "typing",

        "conversation_id":
            typing.conversation_id,

        "user_id":
            APP_ID,

        "is_typing":
            typing.is_typing,

        "timestamp_ms":
            now_ms(),
    }

    try:

        typing_producer.produce(
            KAFKA_TYPING_TOPIC,

            key=(
                f"{typing.conversation_id}:"
                f"{APP_ID}"
            ),

            value=json.dumps(
                event
            ).encode("utf-8"),
        )

        typing_producer.flush(
            5
        )

        return {
            "status":
                "sent",

            "event":
                event,
        }

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to publish "
                "typing event: "
                f"{exc}"
            ),
        )


# ============================================================
# Get typing state
# ============================================================

@app.get("/api/typing")
def get_typing() -> dict[str, Any]:

    with state_lock:

        users = [
            user_id
            for user_id, state
            in typing_users.items()
            if state[
                "is_typing"
            ]
        ]

    return {
        "typing_users":
            users,
    }


# ============================================================
# Create chat message
#
# DB latency starts BEFORE create_message()
# and ends AFTER PostgreSQL returns.
# ============================================================

@app.post("/messages")
def send_message(
    message: MessageRequest,
):

    content = (
        message.content.strip()
    )

    if not content:

        raise HTTPException(
            status_code=400,
            detail=(
                "Message cannot "
                "be empty"
            ),
        )

    try:

        db_start = time.perf_counter()

        created_message = create_message(
            conversation_id=(
                message.conversation_id
            ),
            sender="app2",
            receiver="app1",
            content=content,
        )

        db_latency = (
            time.perf_counter()
            - db_start
        )

        MESSAGE_DB_LATENCY.observe(
            db_latency
        )

        print(
            "Message written to "
            "PostgreSQL:"
            f" db_latency="
            f"{db_latency:.6f}s",
            flush=True,
        )

        return created_message

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to create "
                "message: "
                f"{exc}"
            ),
        )


# ============================================================
# Typing Kafka consumer
# ============================================================

def consume_typing_events() -> None:

    typing_consumer.subscribe(
        [KAFKA_TYPING_TOPIC]
    )

    print(
        "Typing consumer started:"
        f" topic={KAFKA_TYPING_TOPIC}"
        f" group={TYPING_CONSUMER_GROUP}"
        f" instance={INSTANCE_ID}",
        flush=True,
    )

    try:

        while not stop_event.is_set():

            msg = (
                typing_consumer.poll(
                    1.0
                )
            )

            if msg is None:
                continue

            if msg.error():

                print(
                    "Typing Kafka error:"
                    f" {msg.error()}",
                    flush=True,
                )

                continue

            try:

                raw_value = (
                    msg.value()
                )

                if raw_value is None:
                    continue

                event = json.loads(
                    raw_value.decode(
                        "utf-8"
                    )
                )

                user_id = event.get(
                    "user_id"
                )

                if not user_id:
                    continue

                # Don't display our own typing state.
                if user_id == APP_ID:
                    continue

                event_timestamp = int(
                    event.get(
                        "timestamp_ms",
                        0,
                    )
                    or 0
                )

                if event_timestamp <= 0:

                    event_timestamp = now_ms()

                is_typing = bool(
                    event.get(
                        "is_typing",
                        False,
                    )
                )

                with state_lock:

                    previous = (
                        typing_users.get(
                            user_id
                        )
                    )

                    # Ignore an older event that
                    # arrived after a newer event.
                    if (
                        previous
                        and event_timestamp
                        < previous[
                            "timestamp_ms"
                        ]
                    ):

                        continue

                    typing_users[
                        user_id
                    ] = {
                        "is_typing":
                            is_typing,

                        "timestamp_ms":
                            event_timestamp,
                    }

                print(
                    "Typing event received:"
                    f" instance="
                    f"{INSTANCE_ID}"
                    f" user={user_id}"
                    f" is_typing="
                    f"{is_typing}",
                    flush=True,
                )

            except Exception as exc:

                print(
                    "Failed to process "
                    "typing event:"
                    f" {exc}",
                    flush=True,
                )

    except KafkaException as exc:

        print(
            "Typing Kafka consumer "
            "exception:"
            f" {exc}",
            flush=True,
        )

    except Exception as exc:

        print(
            "Typing consumer stopped:"
            f" {exc}",
            flush=True,
        )

    finally:

        try:

            typing_consumer.close()

        except Exception:

            pass


# ============================================================
# Chat Kafka consumer
# ============================================================

def consume_messages() -> None:

    consumer.subscribe(
        [KAFKA_TOPIC]
    )

    print(
        "Kafka chat consumer started:"
        f" topic={KAFKA_TOPIC}"
        f" bootstrap="
        f"{KAFKA_BOOTSTRAP_SERVERS}"
        f" group={CHAT_CONSUMER_GROUP}"
        f" instance={INSTANCE_ID}",
        flush=True,
    )

    try:

        while not stop_event.is_set():

            msg = consumer.poll(
                1.0
            )

            if msg is None:
                continue

            if msg.error():

                print(
                    "Kafka error:"
                    f" {msg.error()}",
                    flush=True,
                )

                continue

            # ====================================================
            # Consumer latency starts here.
            #
            # This measures:
            #
            # Kafka poll
            #      ↓
            # JSON parsing
            #      ↓
            # Debezium extraction
            #      ↓
            # deduplication
            #      ↓
            # state update
            #      ↓
            # sorting
            # ====================================================

            consumer_start = (
                time.perf_counter()
            )

            try:

                raw_value = (
                    msg.value()
                )

                if raw_value is None:

                    continue

                value = json.loads(
                    raw_value.decode(
                        "utf-8"
                    )
                )

                after = value.get(
                    "after"
                )

                # Ignore deletes/tombstones.
                if not after:

                    continue

                # =================================================
                # CDC latency
                #
                # PostgreSQL created_at
                #          ↓
                # Debezium
                #          ↓
                # Kafka
                #          ↓
                # this consumer receives event
                # =================================================

                created_at = after.get(
                    "created_at"
                )

                db_timestamp = (
                    parse_timestamp_seconds(
                        created_at
                    )
                )

                if (
                    db_timestamp
                    is not None
                ):

                    cdc_latency = (
                        time.time()
                        - db_timestamp
                    )

                    # Ignore impossible negative values.
                    # They usually mean clocks/timestamps
                    # are not aligned.
                    if cdc_latency >= 0:

                        MESSAGE_CDC_LATENCY.observe(
                            cdc_latency
                        )

                        print(
                            "CDC latency:"
                            f" {cdc_latency:.6f}s",
                            flush=True,
                        )

                # =================================================
                # Create normalized chat event
                # =================================================

                event = {
                    "id":
                        after.get(
                            "id"
                        ),

                    "conversation_id":
                        after.get(
                            "conversation_id"
                        ),

                    "sender":
                        after.get(
                            "sender"
                        ),

                    "receiver":
                        after.get(
                            "receiver"
                        ),

                    "content":
                        after.get(
                            "content"
                        ),

                    "created_at":
                        created_at,

                    "operation":
                        value.get(
                            "op"
                        ),

                    # Internal timestamp.
                    #
                    # This is useful for measuring how long
                    # the message has been waiting in our
                    # application state before HTTP delivery.
                    "_consumer_received_at":
                        time.time(),
                }

                if not event["id"]:

                    continue

                # =================================================
                # Update local state
                # =================================================

                with state_lock:

                    existing = [
                        item
                        for item in messages
                        if item.get(
                            "id"
                        )
                        == event["id"]
                    ]

                    for item in existing:

                        try:

                            messages.remove(
                                item
                            )

                        except ValueError:

                            pass

                    messages.append(
                        event
                    )

                    # =================================================
                    # Important with multiple Kafka partitions:
                    #
                    # Kafka guarantees ordering inside one partition,
                    # not globally between P0/P1/P2/P3.
                    #
                    # Therefore explicitly sort the local cache.
                    # =================================================

                    ordered = sorted(
                        messages,
                        key=message_sort_key,
                    )

                    messages.clear()

                    for item in ordered[
                        -MAX_MESSAGES:
                    ]:

                        messages.append(
                            item
                        )

                # =================================================
                # Consumer latency ends after local state
                # has been updated.
                # =================================================

                consumer_latency = (
                    time.perf_counter()
                    - consumer_start
                )

                MESSAGE_CONSUMER_LATENCY.observe(
                    consumer_latency
                )

                print(
                    "Chat message received:"
                    f" instance="
                    f"{INSTANCE_ID}"
                    f" partition="
                    f"{msg.partition()}"
                    f" offset="
                    f"{msg.offset()}"
                    f" sender="
                    f"{event['sender']}"
                    f" content="
                    f"{event['content']!r}"
                    f" consumer_latency="
                    f"{consumer_latency:.6f}s",
                    flush=True,
                )

            except Exception as exc:

                print(
                    "Failed to process "
                    "Kafka message:"
                    f" {exc}",
                    flush=True,
                )

            finally:

                # Make sure consumer latency is recorded even
                # when parsing/processing throws an exception.
                #
                # The successful path has already recorded it,
                # so we don't record a second value there.
                pass

    except KafkaException as exc:

        print(
            "Kafka consumer exception:"
            f" {exc}",
            flush=True,
        )

    except Exception as exc:

        print(
            "Consumer stopped:"
            f" {exc}",
            flush=True,
        )

    finally:

        try:

            consumer.close()

        except Exception:

            pass


# ============================================================
# Get messages
#
# We also expose a practical HTTP delivery metric here.
#
# IMPORTANT:
#
# Because the browser polls this endpoint repeatedly,
# we measure each message only once per application instance.
# ============================================================

@app.get("/api/messages")
def get_messages() -> list[dict[str, Any]]:

    request_time = time.time()

    with state_lock:

        result = list(
            messages
        )

    result.sort(
        key=message_sort_key
    )

    # ============================================================
    # Measure time from consumer state arrival to HTTP delivery.
    #
    # We only observe a message once.
    #
    # Internal fields are removed before returning JSON.
    # ============================================================

    response_messages = []

    for message in result:

        consumer_received_at = (
            message.get(
                "_consumer_received_at"
            )
        )

        if (
            consumer_received_at
            is not None
        ):

            delivery_latency = (
                request_time
                - consumer_received_at
            )

            if delivery_latency >= 0:

                # We need to avoid measuring the same message
                # on every polling request.
                #
                # Store an internal marker.
                delivered_at = message.get(
                    "_http_delivered_at"
                )

                if delivered_at is None:

                    MESSAGE_HTTP_DELIVERY_LATENCY.observe(
                        delivery_latency
                    )

                    # We need to update the original object.
                    #
                    # The object lives in the deque.
                    with state_lock:

                        for stored_message in messages:

                            if (
                                stored_message.get(
                                    "id"
                                )
                                == message.get(
                                    "id"
                                )
                            ):

                                stored_message[
                                    "_http_delivered_at"
                                ] = request_time

                                break

        # Never expose internal monitoring fields.
        clean_message = {
            key: value
            for key, value in message.items()
            if not key.startswith("_")
        }

        response_messages.append(
            clean_message
        )

    return response_messages


# ============================================================
# Frontend
# ============================================================

@app.get(
    "/",
    response_class=HTMLResponse,
)
def index() -> str:

    return """
<!DOCTYPE html>

<html>

<head>

    <meta charset="UTF-8">

    <meta
        name="viewport"
        content="width=device-width, initial-scale=1.0"
    >

    <title>App 2 Chat</title>


    <style>

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            min-height: 100vh;

            display: flex;
            justify-content: center;
            align-items: center;

            background: #f3f4f6;

            font-family:
                Arial,
                sans-serif;
        }


        .chat {

            width:
                min(
                    800px,
                    95vw
                );

            height: 80vh;

            background: white;

            border-radius: 16px;

            box-shadow:
                0 10px 40px
                rgba(
                    0,
                    0,
                    0,
                    0.15
                );

            display:
                flex;

            flex-direction:
                column;

            overflow:
                hidden;
        }


        .header {

            padding:
                18px 24px;

            background:
                #111827;

            color:
                white;
        }


        .header h1 {

            margin:
                0;

            font-size:
                20px;
        }

        .header-content {

            display:
                flex;

            justify-content:
                space-between;

            align-items:
                center;

            gap:
                15px;
        }


        #exportButton {

            background:
                #2563eb;

            color:
                white;

            white-space:
                nowrap;
        }


        #exportButton:hover {

            background:
                #1d4ed8;
        }


        .header p {

            margin:
                5px 0 0;

            font-size:
                13px;

            opacity:
                0.7;
        }


        #messages {

            flex:
                1;

            overflow-y:
                auto;

            padding:
                20px;

            display:
                flex;

            flex-direction:
                column;

            gap:
                10px;
        }


        .message {

            max-width:
                70%;

            padding:
                10px 14px;

            border-radius:
                14px;

            line-height:
                1.4;
        }


        .mine {

            align-self:
                flex-end;

            background:
                #dbeafe;
        }


        .theirs {

            align-self:
                flex-start;

            background:
                #f3f4f6;
        }


        .sender {

            font-size:
                11px;

            opacity:
                0.6;

            margin-bottom:
                3px;
        }


        .content {

            font-size:
                15px;

            word-break:
                break-word;
        }


        .composer {

            display:
                flex;

            gap:
                10px;

            padding:
                15px;

            border-top:
                1px solid #e5e7eb;
        }


        #messageInput {

            flex:
                1;

            padding:
                12px 14px;

            border:
                1px solid #d1d5db;

            border-radius:
                10px;

            outline:
                none;

            font-size:
                15px;
        }


        button {

            border:
                0;

            padding:
                12px 20px;

            border-radius:
                10px;

            cursor:
                pointer;

            font-size:
                15px;
        }


        button:disabled {

            opacity:
                0.6;

            cursor:
                not-allowed;
        }


        #typingIndicator {

            min-height:
                24px;

            padding:
                4px 15px;

            font-size:
                13px;

            font-style:
                italic;

            color:
                #666;
        }

    </style>

</head>


<body>


<div class="chat">


<div class="header">

    <div class="header-content">

        <div>
            <h1>
                App 2 Chat
            </h1>

            <p>
                PostgreSQL → Debezium → Kafka
            </p>
        </div>

        <button
            id="exportButton"
            onclick="exportMessages()"
        >
            Export Messages
        </button>

    </div>

</div>


    <div id="messages"></div>


    <div id="typingIndicator"></div>


    <div class="composer">

        <input
            id="messageInput"
            type="text"
            placeholder="Type a message..."
            autocomplete="off"
        >


        <button
            id="sendButton"
            onclick="sendMessage()"
        >
            Send
        </button>

    </div>


</div>


<script>


const messagesElement =
    document.getElementById(
        "messages"
    );


const input =
    document.getElementById(
        "messageInput"
    );


const typingIndicator =
    document.getElementById(
        "typingIndicator"
    );


const sendButton =
    document.getElementById(
        "sendButton"
    );


let isTyping =
    false;


let typingTimeout =
    null;


let displayedIds =
    new Set();


let loadingMessages =
    false;


/* ============================================================
   Publish typing
   ============================================================ */

async function publishTyping(
    isCurrentlyTyping
) {

    try {

        const response =
            await fetch(
                "/typing",
                {
                    method:
                        "POST",

                    headers: {
                        "Content-Type":
                            "application/json"
                    },

                    body:
                        JSON.stringify({
                            conversation_id:
                                "chat-1",

                            is_typing:
                                isCurrentlyTyping
                        })
                }
            );


        if (!response.ok) {

            console.error(
                "Typing request failed:",
                response.status
            );
        }


    } catch (error) {

        console.error(
            "Failed to publish typing event:",
            error
        );
    }
}

/* ============================================================
   Export messages
   ============================================================ */

async function exportMessages() {

    const exportButton =
        document.getElementById(
            "exportButton"
        );


    exportButton.disabled =
        true;


    exportButton.textContent =
        "Exporting...";


    try {

        const response =
            await fetch(
                "/export-messages",
                {
                    method:
                        "POST"
                }
            );


        if (
            !response.ok
        ) {

            let errorMessage =
                "Failed to export messages";


            try {

                const error =
                    await response.json();

                errorMessage =
                    error.detail ||
                    errorMessage;

            } catch (_) {

                // Keep default error message.
            }


            throw new Error(
                errorMessage
            );
        }


        const data =
            await response.json();


        console.log(
            data
        );


        alert(
            "Messages exported successfully!"
        );


    } catch (error) {

        console.error(
            "Failed to export messages:",
            error
        );


        alert(
            error.message ||
            "Failed to export messages"
        );


    } finally {

        exportButton.disabled =
            false;

        exportButton.textContent =
            "Export Messages";
    }
}


/* ============================================================
   Render message
   ============================================================ */

function renderMessage(
    message
) {

    if (!message.id) {

        return;
    }


    const messageId =
        String(
            message.id
        );


    if (
        displayedIds.has(
            messageId
        )
    ) {

        return;
    }


    displayedIds.add(
        messageId
    );


    const wrapper =
        document.createElement(
            "div"
        );


    wrapper.className =
        "message " +
        (
            message.sender
                === "app2"
                ? "mine"
                : "theirs"
        );


    const sender =
        document.createElement(
            "div"
        );


    sender.className =
        "sender";


    sender.textContent =
        message.sender ||
        "";


    const content =
        document.createElement(
            "div"
        );


    content.className =
        "content";


    content.textContent =
        message.content ||
        "";


    wrapper.appendChild(
        sender
    );


    wrapper.appendChild(
        content
    );


    messagesElement.appendChild(
        wrapper
    );


    messagesElement.scrollTop =
        messagesElement.scrollHeight;
}


/* ============================================================
   Load messages
   ============================================================ */

async function loadMessages() {

    if (
        loadingMessages
    ) {

        return;
    }


    loadingMessages =
        true;


    try {

        const response =
            await fetch(
                "/api/messages",
                {
                    cache:
                        "no-store"
                }
            );


        if (
            !response.ok
        ) {

            return;
        }


        const data =
            await response.json();


        // Backend already returns chronological order.

        for (
            const message
            of data
        ) {

            renderMessage(
                message
            );
        }


    } catch (error) {

        console.error(
            "Failed to load messages:",
            error
        );


    } finally {

        loadingMessages =
            false;
    }
}


/* ============================================================
   Load typing
   ============================================================ */

async function loadTyping() {

    try {

        const response =
            await fetch(
                "/api/typing",
                {
                    cache:
                        "no-store"
                }
            );


        if (
            !response.ok
        ) {

            return;
        }


        const data =
            await response.json();


        const users =
            Array.isArray(
                data.typing_users
            )
                ? data.typing_users
                : [];


        if (
            users.length > 0
        ) {

            typingIndicator.textContent =
                users
                    .map(
                        user =>
                            `${user} is typing...`
                    )
                    .join(
                        ", "
                    );

        } else {

            typingIndicator.textContent =
                "";
        }


    } catch (error) {

        console.error(
            "Failed to load typing:",
            error
        );
    }
}


/* ============================================================
   Send message
   ============================================================ */

async function sendMessage() {

    const content =
        input.value.trim();


    if (!content) {

        return;
    }


    clearTimeout(
        typingTimeout
    );


    if (
        isTyping
    ) {

        isTyping =
            false;

        await publishTyping(
            false
        );
    }


    sendButton.disabled =
        true;


    try {

        const response =
            await fetch(
                "/messages",
                {
                    method:
                        "POST",

                    headers: {
                        "Content-Type":
                            "application/json"
                    },

                    body:
                        JSON.stringify({
                            conversation_id:
                                "chat-1",

                            content:
                                content
                        })
                }
            );


        if (
            !response.ok
        ) {

            let errorMessage =
                "Failed to send message";


            try {

                const error =
                    await response.json();

                errorMessage =
                    error.detail ||
                    errorMessage;

            } catch (_) {

                // Keep default.
            }


            alert(
                errorMessage
            );


            return;
        }


        input.value =
            "";


        input.focus();


    } catch (error) {

        console.error(
            "Failed to send message:",
            error
        );


        alert(
            "Failed to send message"
        );


    } finally {

        sendButton.disabled =
            false;
    }
}


/* ============================================================
   Typing detection
   ============================================================ */

input.addEventListener(
    "input",
    function () {

        const hasText =
            input.value
                .trim()
                .length > 0;


        clearTimeout(
            typingTimeout
        );


        if (
            hasText &&
            !isTyping
        ) {

            isTyping =
                true;

            publishTyping(
                true
            );
        }


        if (
            !hasText &&
            isTyping
        ) {

            isTyping =
                false;

            publishTyping(
                false
            );

            return;
        }


        typingTimeout =
            setTimeout(
                async function () {

                    if (
                        isTyping
                    ) {

                        isTyping =
                            false;

                        await publishTyping(
                            false
                        );
                    }

                },
                1500
            );
    }
);


/* ============================================================
   Enter = send
   ============================================================ */

input.addEventListener(
    "keydown",
    function (event) {

        if (
            event.key
                === "Enter"
        ) {

            event.preventDefault();

            sendMessage();
        }
    }
);


/* ============================================================
   Initial load
   ============================================================ */

loadMessages();

loadTyping();


/* ============================================================
   Polling
   ============================================================ */

setInterval(
    loadMessages,
    500
);


setInterval(
    loadTyping,
    300
);

</script>


</body>

</html>
"""


# ============================================================
# Local development
# ============================================================

if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
    )