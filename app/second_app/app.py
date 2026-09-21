# second app

import asyncio
import json
import os
import threading
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import uuid

import psycopg
from confluent_kafka import Consumer, KafkaException, Producer
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from postgres import init_db, create_message


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "my-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "chat-messages")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "app2-chat")
KAFKA_TYPING_TOPIC = os.getenv("KAFKA_TYPING_TOPIC", "chat-typing")

APP_ID = os.getenv("APP_ID", "app2")
OTHER_APP_ID = os.getenv("OTHER_APP_ID", "app1")

MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "100"))
TYPING_TIMEOUT_MS = int(os.getenv("TYPING_TIMEOUT_MS", "4000"))


# ============================================================
# Instance identity / consumer groups
# ============================================================

INSTANCE_ID = os.getenv("HOSTNAME", APP_ID)
CHAT_CONSUMER_GROUP = f"{KAFKA_GROUP_ID}-{INSTANCE_ID}"
TYPING_CONSUMER_GROUP = f"{KAFKA_GROUP_ID}-typing-{INSTANCE_ID}"


# ============================================================
# PostgreSQL export configuration
# ============================================================

DB_HOST = os.environ["POSTGRES_HOST"]
DB_PORT = os.environ["POSTGRES_PORT"]
DB_NAME = os.environ["POSTGRES_DB"]
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def save_messages():
    """Export PostgreSQL messages to a CSV file in the pod."""

    file_path = DATA_DIR / f"exported_data_{uuid.uuid4()}.csv"

    connection = psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=5,
        options="-c statement_timeout=30000",
    )

    try:
        with connection.cursor() as cursor:
            with file_path.open("wb") as file:
                with cursor.copy(
                    """
                    COPY messages TO STDOUT
                    WITH CSV HEADER
                    """
                ) as copy:
                    for chunk in copy:
                        file.write(chunk)

        print(
            f"Messages exported to: {file_path}",
            flush=True,
        )
        return file_path

    finally:
        connection.close()


# ============================================================
# In-memory state
# ============================================================

chat_messages: deque[dict[str, Any]] = deque(maxlen=MAX_MESSAGES)
typing_users: dict[str, dict[str, Any]] = {}
state_lock = threading.Lock()


# ============================================================
# Kafka clients
# ============================================================

chat_consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CHAT_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
)

typing_consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": TYPING_CONSUMER_GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
)

typing_producer = Producer(
    {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
)

stop_event = threading.Event()
chat_consumer_thread: threading.Thread | None = None
typing_consumer_thread: threading.Thread | None = None


# ============================================================
# Utilities
# ============================================================

def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def message_sort_key(message: dict[str, Any]) -> tuple[str, int]:
    created_at = message.get("created_at") or ""

    try:
        message_id = int(message.get("id") or 0)
    except (TypeError, ValueError):
        message_id = 0

    return str(created_at), message_id


# ============================================================
# Chat Kafka consumer
# ============================================================

def consume_chat_messages() -> None:
    chat_consumer.subscribe([KAFKA_TOPIC])

    print(
        "Chat consumer started:"
        f" topic={KAFKA_TOPIC}"
        f" group={CHAT_CONSUMER_GROUP}"
        f" instance={INSTANCE_ID}",
        flush=True,
    )

    try:
        while not stop_event.is_set():
            msg = chat_consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(
                    f"Kafka chat error: {msg.error()}",
                    flush=True,
                )
                continue

            try:
                raw_value = msg.value()
                if raw_value is None:
                    continue

                value = json.loads(raw_value.decode("utf-8"))
                after = value.get("after")

                if not after:
                    continue

                event = {
                    "id": after.get("id"),
                    "conversation_id": after.get("conversation_id"),
                    "sender": after.get("sender"),
                    "receiver": after.get("receiver"),
                    "content": after.get("content"),
                    "created_at": after.get("created_at"),
                }

                if not event["id"] or not event["conversation_id"]:
                    continue

                with state_lock:
                    existing = [
                        item
                        for item in chat_messages
                        if item.get("id") != event["id"]
                    ]
                    existing.append(event)

                    ordered = sorted(
                        existing,
                        key=message_sort_key,
                    )
                    chat_messages.clear()
                    chat_messages.extend(ordered[-MAX_MESSAGES:])

                print(
                    "Received chat message:"
                    f" instance={INSTANCE_ID}"
                    f" partition={msg.partition()}"
                    f" offset={msg.offset()}"
                    f" sender={event['sender']}",
                    flush=True,
                )

            except Exception as exc:
                print(
                    f"Failed to process Kafka chat message: {exc}",
                    flush=True,
                )

    except KafkaException as exc:
        print(
            f"Chat Kafka consumer stopped: {exc}",
            flush=True,
        )
    except Exception as exc:
        print(
            f"Chat consumer stopped unexpectedly: {exc}",
            flush=True,
        )
    finally:
        try:
            chat_consumer.close()
        except Exception:
            pass


# ============================================================
# Typing Kafka consumer
# ============================================================

def consume_typing_events() -> None:
    typing_consumer.subscribe([KAFKA_TYPING_TOPIC])

    print(
        "Typing consumer started:"
        f" topic={KAFKA_TYPING_TOPIC}"
        f" group={TYPING_CONSUMER_GROUP}"
        f" instance={INSTANCE_ID}",
        flush=True,
    )

    try:
        while not stop_event.is_set():
            msg = typing_consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(
                    f"Typing Kafka error: {msg.error()}",
                    flush=True,
                )
                continue

            try:
                raw_value = msg.value()
                if raw_value is None:
                    continue

                event = json.loads(raw_value.decode("utf-8"))
                user_id = event.get("user_id")

                if not user_id or user_id == APP_ID:
                    continue

                event_timestamp = int(event.get("timestamp_ms") or 0)
                if event_timestamp <= 0:
                    event_timestamp = now_ms()

                is_typing = bool(event.get("is_typing", False))

                with state_lock:
                    previous = typing_users.get(user_id)

                    if (
                        previous
                        and event_timestamp < previous["timestamp_ms"]
                    ):
                        continue

                    typing_users[user_id] = {
                        "is_typing": is_typing,
                        "timestamp_ms": event_timestamp,
                    }

            except Exception as exc:
                print(
                    f"Failed to process typing event: {exc}",
                    flush=True,
                )

    except KafkaException as exc:
        print(
            f"Typing Kafka consumer stopped: {exc}",
            flush=True,
        )
    except Exception as exc:
        print(
            f"Typing consumer stopped unexpectedly: {exc}",
            flush=True,
        )
    finally:
        try:
            typing_consumer.close()
        except Exception:
            pass


# ============================================================
# FastAPI lifespan
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global chat_consumer_thread, typing_consumer_thread

    # Keep your existing DB initialization behavior, but run it
    # off the event loop so it does not block asynchronous startup code.
    await asyncio.to_thread(init_db)

    stop_event.clear()

    chat_consumer_thread = threading.Thread(
        target=consume_chat_messages,
        name=f"chat-consumer-{INSTANCE_ID}",
        daemon=True,
    )

    typing_consumer_thread = threading.Thread(
        target=consume_typing_events,
        name=f"typing-consumer-{INSTANCE_ID}",
        daemon=True,
    )

    chat_consumer_thread.start()
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
            f"Application shutting down: instance={INSTANCE_ID}",
            flush=True,
        )

        stop_event.set()

        try:
            typing_producer.flush(5)
        except Exception:
            pass

        if chat_consumer_thread:
            chat_consumer_thread.join(timeout=5)

        if typing_consumer_thread:
            typing_consumer_thread.join(timeout=5)


# ============================================================
# FastAPI
# ============================================================

app = FastAPI(
    title="Kafka Chat - App 2",
    description="PostgreSQL + Debezium + Kafka chat application",
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
# Health / debug
# ============================================================

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "app_id": APP_ID,
        "instance_id": INSTANCE_ID,
    }


@app.get("/api/debug")
def debug():
    with state_lock:
        return {
            "app_id": APP_ID,
            "instance_id": INSTANCE_ID,
            "chat_consumer_group": CHAT_CONSUMER_GROUP,
            "typing_consumer_group": TYPING_CONSUMER_GROUP,
            "cached_messages": len(chat_messages),
            "typing_users": dict(typing_users),
        }


# ============================================================
# Export messages
# ============================================================

@app.post("/export-messages")
def export_messages():
    save_messages()
    return {"status": "export completed"}


# ============================================================
# Typing API
# ============================================================

@app.post("/typing")
def update_typing(typing: TypingRequest):
    event = {
        "event_type": "typing",
        "conversation_id": typing.conversation_id,
        "user_id": APP_ID,
        "is_typing": typing.is_typing,
        "timestamp_ms": now_ms(),
    }

    try:
        typing_producer.produce(
            KAFKA_TYPING_TOPIC,
            key=f"{typing.conversation_id}:{APP_ID}",
            value=json.dumps(event).encode("utf-8"),
        )
        typing_producer.poll(0)

        return {
            "status": "sent",
            "event": event,
        }

    except BufferError:
        try:
            typing_producer.flush(1)
            typing_producer.produce(
                KAFKA_TYPING_TOPIC,
                key=f"{typing.conversation_id}:{APP_ID}",
                value=json.dumps(event).encode("utf-8"),
            )
            typing_producer.poll(0)
            return {"status": "sent", "event": event}
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to publish typing event: {exc}",
            )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to publish typing event: {exc}",
        )


@app.get("/api/typing")
def get_typing() -> dict[str, Any]:
    now = now_ms()

    with state_lock:
        stale_users = [
            user_id
            for user_id, state in typing_users.items()
            if now - int(state.get("timestamp_ms", 0)) > TYPING_TIMEOUT_MS
        ]

        for user_id in stale_users:
            typing_users.pop(user_id, None)

        users = [
            user_id
            for user_id, state in typing_users.items()
            if state.get("is_typing")
        ]

    return {"typing_users": users}


# ============================================================
# Messages
# ============================================================

@app.get("/api/messages")
def get_chat_messages() -> list[dict[str, Any]]:
    with state_lock:
        result = list(chat_messages)

    result.sort(key=message_sort_key)
    return result


@app.post("/messages")
def send_message(message: MessageRequest):
    content = message.content.strip()

    if not content:
        raise HTTPException(
            status_code=400,
            detail="Message cannot be empty",
        )

    try:
        created_message = create_message(
            conversation_id=message.conversation_id,
            sender=APP_ID,
            receiver=OTHER_APP_ID,
            content=content,
        )

        return created_message

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create message: {exc}",
        )


# ============================================================
# Shared chat frontend
# ============================================================

CHAT_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>__APP_TITLE__</title>
    <style>
        * { box-sizing: border-box; }

        body {
            margin: 0;
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            background: #eef2f7;
            font-family: Arial, sans-serif;
            color: #111827;
        }

        .chat {
            width: min(900px, 96vw);
            height: min(860px, 92vh);
            background: white;
            border-radius: 18px;
            box-shadow: 0 14px 45px rgba(0, 0, 0, 0.12);
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }

        .header {
            padding: 16px 20px;
            background: #111827;
            color: white;
        }

        .header-content {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 16px;
        }

        .header h1 {
            margin: 0;
            font-size: 20px;
        }

        .header p {
            margin: 5px 0 0;
            font-size: 12px;
            color: #cbd5e1;
        }

        .header-actions {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .status {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            color: #d1fae5;
            font-size: 12px;
            white-space: nowrap;
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #22c55e;
        }

        button {
            border: 0;
            padding: 11px 16px;
            border-radius: 10px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
        }

        button:disabled {
            opacity: 0.6;
            cursor: not-allowed;
        }

        #exportButton {
            background: #2563eb;
            color: white;
        }

        #exportButton:hover:not(:disabled) {
            background: #1d4ed8;
        }

        #messages {
            flex: 1;
            overflow-y: auto;
            padding: 20px;
            display: flex;
            flex-direction: column;
            gap: 9px;
            background: #f8fafc;
        }

        .message-row {
            display: flex;
            width: 100%;
        }

        .message-row.mine { justify-content: flex-end; }
        .message-row.theirs { justify-content: flex-start; }

        .message {
            max-width: min(72%, 620px);
            padding: 9px 13px;
            border-radius: 15px;
            line-height: 1.4;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
        }

        .mine .message {
            background: #dbeafe;
            border-bottom-right-radius: 5px;
        }

        .theirs .message {
            background: white;
            border: 1px solid #e5e7eb;
            border-bottom-left-radius: 5px;
        }

        .sender {
            font-size: 10px;
            color: #64748b;
            margin-bottom: 3px;
        }

        .content {
            font-size: 14px;
            word-break: break-word;
            white-space: pre-wrap;
        }

        .message-time {
            margin-top: 4px;
            font-size: 10px;
            color: #94a3b8;
        }

        .typing {
            min-height: 30px;
            padding: 5px 16px 2px;
            font-size: 12px;
            color: #64748b;
            font-style: italic;
            background: #fff;
        }

        .composer {
            display: flex;
            gap: 10px;
            padding: 13px;
            border-top: 1px solid #e5e7eb;
            background: white;
        }

        #messageInput {
            flex: 1;
            min-width: 0;
            padding: 12px 14px;
            border: 1px solid #d1d5db;
            border-radius: 11px;
            outline: none;
            font-size: 14px;
        }

        #messageInput:focus {
            border-color: #2563eb;
            box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.12);
        }

        #sendButton {
            background: #111827;
            color: white;
            min-width: 82px;
        }

        #sendButton:hover:not(:disabled) { background: #1f2937; }

        @media (max-width: 650px) {
            .chat {
                width: 100vw;
                height: 100vh;
                border-radius: 0;
            }

            .message { max-width: 84%; }
            .status { display: none; }
            .header { padding: 14px; }
            #exportButton { padding: 10px 12px; }
        }
    </style>
</head>
<body>
<div class="chat">
    <div class="header">
        <div class="header-content">
            <div>
                <h1>__APP_TITLE__</h1>
                <p>PostgreSQL → Debezium → Kafka</p>
            </div>
            <div class="header-actions">
                <span class="status">
                    <span class="status-dot"></span>
                    Connected
                </span>
                <button id="exportButton" onclick="exportMessages()">
                    Export Messages
                </button>
            </div>
        </div>
    </div>

    <div id="messages"></div>
    <div id="typingIndicator" class="typing"></div>

    <div class="composer">
        <input
            id="messageInput"
            type="text"
            placeholder="Type a message..."
            autocomplete="off"
        >
        <button id="sendButton" onclick="sendMessage()">Send</button>
    </div>
</div>

<script>
const APP_ID = "__APP_ID__";
const OTHER_APP_ID = "__OTHER_APP_ID__";

const messagesElement = document.getElementById("messages");
const input = document.getElementById("messageInput");
const typingIndicator = document.getElementById("typingIndicator");
const sendButton = document.getElementById("sendButton");
const exportButton = document.getElementById("exportButton");

let isTyping = false;
let typingTimeout = null;
let loadingMessages = false;
const displayedIds = new Set();

function isNearBottom() {
    return messagesElement.scrollHeight - messagesElement.scrollTop - messagesElement.clientHeight < 90;
}

function formatTime(value) {
    if (!value) return "";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "";
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function renderMessage(message, forceScroll = false) {
    if (!message || !message.id) return;

    const messageId = String(message.id);
    if (displayedIds.has(messageId)) return;

    const shouldScroll = forceScroll || isNearBottom();
    displayedIds.add(messageId);

    const row = document.createElement("div");
    const mine = String(message.sender || "") === APP_ID;
    row.className = "message-row " + (mine ? "mine" : "theirs");

    const bubble = document.createElement("div");
    bubble.className = "message";

    const sender = document.createElement("div");
    sender.className = "sender";
    sender.textContent = mine ? "You" : String(message.sender || OTHER_APP_ID);

    const content = document.createElement("div");
    content.className = "content";
    content.textContent = String(message.content || "");

    const time = document.createElement("div");
    time.className = "message-time";
    time.textContent = formatTime(message.created_at);

    bubble.appendChild(sender);
    bubble.appendChild(content);
    if (time.textContent) bubble.appendChild(time);

    row.appendChild(bubble);
    messagesElement.appendChild(row);

    if (shouldScroll) messagesElement.scrollTop = messagesElement.scrollHeight;
}

async function loadMessages() {
    if (loadingMessages) return;
    loadingMessages = true;

    try {
        const response = await fetch("/api/messages", { cache: "no-store" });
        if (!response.ok) return;

        const data = await response.json();
        for (const message of data) renderMessage(message);
    } catch (error) {
        console.error("Failed to load messages:", error);
    } finally {
        loadingMessages = false;
    }
}

async function publishTyping(isCurrentlyTyping) {
    try {
        await fetch("/typing", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                conversation_id: "chat-1",
                is_typing: isCurrentlyTyping
            })
        });
    } catch (error) {
        console.error("Failed to publish typing event:", error);
    }
}

async function loadTyping() {
    try {
        const response = await fetch("/api/typing", { cache: "no-store" });
        if (!response.ok) return;

        const data = await response.json();
        const users = Array.isArray(data.typing_users) ? data.typing_users : [];

        typingIndicator.textContent = users.length
            ? users.map(user => `${user} is typing...`).join(", ")
            : "";
    } catch (error) {
        console.error("Failed to load typing:", error);
    }
}

async function sendMessage() {
    const content = input.value.trim();
    if (!content || sendButton.disabled) return;

    clearTimeout(typingTimeout);

    if (isTyping) {
        isTyping = false;
        publishTyping(false);
    }

    sendButton.disabled = true;

    try {
        const response = await fetch("/messages", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                conversation_id: "chat-1",
                content: content
            })
        });

        if (!response.ok) {
            let errorMessage = "Failed to send message";
            try {
                const error = await response.json();
                errorMessage = error.detail || errorMessage;
            } catch (_) {}
            alert(errorMessage);
            return;
        }

        const createdMessage = await response.json();
        renderMessage(createdMessage, true);

        input.value = "";
        input.focus();
    } catch (error) {
        console.error("Failed to send message:", error);
        alert("Failed to send message");
    } finally {
        sendButton.disabled = false;
    }
}

async function exportMessages() {
    exportButton.disabled = true;
    exportButton.textContent = "Exporting...";

    try {
        const response = await fetch("/export-messages", { method: "POST" });

        if (!response.ok) {
            let errorMessage = "Failed to export messages";
            try {
                const error = await response.json();
                errorMessage = error.detail || errorMessage;
            } catch (_) {}
            throw new Error(errorMessage);
        }

        alert("Messages exported successfully!");
    } catch (error) {
        console.error("Failed to export messages:", error);
        alert(error.message || "Failed to export messages");
    } finally {
        exportButton.disabled = false;
        exportButton.textContent = "Export Messages";
    }
}

input.addEventListener("input", () => {
    const hasText = input.value.trim().length > 0;
    clearTimeout(typingTimeout);

    if (hasText && !isTyping) {
        isTyping = true;
        publishTyping(true);
    }

    if (!hasText && isTyping) {
        isTyping = false;
        publishTyping(false);
        return;
    }

    if (hasText) {
        typingTimeout = setTimeout(() => {
            if (!isTyping) return;
            isTyping = false;
            publishTyping(false);
        }, 1200);
    }
});

input.addEventListener("keydown", event => {
    if (event.key === "Enter") {
        event.preventDefault();
        sendMessage();
    }
});

loadMessages();
loadTyping();
setInterval(loadMessages, 300);
setInterval(loadTyping, 250);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return (
        CHAT_HTML
        .replace("__APP_TITLE__", "App 2 Chat")
        .replace("__APP_ID__", APP_ID)
        .replace("__OTHER_APP_ID__", OTHER_APP_ID)
    )


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
