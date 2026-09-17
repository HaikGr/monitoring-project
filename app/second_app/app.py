import asyncio
import json
import os
import threading
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

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

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "chat-messages",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "app1-chat",
)

KAFKA_TYPING_TOPIC = os.getenv(
    "KAFKA_TYPING_TOPIC",
    "chat-typing",
)

APP_ID = os.getenv(
    "APP_ID",
    "app1",
)

MAX_MESSAGES = int(
    os.getenv("MAX_MESSAGES", "100")
)

# Kubernetes normally sets HOSTNAME to the pod name.
# This makes every replica receive the complete Kafka stream
# instead of Kafka distributing messages between replicas.
INSTANCE_ID = os.getenv(
    "HOSTNAME",
    APP_ID,
)

# Every replica gets its own group.
CHAT_CONSUMER_GROUP = f"{KAFKA_GROUP_ID}-{INSTANCE_ID}"
TYPING_CONSUMER_GROUP = f"{KAFKA_GROUP_ID}-typing-{INSTANCE_ID}"


# ============================================================
# In-memory state
#
# IMPORTANT:
# Every replica receives the full Kafka stream because the
# consumer groups above are unique per replica.
# ============================================================

chat_messages: deque[dict[str, Any]] = deque(
    maxlen=MAX_MESSAGES
)

typing_users: dict[str, dict[str, Any]] = {}

state_lock = threading.Lock()


# ============================================================
# Kafka consumers
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


# ============================================================
# Kafka producer for typing
# ============================================================

typing_producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }
)


# ============================================================
# Thread lifecycle
# ============================================================

stop_event = threading.Event()

chat_consumer_thread: threading.Thread | None = None
typing_consumer_thread: threading.Thread | None = None


# ============================================================
# Utility
# ============================================================

def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def message_sort_key(message: dict[str, Any]) -> tuple[str, int]:
    """
    Sort by created_at first and id second.

    This is important because Kafka partitions do not guarantee
    a single global ordering across partitions.
    """

    created_at = message.get("created_at") or ""

    try:
        message_id = int(message.get("id") or 0)
    except (TypeError, ValueError):
        message_id = 0

    return (str(created_at), message_id)


# ============================================================
# Chat Kafka Consumer
# ============================================================

def consume_chat_messages() -> None:
    chat_consumer.subscribe([KAFKA_TOPIC])

    print(
        "Chat consumer started:"
        f" topic={KAFKA_TOPIC}"
        f" group={CHAT_CONSUMER_GROUP}"
        f" instance={INSTANCE_ID}"
        f" bootstrap={KAFKA_BOOTSTRAP_SERVERS}"
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

                value = json.loads(
                    raw_value.decode("utf-8")
                )

                # ====================================================
                # Debezium CDC event
                #
                # {
                #     "before": null,
                #     "after": {
                #         "id": 1,
                #         "conversation_id": "chat-1",
                #         "sender": "app1",
                #         "receiver": "app2",
                #         "content": "Hello",
                #         "created_at": "..."
                #     }
                # }
                # ====================================================

                after = value.get("after")

                # Ignore deletes/tombstones.
                if not after:
                    continue

                event = {
                    "id": after.get("id"),
                    "conversation_id": after.get(
                        "conversation_id"
                    ),
                    "sender": after.get("sender"),
                    "receiver": after.get("receiver"),
                    "content": after.get("content"),
                    "created_at": after.get("created_at"),
                }

                if not event["id"]:
                    continue

                if not event["conversation_id"]:
                    continue

                with state_lock:
                    # Replace an existing message with the same ID.
                    # This protects against duplicate delivery.
                    existing = [
                        item
                        for item in chat_messages
                        if item.get("id") == event["id"]
                    ]

                    for item in existing:
                        try:
                            chat_messages.remove(item)
                        except ValueError:
                            pass

                    chat_messages.append(event)

                    # Keep local state in chronological order.
                    ordered = sorted(
                        chat_messages,
                        key=message_sort_key,
                    )

                    chat_messages.clear()

                    for item in ordered[-MAX_MESSAGES:]:
                        chat_messages.append(item)

                print(
                    "Received chat message:"
                    f" instance={INSTANCE_ID}"
                    f" partition={msg.partition()}"
                    f" offset={msg.offset()}"
                    f" sender={event['sender']}"
                    f" content={event['content']!r}",
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
# Typing Kafka Consumer
# ============================================================

def consume_typing_events() -> None:
    typing_consumer.subscribe([KAFKA_TYPING_TOPIC])

    print(
        "Typing consumer started:"
        f" topic={KAFKA_TYPING_TOPIC}"
        f" group={TYPING_CONSUMER_GROUP}"
        f" instance={INSTANCE_ID}"
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

                event = json.loads(
                    raw_value.decode("utf-8")
                )

                user_id = event.get("user_id")

                if not user_id:
                    continue

                # Ignore our own typing events.
                if user_id == APP_ID:
                    continue

                event_timestamp = int(
                    event.get(
                        "timestamp_ms",
                        0,
                    )
                    or 0
                )

                # Backwards compatibility with events that do not
                # contain timestamp_ms.
                if event_timestamp <= 0:
                    event_timestamp = now_ms()

                is_typing = bool(
                    event.get(
                        "is_typing",
                        False,
                    )
                )

                with state_lock:
                    previous = typing_users.get(user_id)

                    # Ignore an older event arriving after a newer one.
                    if (
                        previous
                        and event_timestamp
                        < previous["timestamp_ms"]
                    ):
                        continue

                    typing_users[user_id] = {
                        "is_typing": is_typing,
                        "timestamp_ms": event_timestamp,
                    }

                print(
                    "Typing event received:"
                    f" instance={INSTANCE_ID}"
                    f" user={user_id}"
                    f" is_typing={is_typing}",
                    flush=True,
                )

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
    global chat_consumer_thread
    global typing_consumer_thread

    # Initialize PostgreSQL.
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
        f"Application started:"
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

        # Flush any pending typing events.
        try:
            typing_producer.flush(5)
        except Exception:
            pass

        # Give consumer threads a moment to stop.
        if chat_consumer_thread:
            chat_consumer_thread.join(timeout=5)

        if typing_consumer_thread:
            typing_consumer_thread.join(timeout=5)


# ============================================================
# FastAPI
# ============================================================

app = FastAPI(
    title="Kafka Chat",
    description=(
        "PostgreSQL + Debezium + Kafka chat application"
    ),
    lifespan=lifespan,
)


# ============================================================
# Request Models
# ============================================================

class MessageRequest(BaseModel):
    conversation_id: str
    content: str


class TypingRequest(BaseModel):
    conversation_id: str
    is_typing: bool


# ============================================================
# Health
# ============================================================

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "app_id": APP_ID,
        "instance_id": INSTANCE_ID,
    }


# ============================================================
# Debug
# ============================================================

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
# Send Typing Event
# ============================================================

@app.post("/typing")
def update_typing(
    typing: TypingRequest,
):
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

        # Keep your current simple synchronous behavior.
        typing_producer.flush(5)

        return {
            "status": "sent",
            "event": event,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to publish typing event: "
                f"{exc}"
            ),
        )


# ============================================================
# Get Typing Users
# ============================================================

@app.get("/api/typing")
def get_typing() -> dict[str, Any]:
    with state_lock:
        users = [
            user_id
            for user_id, state in typing_users.items()
            if state["is_typing"]
        ]

    return {
        "typing_users": users,
    }


# ============================================================
# Get Chat Messages
# ============================================================

@app.get("/api/messages")
def get_chat_messages() -> list[dict[str, Any]]:
    with state_lock:
        messages = list(chat_messages)

    # Always return a deterministic order, even when Kafka has
    # multiple partitions.
    messages.sort(
        key=message_sort_key
    )

    return messages


# ============================================================
# Create Chat Message
# ============================================================

@app.post("/messages")
def send_message(
    message: MessageRequest,
):
    content = message.content.strip()

    if not content:
        raise HTTPException(
            status_code=400,
            detail="Message cannot be empty",
        )

    try:
        # ========================================================
        # IMPORTANT:
        #
        # HTTP request
        #      ↓
        # PostgreSQL
        #      ↓
        # Debezium
        #      ↓
        # Kafka
        #      ↓
        # Kafka consumer
        #      ↓
        # local replica state
        #      ↓
        # /api/messages
        #
        # The application does NOT directly publish normal
        # chat messages to Kafka.
        # ========================================================

        created_message = create_message(
            conversation_id=message.conversation_id,
            sender=APP_ID,
            receiver="app2",
            content=content,
        )

        return created_message

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to create message: "
                f"{exc}"
            ),
        )


# ============================================================
# Chat Page
# ============================================================

@app.get(
    "/chat",
    response_class=HTMLResponse,
)
def chat_page():
    return """
<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">

    <meta
        name="viewport"
        content="width=device-width, initial-scale=1.0"
    >

    <title>Kafka Chat</title>

    <style>
        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font-family: Arial, sans-serif;
            background: #f5f5f5;
        }

        .container {
            width: 90%;
            max-width: 800px;
            margin: 40px auto;
        }

        h1 {
            text-align: center;
            margin-bottom: 20px;
        }

        #messages {
            height: 500px;
            overflow-y: auto;
            background: white;
            border: 1px solid #ddd;
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 10px;
        }

        .message {
            margin: 8px 0;
            padding: 10px 14px;
            border-radius: 12px;
            max-width: 70%;
            word-wrap: break-word;
        }

        .mine {
            margin-left: auto;
            background: #d9fdd3;
            text-align: right;
        }

        .theirs {
            margin-right: auto;
            background: #eeeeee;
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

        #typingIndicator {
            min-height: 24px;
            padding: 4px 15px;
            font-size: 13px;
            font-style: italic;
            color: #666;
        }

        #composer {
            display: flex;
            gap: 10px;
        }

        #messageInput {
            flex: 1;
            padding: 12px;
            border: 1px solid #ccc;
            border-radius: 8px;
            font-size: 16px;
        }

        button {
            padding: 12px 20px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            background: #2563eb;
            color: white;
            font-size: 16px;
        }

        button:disabled {
            opacity: 0.6;
            cursor: not-allowed;
        }
    </style>
</head>

<body>

<div class="header">

    <h1>
        App 2 Chat
    </h1>

    <p>
        PostgreSQL → Debezium → Kafka
    </p>

</div>

<div class="container">

    <h1>Kafka Chat</h1>

    <div id="messages"></div>

    <div id="typingIndicator"></div>

    <div id="composer">

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

const messagesContainer =
    document.getElementById("messages");

const input =
    document.getElementById("messageInput");

const typingIndicator =
    document.getElementById("typingIndicator");

const sendButton =
    document.getElementById("sendButton");


const displayedMessages =
    new Set();

let isTyping = false;
let typingTimeout = null;
let loadingMessages = false;


/* ============================================================
   Publish typing event
   ============================================================ */

async function publishTyping(isCurrentlyTyping) {

    try {

        const response = await fetch(
            "/typing",
            {
                method: "POST",

                headers: {
                    "Content-Type":
                        "application/json"
                },

                body: JSON.stringify({
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
   Add message to UI
   ============================================================ */

function addMessage(message) {

    if (!message.id) {
        return;
    }

    const messageId =
        String(message.id);

    if (displayedMessages.has(messageId)) {
        return;
    }

    displayedMessages.add(messageId);

    const div =
        document.createElement("div");

    const sender =
        String(message.sender || "");

    div.className =
        "message " +
        (
            sender === "app1"
                ? "mine"
                : "theirs"
        );

    div.dataset.messageId =
        messageId;

    div.textContent =
        (
            sender === "app1"
                ? "You: "
                : "App 2: "
        ) +
        String(message.content || "");

    messagesContainer.appendChild(div);

    messagesContainer.scrollTop =
        messagesContainer.scrollHeight;
}


/* ============================================================
   Load messages
   ============================================================ */

async function loadMessages() {

    if (loadingMessages) {
        return;
    }

    loadingMessages = true;

    try {

        const response =
            await fetch(
                "/api/messages",
                {
                    cache: "no-store"
                }
            );

        if (!response.ok) {
            return;
        }

        const data =
            await response.json();

        // Backend already sorts messages chronologically.
        for (const message of data) {
            addMessage(message);
        }

    } catch (error) {

        console.error(
            "Failed to load messages:",
            error
        );

    } finally {

        loadingMessages = false;
    }
}


/* ============================================================
   Load typing state
   ============================================================ */

async function loadTyping() {

    try {

        const response =
            await fetch(
                "/api/typing",
                {
                    cache: "no-store"
                }
            );

        if (!response.ok) {
            return;
        }

        const data =
            await response.json();

        const users =
            Array.isArray(data.typing_users)
                ? data.typing_users
                : [];

        if (users.length > 0) {

            typingIndicator.textContent =
                users
                    .map(
                        user =>
                            `${user} is typing...`
                    )
                    .join(", ");

        } else {

            typingIndicator.textContent = "";
        }

    } catch (error) {

        console.error(
            "Failed to load typing state:",
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

    clearTimeout(typingTimeout);

    if (isTyping) {

        isTyping = false;

        // Wait so the "false" event is actually sent
        // before sending the message.
        await publishTyping(false);
    }

    sendButton.disabled = true;

    try {

        const response =
            await fetch(
                "/messages",
                {
                    method: "POST",

                    headers: {
                        "Content-Type":
                            "application/json"
                    },

                    body: JSON.stringify({
                        conversation_id:
                            "chat-1",

                        content:
                            content
                    })
                }
            );

        if (!response.ok) {

            let errorMessage =
                "Failed to send message";

            try {
                const error =
                    await response.json();

                errorMessage =
                    error.detail ||
                    errorMessage;

            } catch (_) {
                // Keep default error message.
            }

            alert(errorMessage);

            return;
        }

        input.value = "";

        input.focus();

        // We intentionally do NOT add the message here.
        //
        // PostgreSQL receives it first.
        // Debezium publishes it.
        // Kafka consumer receives it.
        // /api/messages exposes it.
        //
        // This preserves your intended CDC flow.

    } catch (error) {

        console.error(
            "Failed to send message:",
            error
        );

        alert(
            "Failed to send message"
        );

    } finally {

        sendButton.disabled = false;
    }
}


/* ============================================================
   Typing detection
   ============================================================ */

input.addEventListener(
    "input",
    function () {

        const hasText =
            input.value.trim().length > 0;

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

        typingTimeout =
            setTimeout(
                async function () {

                    if (isTyping) {

                        isTyping = false;

                        await publishTyping(false);
                    }

                },
                1500
            );
    }
);


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
   Enter = Send
   ============================================================ */

input.addEventListener(
    "keydown",
    function (event) {

        if (event.key === "Enter") {

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
