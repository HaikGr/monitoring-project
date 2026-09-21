"""APP 1 — Kafka chat.

Save as main.py in app1, next to: chat_postgres.py, s3_import.py, metrics.py

Flow (unchanged):
  POST /messages -> Postgres INSERT -> Debezium -> Kafka (chat-messages)
                 -> consumer thread -> in-memory deque -> GET /api/messages
  Typing         -> Kafka (chat-typing) directly, no DB.
"""

import json
import os
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from chat_postgres import create_message, open_export_connection
from metrics import (
    HTTP_ERRORS,
    HTTP_REQUESTS,
    HTTP_REQUEST_DURATION,
    MESSAGE_CDC_LATENCY,
    MESSAGE_CONSUMER_LATENCY,
    MESSAGE_DB_LATENCY,
    MESSAGE_HTTP_DELIVERY_LATENCY,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "my-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "chat-messages")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "app1-chat")
KAFKA_TYPING_TOPIC = os.getenv("KAFKA_TYPING_TOPIC", "chat-typing")
APP_ID = os.getenv("APP_ID", "app1")
OTHER_APP_ID = os.getenv("OTHER_APP_ID", "app2")
APP_TITLE = os.getenv("APP_TITLE", "App 1 Chat")
CONVERSATION_ID = os.getenv("CONVERSATION_ID", "chat-1")
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "100"))
TYPING_TIMEOUT_MS = int(os.getenv("TYPING_TIMEOUT_MS", "4000"))

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

INSTANCE_ID = os.getenv("HOSTNAME", APP_ID)
CHAT_CONSUMER_GROUP = f"{KAFKA_GROUP_ID}-{INSTANCE_ID}"
TYPING_CONSUMER_GROUP = f"{KAFKA_GROUP_ID}-typing-{INSTANCE_ID}"

messages: deque[dict[str, Any]] = deque(maxlen=MAX_MESSAGES)
message_index: dict[str, dict[str, Any]] = {}
typing_users: dict[str, dict[str, Any]] = {}
state_lock = threading.Lock()
stop_event = threading.Event()

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": CHAT_CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
})
typing_consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": TYPING_CONSUMER_GROUP,
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
})
typing_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

consumer_thread: threading.Thread | None = None
typing_consumer_thread: threading.Thread | None = None


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def parse_timestamp_seconds(value: Any) -> float | None:
    """Debezium emits created_at as epoch micros by default; ISO strings also work."""
    if value is None or value == "":
        return None
    try:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            n = float(value)
            if n > 1e18:
                return n / 1e9
            if n > 1e15:
                return n / 1e6
            if n > 1e12:
                return n / 1e3
            return n
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


def to_iso(value: Any) -> Any:
    """Normalise created_at so the browser can always Date.parse() it."""
    seconds = parse_timestamp_seconds(value)
    if seconds is None:
        return value
    return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()


def sort_key(message: dict[str, Any]):
    ts = parse_timestamp_seconds(message.get("created_at")) or 0.0
    try:
        mid = int(message.get("id") or 0)
    except (TypeError, ValueError):
        mid = 0
    return ts, mid


def unwrap_cdc(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    payload = value.get("payload")
    return payload if isinstance(payload, dict) else value


def process_chat_record(raw_value: bytes) -> dict[str, Any] | None:
    value = json.loads(raw_value.decode("utf-8"))
    cdc = unwrap_cdc(value)
    if not cdc:
        return None
    if cdc.get("op") == "d":
        return None
    after = cdc.get("after")
    if not isinstance(after, dict):
        return None
    if after.get("id") is None or after.get("conversation_id") is None:
        return None
    if after.get("conversation_id") != CONVERSATION_ID:
        return None
    return {
        "id": after.get("id"),
        "conversation_id": after.get("conversation_id"),
        "sender": after.get("sender"),
        "receiver": after.get("receiver"),
        "content": after.get("content") or "",
        "created_at": to_iso(after.get("created_at")),
        "operation": cdc.get("op"),
        "_consumer_received_at": time.time(),
    }


def store_message(event: dict[str, Any]) -> None:
    """Insert/replace one message, keeping the deque sorted and the index in sync."""
    key = str(event["id"])
    with state_lock:
        existing = message_index.get(key)
        if existing is not None:
            # Preserve delivery bookkeeping across CDC updates of the same row.
            event["_consumer_received_at"] = existing.get(
                "_consumer_received_at", event["_consumer_received_at"]
            )
            event["_http_delivered"] = existing.get("_http_delivered", False)

        current = [m for m in messages if str(m.get("id")) != key]
        current.append(event)
        current.sort(key=sort_key)
        current = current[-MAX_MESSAGES:]

        messages.clear()
        messages.extend(current)
        message_index.clear()
        message_index.update({str(m.get("id")): m for m in current})


def consume_messages() -> None:
    consumer.subscribe([KAFKA_TOPIC])
    print(f"Chat consumer started: group={CHAT_CONSUMER_GROUP}", flush=True)
    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}", flush=True)
                continue
            started = time.perf_counter()
            try:
                raw = msg.value()
                if not raw:
                    continue
                event = process_chat_record(raw)
                if event is None:
                    continue

                created_ts = parse_timestamp_seconds(event.get("created_at"))
                if created_ts is not None:
                    latency = time.time() - created_ts
                    if latency >= 0:
                        MESSAGE_CDC_LATENCY.observe(latency)

                store_message(event)
                MESSAGE_CONSUMER_LATENCY.observe(time.perf_counter() - started)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                print(f"Invalid Kafka record ignored: {exc}", flush=True)
            except Exception as exc:
                print(f"Failed to process Kafka message: {exc!r}", flush=True)
    except KafkaException as exc:
        print(f"Kafka consumer exception: {exc}", flush=True)
    finally:
        try:
            consumer.close()
        except Exception:
            pass


def consume_typing_events() -> None:
    typing_consumer.subscribe([KAFKA_TYPING_TOPIC])
    print(f"Typing consumer started: group={TYPING_CONSUMER_GROUP}", flush=True)
    try:
        while not stop_event.is_set():
            msg = typing_consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Typing Kafka error: {msg.error()}", flush=True)
                continue
            try:
                raw = msg.value()
                if not raw:
                    continue
                event = json.loads(raw.decode("utf-8"))
                if event.get("conversation_id") != CONVERSATION_ID:
                    continue
                user_id = event.get("user_id")
                if not user_id or user_id == APP_ID:
                    continue
                ts = int(event.get("timestamp_ms") or now_ms())
                with state_lock:
                    previous = typing_users.get(user_id)
                    if previous and ts < previous["timestamp_ms"]:
                        continue
                    typing_users[user_id] = {
                        "is_typing": bool(event.get("is_typing", False)),
                        "timestamp_ms": ts,
                    }
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                print(f"Invalid typing record ignored: {exc}", flush=True)
            except Exception as exc:
                print(f"Failed to process typing event: {exc!r}", flush=True)
    except KafkaException as exc:
        print(f"Typing consumer exception: {exc}", flush=True)
    finally:
        try:
            typing_consumer.close()
        except Exception:
            pass


def on_typing_delivery(err, msg) -> None:
    if err is not None:
        print(f"Typing event delivery failed: {err}", flush=True)


def save_messages() -> Path:
    """COPY the messages table out to a local CSV."""
    file_path = DATA_DIR / f"exported_data_{uuid.uuid4()}.csv"
    connection = open_export_connection()
    try:
        with connection.cursor() as cursor:
            with file_path.open("wb") as file:
                with cursor.copy("COPY messages TO STDOUT WITH CSV HEADER") as copy:
                    for chunk in copy:
                        file.write(chunk)
        return file_path
    except Exception:
        file_path.unlink(missing_ok=True)
        raise
    finally:
        connection.close()


def upload_file(local_file: Path) -> str:
    from s3_import import upload_file_to_s3
    return upload_file_to_s3(local_file)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_thread, typing_consumer_thread
    stop_event.clear()
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    typing_consumer_thread = threading.Thread(target=consume_typing_events, daemon=True)
    consumer_thread.start()
    typing_consumer_thread.start()
    try:
        yield
    finally:
        stop_event.set()
        try:
            typing_producer.flush(5)
        except Exception:
            pass
        if consumer_thread:
            consumer_thread.join(5)
        if typing_consumer_thread:
            typing_consumer_thread.join(5)


app = FastAPI(title=f"Kafka Chat - {APP_ID}", lifespan=lifespan)


class MessageRequest(BaseModel):
    conversation_id: str
    content: str


class TypingRequest(BaseModel):
    conversation_id: str
    is_typing: bool


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    started = time.perf_counter()
    route = request.scope.get("route")
    # Use the route template, not the raw path, so metric labels stay bounded.
    label = getattr(route, "path", None) or request.url.path
    try:
        response = await call_next(request)
    except Exception:
        HTTP_REQUEST_DURATION.labels(method=request.method, route=label).observe(
            time.perf_counter() - started
        )
        HTTP_ERRORS.labels(method=request.method, route=label, status="500").inc()
        HTTP_REQUESTS.labels(method=request.method, route=label, status="500").inc()
        raise

    duration = time.perf_counter() - started
    route = request.scope.get("route")
    label = getattr(route, "path", None) or request.url.path
    status = str(response.status_code)
    HTTP_REQUESTS.labels(method=request.method, route=label, status=status).inc()
    HTTP_REQUEST_DURATION.labels(method=request.method, route=label).observe(duration)
    if response.status_code >= 400:
        HTTP_ERRORS.labels(method=request.method, route=label, status=status).inc()
    return response


@app.get("/metrics")
def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health():
    return {"status": "ok", "app_id": APP_ID, "instance_id": INSTANCE_ID}


@app.get("/api/debug")
def debug():
    with state_lock:
        return {
            "app_id": APP_ID,
            "instance_id": INSTANCE_ID,
            "conversation_id": CONVERSATION_ID,
            "kafka_topic": KAFKA_TOPIC,
            "typing_topic": KAFKA_TYPING_TOPIC,
            "chat_consumer_group": CHAT_CONSUMER_GROUP,
            "typing_consumer_group": TYPING_CONSUMER_GROUP,
            "cached_messages": len(messages),
            "typing_users": dict(typing_users),
        }


@app.post("/messages")
def send_message(message: MessageRequest):
    conversation_id = message.conversation_id.strip()
    content = message.content.strip()
    if not conversation_id:
        raise HTTPException(400, "Conversation ID is required")
    if not content:
        raise HTTPException(400, "Message cannot be empty")

    started = time.perf_counter()
    try:
        result = create_message(
            conversation_id=conversation_id,
            sender=APP_ID,
            receiver=OTHER_APP_ID,
            content=content,
        )
    except Exception as exc:
        raise HTTPException(500, f"Failed to create message: {exc}") from exc

    MESSAGE_DB_LATENCY.observe(time.perf_counter() - started)
    return result


@app.post("/typing")
def update_typing(typing: TypingRequest):
    conversation_id = typing.conversation_id.strip()
    if not conversation_id:
        raise HTTPException(400, "Conversation ID is required")
    event = {
        "event_type": "typing",
        "conversation_id": conversation_id,
        "user_id": APP_ID,
        "is_typing": typing.is_typing,
        "timestamp_ms": now_ms(),
    }
    try:
        typing_producer.produce(
            KAFKA_TYPING_TOPIC,
            key=f"{conversation_id}:{APP_ID}",
            value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            on_delivery=on_typing_delivery,
        )
        typing_producer.poll(0)
        return {"status": "sent", "event": event}
    except BufferError as exc:
        raise HTTPException(503, f"Typing queue full: {exc}") from exc
    except Exception as exc:
        raise HTTPException(500, f"Failed to publish typing event: {exc}") from exc


@app.get("/api/typing")
def get_typing():
    current = now_ms()
    with state_lock:
        stale = [
            user_id for user_id, state in typing_users.items()
            if current - int(state.get("timestamp_ms", 0)) > TYPING_TIMEOUT_MS
        ]
        for user_id in stale:
            typing_users.pop(user_id, None)
        users = [u for u, state in typing_users.items() if state.get("is_typing")]
    return {"typing_users": users}


@app.get("/api/messages")
def get_messages():
    request_time = time.time()
    response: list[dict[str, Any]] = []

    # One lock acquisition, one pass. The old version re-locked and rescanned
    # the deque for every message, which got quadratic under polling.
    with state_lock:
        for message in sorted(messages, key=sort_key):
            received = message.get("_consumer_received_at")
            if received is not None and not message.get("_http_delivered"):
                latency = request_time - received
                if latency >= 0:
                    MESSAGE_HTTP_DELIVERY_LATENCY.observe(latency)
                message["_http_delivered"] = True
            response.append(
                {k: v for k, v in message.items() if not k.startswith("_")}
            )
    return response


@app.post("/export-messages")
def export_messages():
    file_path: Path | None = None
    try:
        file_path = save_messages()
        s3_key = upload_file(file_path)
        bucket = os.environ["S3_BUCKET"]
        return {
            "status": "success",
            "s3_bucket": bucket,
            "s3_key": s3_key,
            "s3_uri": f"s3://{bucket}/{s3_key}",
        }
    except Exception as exc:
        raise HTTPException(500, f"Failed to export messages and upload to S3: {exc}") from exc
    finally:
        # Don't let the pod's disk fill up with old exports.
        if file_path is not None:
            file_path.unlink(missing_ok=True)


CHAT_HTML = """
<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>__APP_TITLE__</title>
<style>
body{margin:0;background:#eef2f7;font-family:Arial,sans-serif;min-height:100vh;display:flex;justify-content:center;align-items:center}
.chat{width:min(900px,96vw);height:min(860px,92vh);background:#fff;border-radius:18px;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 14px 45px rgba(0,0,0,.12)}
.header{padding:16px 20px;background:#111827;color:#fff}.head{display:flex;justify-content:space-between;align-items:center;gap:12px}.header h1{margin:0;font-size:20px}.header p{margin:5px 0 0;color:#cbd5e1;font-size:12px}
.actions{display:flex;gap:10px;align-items:center}.dot{width:8px;height:8px;background:#22c55e;border-radius:50%;display:inline-block}.dot.off{background:#ef4444}.status{font-size:12px;color:#d1fae5}
button{border:0;border-radius:10px;padding:11px 16px;cursor:pointer;font-weight:600}button:disabled{opacity:.6;cursor:not-allowed}#exportButton{background:#2563eb;color:#fff}#sendButton{background:#111827;color:#fff}
#messages{flex:1;overflow-y:auto;padding:20px;display:flex;flex-direction:column;gap:9px;background:#f8fafc}.row{display:flex}.mine{justify-content:flex-end}.theirs{justify-content:flex-start}.bubble{max-width:min(72%,620px);padding:9px 13px;border-radius:15px;background:#fff;border:1px solid #e5e7eb}.mine .bubble{background:#dbeafe;border:0;border-bottom-right-radius:5px}.theirs .bubble{border-bottom-left-radius:5px}.sender{font-size:10px;color:#64748b;margin-bottom:3px}.content{font-size:14px;white-space:pre-wrap;word-break:break-word}.time{font-size:10px;color:#94a3b8;margin-top:4px}.pending{opacity:.6}.typing{min-height:30px;padding:5px 16px;color:#64748b;font-size:12px;font-style:italic}.composer{display:flex;gap:10px;padding:13px;border-top:1px solid #e5e7eb}.composer input{flex:1;min-width:0;padding:12px 14px;border:1px solid #d1d5db;border-radius:11px;outline:0;font-size:14px}
@media(max-width:650px){.chat{width:100vw;height:100vh;border-radius:0}.status{display:none}.bubble{max-width:84%}}
</style></head>
<body><div class="chat"><div class="header"><div class="head"><div><h1>__APP_TITLE__</h1><p>PostgreSQL &rarr; Debezium &rarr; Kafka</p></div><div class="actions"><span class="status"><span class="dot" id="statusDot"></span><span id="statusText">Connected</span></span><button id="exportButton" onclick="exportMessages()">Export Messages</button></div></div></div>
<div id="messages"></div><div id="typingIndicator" class="typing"></div><div class="composer"><input id="messageInput" autocomplete="off" placeholder="Type a message..."><button id="sendButton" onclick="sendMessage()">Send</button></div></div>
<script>
const APP_ID="__APP_ID__", OTHER_APP_ID="__OTHER_APP_ID__", CONVERSATION_ID="__CONVERSATION_ID__";
const box=document.getElementById("messages"),input=document.getElementById("messageInput"),typing=document.getElementById("typingIndicator"),send=document.getElementById("sendButton"),exportBtn=document.getElementById("exportButton"),dot=document.getElementById("statusDot"),statusText=document.getElementById("statusText");
let typingActive=false,typingTimer=null,heartbeatTimer=null,loading=false,loadingTyping=false;
const store=new Map();
function setOnline(ok){dot.className="dot"+(ok?"":" off");statusText.textContent=ok?"Connected":"Reconnecting"}
function nearBottom(){return box.scrollHeight-box.scrollTop-box.clientHeight<100}
function time(v){if(!v)return"";const d=new Date(v);return Number.isNaN(d.getTime())?"":d.toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"})}
function cmp(a,b){const x=Date.parse(a.created_at||""),y=Date.parse(b.created_at||"");if(Number.isFinite(x)&&Number.isFinite(y)&&x!==y)return x-y;return Number(a.id||0)-Number(b.id||0)}
function render(){const stick=nearBottom();const frag=document.createDocumentFragment();const list=[...store.values()].sort(cmp);
for(const m of list){const row=document.createElement("div"),mine=String(m.sender||"")===APP_ID;row.className="row "+(mine?"mine":"theirs");
const b=document.createElement("div");b.className="bubble"+(m._server?"":" pending");
const s=document.createElement("div");s.className="sender";s.textContent=mine?"You":String(m.sender||OTHER_APP_ID);
const c=document.createElement("div");c.className="content";c.textContent=String(m.content||"");
const t=document.createElement("div");t.className="time";t.textContent=time(m.created_at);
b.append(s,c);if(t.textContent)b.appendChild(t);row.appendChild(b);frag.appendChild(row)}
box.replaceChildren(frag);if(stick)box.scrollTop=box.scrollHeight}
async function loadMessages(){if(loading)return;loading=true;try{const r=await fetch("/api/messages",{cache:"no-store"});if(!r.ok){setOnline(false);return}const data=await r.json();if(!Array.isArray(data))return;setOnline(true);
const ids=new Set(data.map(m=>String(m.id)));
for(const [id,m] of store){if(m._server&&!ids.has(id))store.delete(id)}
for(const m of data){if(m.id!=null)store.set(String(m.id),{...m,_server:true})}
render()}catch(e){setOnline(false);console.error(e)}finally{loading=false}}
async function publishTyping(flag){try{await fetch("/typing",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({conversation_id:CONVERSATION_ID,is_typing:flag})})}catch(e){console.error(e)}}
async function loadTyping(){if(loadingTyping)return;loadingTyping=true;try{const r=await fetch("/api/typing",{cache:"no-store"});if(!r.ok)return;const d=await r.json();const users=Array.isArray(d.typing_users)?d.typing_users:[];typing.textContent=users.map(u=>u+" is typing...").join(", ")}catch(e){console.error(e)}finally{loadingTyping=false}}
function stopTyping(){clearTimeout(typingTimer);clearInterval(heartbeatTimer);heartbeatTimer=null;if(typingActive){typingActive=false;publishTyping(false)}}
function startTyping(){if(!typingActive){typingActive=true;publishTyping(true);
// Re-send while the user keeps typing so the peer's 4s timeout never expires early.
heartbeatTimer=setInterval(()=>{if(typingActive)publishTyping(true)},2000)}}
async function sendMessage(){const content=input.value.trim();if(!content||send.disabled)return;stopTyping();send.disabled=true;
try{const r=await fetch("/messages",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({conversation_id:CONVERSATION_ID,content})});
let data=null;try{data=await r.json()}catch(_){}
if(!r.ok)throw new Error((data&&data.detail)||"Failed to send message");
if(data&&data.id!=null){store.set(String(data.id),{...data,_server:false});render();box.scrollTop=box.scrollHeight}
input.value=""}catch(e){console.error(e);alert(e.message||"Failed to send message")}finally{send.disabled=false;input.focus()}}
async function exportMessages(){exportBtn.disabled=true;exportBtn.textContent="Exporting...";
try{const r=await fetch("/export-messages",{method:"POST"});let d=null;try{d=await r.json()}catch(_){}
if(!r.ok)throw new Error((d&&d.detail)||"Export failed");
alert("Messages exported successfully!\\n\\n"+((d&&d.s3_uri)||"CSV uploaded to S3."))}
catch(e){console.error(e);alert(e.message||"Export failed")}finally{exportBtn.disabled=false;exportBtn.textContent="Export Messages"}}
input.addEventListener("input",()=>{const has=input.value.trim().length>0;
if(!has){stopTyping();return}
startTyping();clearTimeout(typingTimer);typingTimer=setTimeout(stopTyping,1500)});
input.addEventListener("blur",stopTyping);
window.addEventListener("beforeunload",stopTyping);
input.addEventListener("keydown",e=>{if(e.key==="Enter"){e.preventDefault();sendMessage()}});
loadMessages();loadTyping();setInterval(loadMessages,700);setInterval(loadTyping,700);
</script></body></html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return (
        CHAT_HTML
        .replace("__APP_TITLE__", APP_TITLE)
        .replace("__APP_ID__", APP_ID)
        .replace("__OTHER_APP_ID__", OTHER_APP_ID)
        .replace("__CONVERSATION_ID__", CONVERSATION_ID)
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)