"""Postgres access for APP 2.

main.py (app2) does: `from postgres import init_db, create_message`
Save this as postgres.py in app2.
"""

import os
from typing import Any

import psycopg
from psycopg_pool import ConnectionPool

DB_HOST = os.environ["POSTGRES_HOST"]
DB_PORT = os.environ["POSTGRES_PORT"]
DB_NAME = os.environ["POSTGRES_DB"]
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]

POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))
STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "10000"))

CONNINFO = (
    f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
    f"user={DB_USER} password={DB_PASSWORD} connect_timeout=5"
)

pool = ConnectionPool(
    conninfo=CONNINFO,
    min_size=POOL_MIN,
    max_size=POOL_MAX,
    timeout=10,
    kwargs={"options": f"-c statement_timeout={STATEMENT_TIMEOUT_MS}"},
    open=True,
)


def init_db() -> None:
    """Create the messages table if missing. Safe to run on every startup.

    REPLICA IDENTITY FULL makes Debezium emit the complete row in `before`
    on UPDATE/DELETE. Harmless for INSERT-only chat, but keeps CDC consistent.
    """
    with pool.connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id              BIGSERIAL PRIMARY KEY,
                    conversation_id VARCHAR(255) NOT NULL,
                    sender          VARCHAR(255) NOT NULL,
                    receiver        VARCHAR(255) NOT NULL,
                    content         TEXT NOT NULL,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_messages_conversation_created
                ON messages (conversation_id, created_at);
                """
            )
            cursor.execute("ALTER TABLE messages REPLICA IDENTITY FULL;")
    print("Database initialised.", flush=True)


def create_message(
    conversation_id: str,
    sender: str,
    receiver: str,
    content: str,
) -> dict[str, Any]:
    """Insert one message and return it. Debezium picks the row up from the WAL."""
    with pool.connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO messages (conversation_id, sender, receiver, content)
                VALUES (%s, %s, %s, %s)
                RETURNING id, conversation_id, sender, receiver, content, created_at;
                """,
                (conversation_id, sender, receiver, content),
            )
            row = cursor.fetchone()

    return {
        "id": row[0],
        "conversation_id": row[1],
        "sender": row[2],
        "receiver": row[3],
        "content": row[4],
        "created_at": row[5].isoformat(),
    }


def open_export_connection() -> psycopg.Connection:
    """Dedicated connection for COPY exports (long statement timeout)."""
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=5,
        options="-c statement_timeout=60000",
    )