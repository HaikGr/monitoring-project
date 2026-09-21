"""Postgres access for APP 1.

main.py (app1) does: `from chat_postgres import create_message`
Save this as chat_postgres.py in app1.
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

# One pool for the process. Opening a fresh connection on every POST /messages
# is what made the endpoint hang whenever Postgres was slow.
pool = ConnectionPool(
    conninfo=CONNINFO,
    min_size=POOL_MIN,
    max_size=POOL_MAX,
    timeout=10,
    kwargs={"options": f"-c statement_timeout={STATEMENT_TIMEOUT_MS}"},
    open=True,
)


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