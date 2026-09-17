import os

import psycopg


DB_HOST = os.environ["POSTGRES_HOST"]
DB_PORT = os.environ["POSTGRES_PORT"]
DB_NAME = os.environ["POSTGRES_DB"]
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]


def get_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def init_db():
    """
    Create the chat messages table if it does not already exist.
    """

    with get_connection() as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    conversation_id VARCHAR(255) NOT NULL,
                    sender VARCHAR(255) NOT NULL,
                    receiver VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

        conn.commit()


def create_message(
    conversation_id: str,
    sender: str,
    receiver: str,
    content: str,
) -> dict:
    """
    Insert a chat message into PostgreSQL.

    Debezium watches this table and publishes
    the INSERT event to Kafka.
    """

    with get_connection() as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO messages (
                    conversation_id,
                    sender,
                    receiver,
                    content
                )
                VALUES (%s, %s, %s, %s)
                RETURNING
                    id,
                    conversation_id,
                    sender,
                    receiver,
                    content,
                    created_at;
                """,
                (
                    conversation_id,
                    sender,
                    receiver,
                    content,
                ),
            )

            row = cursor.fetchone()

        conn.commit()

    return {
        "id": row[0],
        "conversation_id": row[1],
        "sender": row[2],
        "receiver": row[3],
        "content": row[4],
        "created_at": row[5].isoformat(),
    }