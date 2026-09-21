import os
import uuid
from pathlib import Path

import psycopg

DB_HOST = os.environ["POSTGRES_HOST"]
DB_PORT = os.environ["POSTGRES_PORT"]
DB_NAME = os.environ["POSTGRES_DB"]
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def create_message(
    conversation_id: str,
    sender: str,
    receiver: str,
    content: str,
) -> dict:
    connection = psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    try:
        with connection.cursor() as cursor:
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
            connection.commit()

            return {
                "id": row[0],
                "conversation_id": row[1],
                "sender": row[2],
                "receiver": row[3],
                "content": row[4],
                "created_at": row[5].isoformat(),
            }

    finally:
        connection.close()


def save_messages() -> Path:
    """
    Export all messages from PostgreSQL into a CSV file.

    Returns:
        Path to the generated CSV file.
    """

    connection = psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    file_path = (
        DATA_DIR
        / f"exported_data_{uuid.uuid4()}.csv"
    )

    try:
        with connection.cursor() as cursor:
            with file_path.open("wb") as f:
                with cursor.copy(
                    """
                    COPY messages TO STDOUT
                    WITH CSV HEADER
                    """
                ) as copy:
                    for data in copy:
                        f.write(data)

        print(
            f"Messages exported to: {file_path}",
            flush=True,
        )

        return file_path

    finally:
        connection.close()