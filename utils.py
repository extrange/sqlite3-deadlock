import sqlite3
import uuid
from pathlib import Path


def init_db(dir: str):
    db_path = Path(dir) / str(uuid.uuid4())
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
    conn.commit()
    return db_path


conn_from_another_file = sqlite3.Connection(
    f"file:connection_in_another_file?mode=memory&cache=shared", check_same_thread=False
)
