import sqlite3
import uuid

def init_mem_db():
    db_name = str(uuid.uuid4())
    conn = sqlite3.connect(f"file:{db_name}?mode=memory&cache=shared")
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
    conn.commit()

    # Don't close or else database will be deleted
    return db_name