# Demo of 2 connections simultaneously INSERTing, until one fails and holds a reserved lock permanently, preventing subsequent writes
import sqlite3
import threading
import uuid
import time
from multiprocessing import Process
import traceback

db = "file:app.db"

print(f'{db=}')

conn = sqlite3.connect(db)
conn1 = sqlite3.connect(db, check_same_thread=False)
conn2 = sqlite3.connect(db)

# Disable caching to slow performance down further
conn.execute("PRAGMA cache_size=-1")
conn1.execute("PRAGMA cache_size=-1")
conn2.execute("PRAGMA cache_size=-1")


# Initialize
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT UNIQUE);")
conn.executemany("INSERT INTO users (name) VALUES (?)", [(str(uuid.uuid4()),) for _ in range(1_000_000)])
conn.commit()
print("Database created.")

def start_selects(thread_conn: sqlite3.Connection):
    print('started spamming INSERTS')
    while True:
        try:
            # thread_conn.execute(f"SELECT * FROM users WHERE name LIKE '{str(uuid.uuid4())[:7]}'").fetchall()
            thread_conn.execute("INSERT INTO users (name) VALUES (?) ON CONFLICT DO UPDATE SET name=1", (str(uuid.uuid4()),))
            thread_conn.commit()
        except Exception as e:
            print(f'in thread: {e}')

process = Process(target=start_selects, args=[conn1])
process.start()

# Now conn2 comes and spams INSERTS
print("Started spamming INSERTS")
while True:
    try:
        conn2.execute("INSERT INTO users (name) VALUES (?) ON CONFLICT DO UPDATE SET name=1", (str(uuid.uuid4()),))
        # RESERVED lock is obtained
        conn2.commit()
    except Exception as e:
        print("conn2 has locked the db:")
        print(traceback.format_exc())
        break

# The database should be locked for writes by now
# The below attempts to obtain a RESERVED lock and will fail
conn.execute("INSERT INTO users (name) VALUES (?) ON CONFLICT DO UPDATE SET name=1", (str(uuid.uuid4()),))