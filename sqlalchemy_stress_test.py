"""
Stress testing demo for sqlite3 usiing SQLAlchemy.

Creates a DB and then nuns a main loop which will continually INSERT.

Will also run additional threads performing INSERTs.

num_threads: number of additional threads to run (default 1)
pool_size: SQLAlchemy connection pool size

Findings:
With num_threads > pool_size (e.g. 11 and 2), after a few minutes, the QueuePool limit is exceeded and the connection times out.

With num_threads < pool_size, (e.g. 5 and 10), after a few minutes, the main thread encounters a 'database is locked' exception.

The above are inherent limitations with using SQLite as a database for high concurrency, and while connection pooling and other opimizations can be done, ultimately a db with better concurrency support is a better choice.
"""
import uuid
import threading
import tempfile
import streamlit as st
from sqlalchemy.engine import Engine
from sqlalchemy import event, text
from streamlit.connections import SQLConnection

# Number of additional threads simultaneously connecting to the db
num_threads = 5
pool_size = 10

@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Run all sqlite3 connections in WAL mode."""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()

def main():
    with tempfile.NamedTemporaryFile() as f:

        db = f.name

        # Thread control
        should_stop = False

        # Returns a wrapper over an SQLAlchemy Engine.
        conn = st.connection("sqlite", type=SQLConnection, url=f"sqlite:///{db}", pool_size=2)

        with conn.session as s:
            s.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT UNIQUE)"))
            for _ in range(1_000_000):
                s.execute(text("INSERT INTO users (name) VALUES (:name)"), {"name": str(uuid.uuid4())} )
            print("Database created.")

        def start_inserts():
            print("Thread: Started spamming INSERTS")
            while True:
                if should_stop:
                    break
                try:
                    with conn.session as s:
                        s.execute(text("INSERT INTO users (name) VALUES (:name)"), {"name": str(uuid.uuid4())} )
                        s.commit()
                except Exception as e:
                    print(f"Thread: {e}")

        for _ in range(num_threads):
            thread = threading.Thread(target=start_inserts)
            thread.start()

        # Now conn2 comes and spams INSERTS
        print("Main: Started spamming INSERTS")
        while True:
            try:
                with conn.session as s:
                    s.execute(text("INSERT INTO users (name) VALUES (:name)"), {"name": str(uuid.uuid4())} )
                    s.commit()
            except Exception as e:
                print("Main: ", e)
                break

        should_stop = True
        thread.join()

        # Note that the database is not locked
        # QueuePool does a reset on return, which releases any locks held.
        with conn.session as s:
            s.execute(text("INSERT INTO users (name) VALUES (:name)"), {"name": str(uuid.uuid4())} )
            s.commit()
        print("Successfully inserted into db")


if __name__ == "__main__":
    main()
