import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
import sqlite3
import threading
import time

st.title("Shared lock with write attempt")

st.write(
    """
    In this example, we have a thread which obtains a `SHARED` lock on the database, but never commits the transaction.
    
    This prevents other threads from being able to write to the database, even with waiting between attempts.
    Prepare the database `memdb5`:
    """
)

with st.echo():
    conn = sqlite3.connect("file:memdb5?mode=memory&cache=shared")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS users;")
    c.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
    conn.commit()

st.write("Setup threads:")

with st.echo():

    conn1 = sqlite3.connect(
        "file:memdb5?mode=memory&cache=shared", check_same_thread=False
    )
    conn2 = sqlite3.connect(
        "file:memdb5?mode=memory&cache=shared", check_same_thread=False
    )

    def get_shared_lock(conn: sqlite3.Connection, thread_num: int):
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION;")
        cursor.execute("SELECT * FROM users;")
        # The connection is never committed
        # As a result, the SHARED lock is never released

    def attempt_write(conn: sqlite3.Connection, thread_num: int):

        cursor = conn.cursor()

        # Ensure that the other thread has obtained a SHARED lock
        time.sleep(1)

        wait = 1

        def attempt_write():
            nonlocal wait
            try:
                cursor.execute(
                    """INSERT INTO users (name) VALUES (?);""",
                    (f"From Thread {thread_num}",),
                )
                conn.commit()
            except Exception as e:
                st.write(
                    f"Exception on `INSERT` from thread {thread_num}, retrying in {wait}s",
                    e,
                )
                time.sleep(wait)
                wait *= 2  # exponential backoff
                attempt_write()
            finally:
                conn.commit()

        attempt_write()

    thread1 = threading.Thread(target=get_shared_lock, args=[conn1, 1])
    thread2 = threading.Thread(target=attempt_write, args=[conn2, 2])

    # Streamlit-specific fix for displaying executed code (unrelated)
    add_script_run_ctx(thread1)
    add_script_run_ctx(thread2)

st.write("Start the threads:")

with st.echo():

    # Start
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
