import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
import sqlite3
import threading
import time
import uuid

db_name = str(uuid.uuid4())

st.title("2 Threads Using the Same Connection")

st.write(
    """
    In this example, we have 2 threads using the **same connection**, which obtain read-level (`SHARED`) locks, and then attempt to write simultaneously.

    In this example, we get the error `cannot start a transaction within a transaction`.

    This seems to suggest that in multithread use, statements from multiple threads are being interleaved.

    Prepare the database:
    """
)

with st.echo():
    conn = sqlite3.connect(f"file:{db_name}?mode=memory&cache=shared")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS users;")
    c.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
    conn.commit()

st.write("Setup shared connection `shared_conn` and threads:")

with st.echo():

    # check_same_thread=False to allow multithread use
    shared_conn = sqlite3.connect(f"file:{db_name}?mode=memory&cache=shared", check_same_thread=False)

    def attempt_read_then_write(conn: sqlite3.Connection, thread_num: int):

        cursor = conn.cursor()

        try:
            # Any locks obtained will be held until a commit()
            cursor.execute("BEGIN TRANSACTION;")

            # Obtain a SHARED lock
            cursor.execute("SELECT * FROM users;")

            # Ensure that both threads reach here, before we attempt INSERT
            time.sleep(1)

            cursor.execute(
                """INSERT INTO users (name) VALUES (?);""",
                (f"From Thread {thread_num}",),
            )
            conn.commit()
        except Exception as e:
            st.write(f'Exception on `INSERT` from thread {thread_num}', e)

    thread1 = threading.Thread(target=attempt_read_then_write, args=[shared_conn, 1])
    thread2 = threading.Thread(target=attempt_read_then_write, args=[shared_conn, 2])

    # Streamlit-specific fix for displaying executed code (unrelated)
    add_script_run_ctx(thread1)
    add_script_run_ctx(thread2)

st.write("Run:")

with st.echo():

    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    result = shared_conn.cursor().execute("SELECT * FROM users;").fetchall()
    st.write(result)
