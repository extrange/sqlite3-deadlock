import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
import sqlite3
import threading
import time

st.title("2 Threads Using Different Connections Can Lock")

st.write(
    """
    Prepare the database `memdb3`:
    """
)

with st.echo():
    conn = sqlite3.connect("file:memdb3?mode=memory&cache=shared")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS users;")
    c.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")
    conn.commit()

st.write("Setup threads:")

with st.echo():

    conn1 = sqlite3.connect(
        "file:memdb3?mode=memory&cache=shared", check_same_thread=False
    )
    conn2 = sqlite3.connect(
        "file:memdb3?mode=memory&cache=shared", check_same_thread=False
    )

    def attempt_read_then_write(conn: sqlite3.Connection, thread_num: int):

        cursor = conn.cursor()

        # Using BEGIN ensures that we retain a SHARED lock until a commit()
        # Otherwise, the SHARED lock would be released after the SELECT
        cursor.execute("BEGIN TRANSACTION;")

        cursor.execute("SELECT * FROM users;")

        # Ensure that both threads have obtained SHARED locks before the insert
        time.sleep(1)

        try:
            cursor.execute(
                """INSERT INTO users (name) VALUES (?);""",
                (f"From Thread {thread_num}",),
            )
            conn.commit()
        except Exception as e:
            st.write(f'Exception on `INSERT` from thread {thread_num}', e)

        finally:
            # TODO without this, the database is locked on page refresh
            conn.commit()

    thread1 = threading.Thread(target=attempt_read_then_write, args=[conn1, 1])
    thread2 = threading.Thread(target=attempt_read_then_write, args=[conn2, 2])

    # Streamlit-specific fix for displaying executed code (unrelated)
    add_script_run_ctx(thread1)
    add_script_run_ctx(thread2)

st.write("Run:")

with st.echo():

    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    result = conn1.cursor().execute("SELECT * FROM users;").fetchall()
    st.write(result)
