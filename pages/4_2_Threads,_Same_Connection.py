import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
import sqlite3
import threading
import time

st.title("2 Threads Using the Same Connection Can Lock")

st.write(
    """
    Prepare the database `memdb4`:
    """
)

with st.echo():
    conn = sqlite3.connect("file:memdb4?mode=memory&cache=shared")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS users;")
    c.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
    conn.commit()

st.write("Setup threads:")

with st.echo():

    shared_conn = sqlite3.connect("file:memdb4?mode=memory&cache=shared")
    def attempt_read_then_write(conn: sqlite3.Connection, thread_num: int):

        cursor = conn.cursor()
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
            conn.commit()

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
