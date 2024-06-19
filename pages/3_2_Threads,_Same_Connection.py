import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
import sqlite3
import threading
import time
import tempfile
import utils

st.title("2 Threads Using the Same Connection")

st.write(
    """
    In this example, we have 2 threads using the **same connection**, which obtain read-level (`SHARED`) locks (via `BEGIN` and `SELECT`), and then attempt to write simultaneously.

    In this example, we get the error `cannot start a transaction within a transaction`.

    This is because SQLite3 runs in **serialized** mode by default:

    > In serialized mode, API calls to affect or use any SQLite database connection or any object derived from such a database connection can be made safely from multiple threads. **The effect on an individual object is the same as if the API calls had all been made in the same order from a single thread.** The name "serialized" arises from the fact that SQLite uses mutexes to serialize access to each object.

    In effect, what we have done is call `BEGIN` twice in the same transaction, which is not allowed.

    We start by preparing the database:
    """
)

with tempfile.TemporaryDirectory() as d:

    db = utils.init_db(d)

    with st.echo():
        conn = sqlite3.connect(db)

    st.write("Setup shared connection `shared_conn` and threads:")

    with st.echo():

        shared_conn = sqlite3.connect(db, check_same_thread=False)

        def attempt_read_then_write(conn: sqlite3.Connection, thread_num: int):

            try:
                # Any locks obtained will be held until a commit()
                conn.execute("BEGIN TRANSACTION;")

                # Ensure that both threads reach here
                time.sleep(1)

                # Obtain a SHARED lock
                conn.execute("SELECT * FROM users;")

                conn.execute(
                    """INSERT INTO users (name) VALUES (?);""",
                    (f"From Thread {thread_num}",),
                )
                conn.commit()
            except Exception as e:
                st.write(f"Exception on `INSERT` from thread {thread_num}", e)

        thread1 = threading.Thread(
            target=attempt_read_then_write, args=[shared_conn, 1]
        )
        thread2 = threading.Thread(
            target=attempt_read_then_write, args=[shared_conn, 2]
        )

    # Streamlit-specific fix for displaying executed code (unrelated)
    add_script_run_ctx(thread1)
    add_script_run_ctx(thread2)

    st.write("As we expect, on running the code above we encounter an error:")

    with st.echo():

        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        result = shared_conn.cursor().execute("SELECT * FROM users;").fetchall()
        st.write(result)

    st.page_link(
        "pages/7_Misc:_Shared_Cache.py",
        label="Next: Shared Cache",
        icon=":material/arrow_forward:",
    )
