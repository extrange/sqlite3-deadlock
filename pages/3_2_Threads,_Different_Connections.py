import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
import sqlite3
import threading
import time
import uuid

db_name = str(uuid.uuid4())

st.title("2 Threads Using Different Connections")

st.write(
    """
    In this example, we have 2 threads which both initiate a read transaction, obtaining `SHARED` locks on the database. Then, they both attempt to write, which will fail.

    This is a deadlock scenario.

    Interestingly, while we would expect that SQLite would attempt to retry via the [busy_handler](https://sqlite.org/c3ref/busy_timeout.html) (retrying for 5-30s), it fails almost instantly here. This is because SQLite [**cannot retry**](https://fractaledmind.github.io/2023/12/11/sqlite-on-rails-improving-concurrency/) in the middle of a transaction, as that would break [serializable isolation](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable) guarantees.

    This resolves the deadlock scenario, and subsequent writes on the database are possible.

    Prepare the database:
    """
)

with st.echo():
    conn = sqlite3.connect(f"file:{db_name}?mode=memory&cache=shared")
    c = conn.cursor()
    c.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
    conn.commit()

st.write("Setup new connections `conn1` and `conn2` and threads 1 & 2, which will both obtain `SHARED` locks via `SELECT` and then attempt to write simultaneously:")

with st.echo():

    conn1 = sqlite3.connect(
        f"file:{db_name}?mode=memory&cache=shared", check_same_thread=False
    )
    conn2 = sqlite3.connect(
        f"file:{db_name}?mode=memory&cache=shared", check_same_thread=False
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
            st.write(f"Exception on `INSERT` from thread {thread_num}", e)

        finally:
            # TODO without this, the database is locked on page refresh
            conn.commit()

    thread1 = threading.Thread(target=attempt_read_then_write, args=[conn1, 1])
    thread2 = threading.Thread(target=attempt_read_then_write, args=[conn2, 2])

    # Streamlit-specific fix for displaying executed code (unrelated)
    add_script_run_ctx(thread1)
    add_script_run_ctx(thread2)

st.write("Start the threads. We expect an exception to be thrown for both threads:")

with st.echo():

    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

st.write("If we inspect the database, we see nothing has been written:")

with st.echo():

    result = conn1.cursor().execute("SELECT * FROM users;").fetchall()
    st.write(result)

st.write("Finally, we attempt to write to the DB again, using `conn1`, and succeed:")

with st.echo():

    try:
        conn1.cursor().execute("INSERT INTO users (name) VALUES (?);", ("write at the end",)) 
        conn1.commit()
    except Exception as e:
        st.write(e)
    finally:
        conn1.commit()

st.write(conn1.cursor().execute("SELECT * FROM users;").fetchall())