import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
import sqlite3
import threading
import time
import uuid
from utils import init_db
import tempfile


st.title("2 Threads Using Different Connections")

st.write(
    """
    In this example, we have 2 threads which simultaneously read from the database, then attempt to write.

    We first prepare the database with connection `conn`:
    """
)

with tempfile.TemporaryDirectory() as d:

    db = init_db(d)

    with st.echo():
        conn = sqlite3.connect(db)

    st.write(
        "Then, we setup new connections `conn1` and `conn2` which will be used by threads 1 & 2 respectively:"
    )

    with st.echo():

        conn1 = sqlite3.connect(
            db,
            check_same_thread=False,
        )
        conn2 = sqlite3.connect(
            db,
            check_same_thread=False,
        )

        def attempt_read_then_write(thread_conn: sqlite3.Connection, thread_num: int):

            # Start a deferred transaction
            thread_conn.execute("BEGIN")

            thread_conn.execute("SELECT * FROM USERS;")

            # Ensure both threads have executed the SELECT before continuing
            time.sleep(1)

            if thread_num == 2:
                # Schedule thread2 to be just behind thread1
                # thread2 will be holding the SHARED lock as a result
                time.sleep(0.1)

            try:
                # Before this INSERT, a BEGIN DEFERRED TRANSACTION is issued by the Python sqlite3 driver
                thread_conn.execute(
                    """INSERT INTO users (name) VALUES (?);""",
                    (f"From Thread {thread_num}",),
                )

                # Attempt to obtain a PENDING then EXCLUSIVE lock
                thread_conn.commit()

            except Exception as e:
                st.write(
                    f"Exception on `INSERT` from thread {thread_num} at {time.ctime()}",
                    e,
                )

        thread1 = threading.Thread(target=attempt_read_then_write, args=[conn1, 1])
        thread2 = threading.Thread(target=attempt_read_then_write, args=[conn2, 2])

    # Streamlit-specific fix for displaying executed code (unrelated)
    add_script_run_ctx(thread1)
    add_script_run_ctx(thread2)

    st.write("Start the threads:")

    with st.echo():

        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

    st.write(
        """
        Note that `thread2` has thrown an exception on the `INSERT`, while `thread1` has thrown an exception on the `commit()`.

        This is because `thread1`, being ahead, has obtained a `RESERVED` lock. On the `commit()`, it successfully obtains a `PENDING` lock, but then tries to obtain an `EXCLUSIVE` lock, which fails. `thread2`, being behind, cannot obtain even a `RESERVED` lock, and so fails at the `INSERT`.

        Also, note that `thread2` fails almost immediately, while `thread1` fail around 5s later.
        """
    )

    with st.expander(
        "Why is `thread2` failing immediately? Why is it not retrying with `busy_handler`?"
    ):
        st.write(
            """
            SQLite will not retry (call the `busy_handler`) when it [detects a possible deadlock scenario](https://sqlite.org/c3ref/busy_handler.html). Instead, it returns `SQLITE_BUSY` for the process that is holding a read lock (`thread2`), hoping that it will give up its read lock and let the other process proceed.
            """
        )

    st.write(
        "`conn1` can read the database as it is still holding a `SHARED` lock. In addition, note that it can see its own changes, while `conn2` cannot:"
    )

    with st.echo():
        st.write(conn1.execute("SELECT * FROM users;").fetchall())

    with st.echo():
        st.write(conn2.execute("SELECT * FROM users;").fetchall())

    st.write(
        """
        `conn` cannot read, since `conn1` is holding a `PENDING` lock:
        """
    )

    with st.echo():

        try:
            st.write(conn.execute("SELECT * FROM users;").fetchall())
        except Exception as e:
            st.write(e)

    st.write(
        f"""
        _Note that both `thread1` and `thread2` are not running at this point (the locks are held by the connections, irrespective of the thread's status):_

        - `{thread1.is_alive()=}`
        - `{thread2.is_alive()=}`
        """
    )

    st.write(
        f"""
        In order for the database to be accessible again, `conn2` (holding the `SHARED` lock) must first `commit()` (releasing the lock), followed by `conn1` (allowing it to write, and then releasing the lock).
        """
    )

    with st.echo():
        conn2.commit()
        conn1.commit()

    st.write(
        f"""
        Now, the database is `UNLOCKED`, and any connection can read/write. Also note that the `INSERT` by `conn1` is successful:
        """
    )

    with st.echo():
        st.write(conn.execute("SELECT * FROM users;").fetchall())

    st.page_link(
        "pages/3_2_Threads,_Same_Connection.py",
        label="Next: 2 Threads, Same Connection",
        icon=":material/arrow_forward:",
    )
