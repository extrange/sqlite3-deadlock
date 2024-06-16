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
    In this example, we have 2 threads which simultaneously read from the database, then attempt to write, one after the other. However, the

    This explores the following scenario in Streamlit where two separate requests occur simultaneously to `page1.py` and `page2.py`, but the conne:

    ```python
    # page1.py
    conn1 = sqlite3.connect(...)
    # Some select with insert logic here

    # page2.py
    conn2 = sqlite3.connect(...)
    # Some select with insert logic here
    ```

    We first prepare the database with connection `conn`:
    """
)

with st.echo():
    conn = sqlite3.connect(f"file:{db_name}?mode=memory&cache=shared")
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
    conn.commit()

st.write(
    "Then, we setup new connections `conn1` and `conn2` which will be used by threads 1 & 2 respectively:"
)

with st.echo():

    conn1 = sqlite3.connect(
        f"file:{db_name}?mode=memory&cache=shared",
        check_same_thread=False,
    )
    conn2 = sqlite3.connect(
        f"file:{db_name}?mode=memory&cache=shared",
        check_same_thread=False,
    )

    def attempt_read_then_write(thread_conn: sqlite3.Connection, thread_num: int):

        thread_conn.execute("SELECT * FROM USERS;")

        # Ensure both threads have executed the SELECT before continuing
        time.sleep(1)

        if thread_num == 2:
            # Schedule thread2 to be just behind thread1
            time.sleep(0.1)

        try:
            # Before this INSERT, a BEGIN DEFERRED TRANSACTION is issued by the Python sqlite3 driver
            thread_conn.execute(
                """INSERT INTO users (name) VALUES (?);""",
                (f"From Thread {thread_num}",),
            )

        except Exception as e:
            st.write(f"Exception on `INSERT` from thread {thread_num}", e)

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
    `thread2` has thrown an exception, while `thread1` has successfully written to the database.

    Now, since `conn1` (which successfully wrote to the database) is still holding an `EXCLUSIVE` lock since `commit()` was not called, `conn2` cannot even read from the database, only `conn1` can.
    """
)

with st.expander(
    "Why is `sqlite3` failing immediately and not retrying with `busy_handler`?"
):
    st.write(
        """

        In order to observe the `busy_handler` in action, we need to take the `INSERT` statement into its own transaction, for example:
        """
    )


with st.echo():

    st.write(
        "Read by `conn1`:",
        conn1.execute("SELECT * FROM users;").fetchall(),
    )

    try:
        # Try read with conn2
        st.write("Read by `conn2`:")
        st.write(conn2.execute("SELECT * FROM users;").fetchall())
    except Exception as e:
        st.write(e)

st.write(
    """
    Neither can `conn`:
    """
)

with st.echo():

    try:
        st.write(conn.execute("SELECT * FROM users;").fetchall())
    except Exception as e:
        st.write(e)

st.write(
    f"""
    _Note that both `thread1` and `thread1` are not running at this point (the locks are held by the connections, irrespective of the thread's status):_

    - `{thread1.is_alive()=}`
    - `{thread2.is_alive()=}`
    """
)

st.write(
    f"""
    In order for the database to be accessible again, `conn1` which is still holding the `EXCLUSIVE` lock needs to call `commit()` to release its locks.
    """
)

with st.echo():
    conn1.commit()


st.write(
    f"""
    Now, the database is `UNLOCKED`, and any connection can read/write:
    """
)

with st.echo():
    st.write(conn.execute("SELECT * FROM users;").fetchall())

st.write(
    f"""
    Note that `thread2` is still in a (`DEFERRED`) transaction:
    
    {f"`{conn2.in_transaction=}`"}
    """
)


st.page_link(
    "pages/3_2_Threads,_Same_Connection.py",
    label="Next: 2 Threads, Same Connection",
    icon=":material/arrow_forward:",
)
