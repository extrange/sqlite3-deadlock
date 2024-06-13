import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
import sqlite3
import threading
import time
import uuid

st.title("Shared lock with write attempt")

# We use a random name on each run
db_name = str(uuid.uuid4())

st.write(
    """
    In this example, we have a thread which obtains a `SHARED` lock on the database, but dooesn't commit the transaction until 10s later.
    
    This prevents other threads from being able to write to the database until the first thread's call to `commit()`, which closes the transaction.

    Prepare the database:
    """
)

def main():

    with st.echo():
        conn = sqlite3.connect(f"file:{db_name}?mode=memory&cache=shared")
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS users;")
        c.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
        conn.commit()

    st.write("Setup threads:")

    with st.echo():

        conn1 = sqlite3.connect(
            f"file:{db_name}?mode=memory&cache=shared", check_same_thread=False
        )
        conn2 = sqlite3.connect(
            f"file:{db_name}?mode=memory&cache=shared", check_same_thread=False
        )

        def get_shared_lock(conn: sqlite3.Connection, thread_num: int):
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            cursor.execute("SELECT * FROM users;")
            # The connection not committed until 10s later

            time.sleep(10)
            conn.commit()

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

                    st.write("Write successful!")
                    st.write(cursor.execute("SELECT * FROM users;").fetchall())
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

if __name__=="__main__":
    main()