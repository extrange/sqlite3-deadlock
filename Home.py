import streamlit as st
from auth_utils import wrap, create_row
from threading import Thread
from functools import partial
import uuid
import time
import threading
import sqlite3
import os
from streamlit.runtime.scriptrunner import add_script_run_ctx

st.set_page_config(
    page_title="SQLite3 Locking and Deadlocks in Streamlit Apps",
)


def thread_id():
    return str(threading.get_ident())[-5:]


# @wrap
def main():
    st.write(
        """
        # SQLite3 Locking and Deadlocks in Streamlit Apps

        _Certain code examples here are actually executed with the results below the code block._
        
        In Streamlit apps using `sqlite3` as the database, it is common to see the following design pattern where several Pages make their own connections to the database:

        ```python
        # page1.py
        conn = sqlite3.connect('data/db/app.db', check_same_thread=False)
        cursor = conn.cursor()
        # Code using the cursor
        # ...

        # page2.py
        conn = sqlite3.connect('data/db/app.db', check_same_thread=False)
        cursor = conn.cursor()
        # Code using the cursor
        # ...
        ```

        _Note: [`check_same_thread=False`](https://docs.python.org/3/library/sqlite3.html#module-functions) allows the database connection object (`conn` above)to be used by a thread **other** than the one that created it._

        Due to the nature of how Streamlit handles concurrent requests (in separate threads), it is possible for [deadlocks](https://en.wikipedia.org/wiki/Deadlock) to happen, which, in the case of `sqlite3` with file-level locking, will block further attempts to write to the database.

        In order to demonstrate this, we will show, in order, that:

        1. 2 threads using different connections cause deadlock (independently of Streamlit)
        2. Streamlit runs separate threads for requests
        3. End-to-end deadlock example

        ### Base case: Deadlock with 2 threads using different connections

        The following creates 2 threads which use different connections to access the database.

        First, we create a [shared in-memory database](https://www.sqlite.org/inmemorydb.html) named `memdb1` which will be used subsequently on this page:
        """
    )

    with st.echo():
        conn = sqlite3.connect("file:memdb1?mode=memory&cache=shared", check_same_thread=False)
        c = conn.cursor()
        c.execute(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);"
        )
        conn.commit()

    st.write(
        """
        Then, we start 2 threads which attempt to write to the database 5 times using different connections simultaneously. This will cause an exception:

        This can be fixed with:

        conn1 = sqlite3.connect("file::memory:?cache=shared", check_same_thread=False, isolation_level=None)
        conn.execute('pragma journal_mode=wal')
        """
    )

    with st.echo():
        conn1 = sqlite3.connect("file::memory:?cache=shared", check_same_thread=False)
        conn2 = sqlite3.connect("file::memory:?cache=shared", check_same_thread=False)

        def insert(conn: sqlite3.Connection, thread_num: int):

            cursor = conn.cursor()

            # Busy handler not called if SQLite determines deadlock?
            # https://stackoverflow.com/questions/15143871/simplest-way-to-retry-sqlite-query-if-db-is-locked#comment47938871_15144547
            # I'm not really getting a deadlock - sqlite3 is giving up very early on. Maybe the nested transaction trick might work?
            
            last_row_added = 1

            def try_insert():
                nonlocal last_row_added
                for i in range(5): # Deadlock doesn't happen for small numbers

                    # This acquires a shared read-lock
                    cursor.execute(
                        """INSERT INTO users (name) VALUES (?);""", (f"User {i} by thread {thread_num}",)
                    )

                    # Acquisition of exclusive write lock attempted here
                    conn.commit()

                    last_row_added += 1
            try:
                try_insert()
            except Exception as e:
                st.write(f"Exception from thread {thread_num} while inserting row {last_row_added}:", e)
                try_insert()

        thread1 = threading.Thread(target=insert, args=[conn1, 1])
        thread2 = threading.Thread(target=insert, args=[conn2, 2])

        # Streamlit-specific fix for displaying executed code (unrelated)
        add_script_run_ctx(thread1)
        add_script_run_ctx(thread2)

        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        result = conn1.cursor().execute("SELECT * FROM users;").fetchall()
        st.write(result)
    
    st.write("_Note: results may vary on refreshes of the page._")

    st.write(f"Thread ID: {thread_id()}")
    st.write(f"Process ID: {os.getpid()}")

    if st.button("`time.sleep(1_000_000)`"):
        st.write(f"Thread {thread_id()} sleeping...")
        time.sleep(1_000_000)

    if st.button("Create 100 users in 100 threads"):
        threads = []
        for _ in range(3):
            t = Thread(target=create_row, args=[str(uuid.uuid4())])
            threads.append(t)
            t.start()
        for t in threads:
            t.join()


if __name__ == "__main__":
    main()
