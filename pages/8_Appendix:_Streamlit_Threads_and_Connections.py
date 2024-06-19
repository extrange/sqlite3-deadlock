import streamlit as st
import sqlite3
import threading
import time
import os
import uuid
import tempfile
from utils import init_db

st.title("Streamlit Threads and Connections")

st.write(
    """
    Here we explore two questions:
    
    1. Does Streamlit run different threads for each request? - No, unless the thread is blocked by e.g. `time.sleep`
    2. Does the same thread use the same `sqlite.Connection` on different reloads? - No
    3. Do different threads use the same sqlite.Connection? - No
    4. Does a connection imported from another file remain the same across the same thread/different threads? - Yes

    You can verify the above by duplicating this tab, or sleeping the current thread and then duplicating this tab to see the IDs of the thread and connection object change.
    """
)

with tempfile.TemporaryDirectory() as d:

    db = init_db(d)

    with st.echo():
        # The use of st.echo() does not affect the results
        conn = sqlite3.Connection(
            # The use of a shared in-memory db does not affect the results
            f"file:{db}?mode=memory&cache=shared",
            check_same_thread=False,
        )
        st.write(f"The ID of the current process is {os.getpid()}")
        st.write(
            f"The ID of the current thread serving this page is {str(threading.get_ident())[-5:]}."
        )
        st.write(f"The ID of the connection object is {str(id(conn))[-5:]}.")

        from utils import conn_from_another_file

        st.write(
            f"The ID of `conn_from_another_file` is {str(id(conn_from_another_file))[-5:]}"
        )

    st.write(
        """
        Additional questions:

        1. If you reuse a connection after `commit()`-ing, does its `id()` change? - No
        """
    )

    with st.echo():

        random_db_name = str(uuid.uuid4())
        conn = sqlite3.Connection(
            # The use of a shared in-memory db does not affect the results
            f"file:{random_db_name}?mode=memory&cache=shared",
            check_same_thread=False,
        )

        st.write(f"ID of `conn` before `commit()`: {id(conn)}")

        c = conn.cursor()
        c.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
        c.execute(
            """INSERT INTO users (name) VALUES (?);""",
            (f"test",),
        )
        conn.commit()
        st.write(f"ID of `conn` after `commit()`: {id(conn)}")


    if st.button("Sleep current thread for 100s"):
        time.sleep(100)

st.page_link("pages/9_Appendix:_SQLite_Locking_in_Detail.py", label="Next: SQLite Locking in Detail", icon=":material/arrow_forward:")