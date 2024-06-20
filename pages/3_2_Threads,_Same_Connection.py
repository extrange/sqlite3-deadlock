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
    In this example, we have 2 threads using the **same connection**, which obtain a `RESERVED` lock for writing, but due to an exception, never `commit()`, thus leaving the database locked for writes.

    For the purposes of demonstration, we don't actually have to use 2 threads, but can instead just use one thread with a single connection, as SQLite runs in serialized mode.
    """
)

with st.expander("Serialized Mode"):
    st.write(
        """
        From the [docs](https://www.sqlite.org/threadsafe.html):

        > In serialized mode, API calls to affect or use any SQLite database connection or any object derived from such a database connection can be made safely from multiple threads. **The effect on an individual object is the same as if the API calls had all been made in the same order from a single thread.** The name "serialized" arises from the fact that SQLite uses mutexes to serialize access to each object.
        """
    )

st.write("We start by preparing the database:")

with tempfile.TemporaryDirectory() as d:

    db = utils.init_db(d)

    with st.echo():
        conn = sqlite3.connect(db)

    st.write("Setup shared connection `shared_conn` and threads:")

    with st.echo():

        shared_conn = sqlite3.connect(db)

        # We first perform an INSERT to obtain a RESERVED lock
        shared_conn.execute(
            "INSERT INTO users (name) VALUES (?);",
            ("test",),
        )

    st.write("Then, simulate an exception being thrown which prevents the `commit()`:")

    with st.echo():
        try:
            raise Exception("test exception")
            shared_conn.commit()
        except Exception as e:
            st.write(e)

    st.write(
        "At this point, `shared_conn` is holding a `RESERVED` lock, which will prevent any other connection from writing:"
    )

    with st.echo():

        try:
            conn.execute(
                "INSERT INTO users (name) VALUES (?);",
                (f"test",),
            )
        except Exception as e:
            st.write(e)

    st.write(
        """
        This lock will persist until `commit()` is called, which may never happen if subsequent threads attempt to write before committing.

        For the app in question, an upsert was done on each user request initially, which would prevent `commit()` from ever being called.
        """
    )

    st.page_link(
        "pages/7_Misc:_Shared_Cache.py",
        label="Next: Shared Cache",
        icon=":material/arrow_forward:",
    )
