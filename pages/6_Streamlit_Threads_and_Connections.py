import streamlit as st
import sqlite3
import threading
import time
import os

db_name = "streamlit_test"

st.title("Streamlit Threads and Connections")

st.write(
    """
    Here we explore two questions:
    
    1. Does Streamlit run different threads for each request? - No, unless the thread is blocked by e.g. `time.sleep`
    2. Does the same thread use the same `sqlite.Connection` on different reloads? - No
    3. Do different threads use the same sqlite.Connection? - No

    You can verify the above by reloading the page, or sleeping the current thread and then reloading to see the IDs of the thread and connection object change.
    """
)

with st.echo():
    # The use of st.echo() does not affect the results
    conn = sqlite3.Connection(
        # The use of a shared in-memory db does not affect the results
        f"file:{db_name}?mode=memory&cache=shared", check_same_thread=False
    )
    st.write(f"The ID of the current process is {os.getpid()}")
    st.write(f"The ID of the current thread serving this page is {str(threading.get_ident())[-5:]}.")
    st.write(f"The ID of the connection object is {str(id(conn))[-5:]}.")


if st.button('Sleep current thread for 100000s'):
    time.sleep(100000)