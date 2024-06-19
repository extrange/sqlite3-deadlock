import sqlite3
import streamlit as st
import tempfile
from utils import init_db
from uuid import uuid4

st.title("Misc: Shared Cache")

st.write(
    """
    SQLite has a [shared cache](https://www.sqlite.org/sharedcache.html) mode, which allows multiple connections to share the same cache (albeit currently discouraged).

    The net effect is that there are an additional set of table-level read and write locks.
    """
)

with st.echo():
    db = f"file:{uuid4()}?mode=memory&cache=shared"
    conn = sqlite3.connect(db)
    conn1 = sqlite3.connect(db)
    conn2 = sqlite3.connect(db)

    # Initialize
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
    conn.commit()

    # conn2.execute("PRAGMA read_uncommitted=true").fetchall()

    conn1.execute("BEGIN")
    conn2.execute("BEGIN")
    conn1.execute("SELECT * FROM users")
    conn2.execute("SELECT * FROM users")

st.write(
    """
    `conn1` and `conn2` have read table-level locks now. As a result, neither can write:
    """
)

with st.echo():

    # Will fail
    try:
        conn1.execute("INSERT INTO users (name) VALUES (?)", ("conn1",))
        conn1.commit()
    except Exception as e:
        st.write(e)

st.write(
    """
    _Note the exception specifies that the database table named `users` is locked. This is **different** from the exception that would be thrown if the whole file were locked._

    _Also note that the exception is thrown almost immediately (i.e. `busy_handler` is [not invoked](https://github.com/mattn/go-sqlite3/issues/632#issue-360437494) in the case of table-level locks)._

    _Finally, note that the exception happens at the `INSERT` statement, not at the `commit()`. Acquiring table-level locks happens at statement execution._

    TODO: Why can't `conn1` read, if neither connection has a `PENDING` lock?
    """
)

with st.echo():

    try:
        conn.execute("SELECT * FROM users").fetchall()
    except Exception as e:
        st.write(e)

st.write("`conn2` and `conn1` still have their `SHARED` locks, so they can read:")

with st.echo():

    st.write(conn1.execute("SELECT * FROM users").fetchall())
    st.write(conn2.execute("SELECT * FROM users").fetchall())

st.page_link("pages/8_Appendix:_Streamlit_Threads_and_Connections.py", label="Next: Streamlit Threads and Connections", icon=":material/arrow_forward:")