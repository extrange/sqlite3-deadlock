import streamlit as st
import sqlite3
import uuid

db_name = str(uuid.uuid4())

st.set_page_config(
    page_title="SQLite Locking in Detail",
)


st.title("SQLite Locking in Detail")

st.write(
    """
    Locking is a mechanism to ensure consistency of operations, when multiple threads are involved, either reading, writing or both. It enables [**isolation**](https://en.wikipedia.org/wiki/Isolation_(database_systems)?oldformat=true#Isolation_levels), which is one of the ACID (Atomicity, Consistency, Isolation, Durability) properties.

    SQLite has several locks, which will be explored as we go through a transaction.
    
    | Lock State | Description |
    |------------|-------------|
    | **UNLOCKED** | No locks are held on the database. The database may be neither read nor written. Any internally cached data is considered suspect and subject to verification against the database file before being used. Other processes can read or write the database as their own locking states permit. This is the default state. |
    | **SHARED** | The database may be read but not written. Any number of processes can hold SHARED locks at the same time, hence there can be many simultaneous readers. But no other thread or process is allowed to write to the database file while one or more SHARED locks are active. |
    | **RESERVED** | A RESERVED lock means that the process is planning on writing to the database file at some point in the future but that it is currently just reading from the file. Only a single RESERVED lock may be active at one time, though multiple SHARED locks can coexist with a single RESERVED lock. RESERVED differs from PENDING in that new SHARED locks can be acquired while there is a RESERVED lock. |
    | **PENDING** | A PENDING lock means that the process holding the lock wants to write to the database as soon as possible and is just waiting on all current SHARED locks to clear so that it can get an EXCLUSIVE lock. No new SHARED locks are permitted against the database if a PENDING lock is active, though existing SHARED locks are allowed to continue. |
    | **EXCLUSIVE** | An EXCLUSIVE lock is needed in order to write to the database file. Only one EXCLUSIVE lock is allowed on the file and no other locks of any kind are allowed to coexist with an EXCLUSIVE lock. In order to maximize concurrency, SQLite works to minimize the amount of time that EXCLUSIVE locks are held. |

    _Based on the [SQLite docs](https://sqlite.org/lockingv3.html)._

    First, we prepare an in-memory database using a connection `conn`:
    """
)

with st.echo():
    conn = sqlite3.connect(f"file:{db_name}?mode=memory&cache=shared")
    c = conn.cursor()
    c.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
    c.execute("INSERT INTO users (name) VALUES (?);", ("User 1",))
    conn.commit()

st.write(conn.cursor().execute("SELECT * FROM users;").fetchall())

st.write(
    """
    The database is currently in the `UNLOCKED` state.

    We make a new connection `conn1` to the database. Then, we manually start a transaction with `BEGIN`. This ensures that read transaction does not end immediately after the `SELECT` statement (it only ends on the `commit()` call).
    
    We then perform a `SELECT`. This acquires a `SHARED` lock.
    """
)

with st.echo():
    # Connect to the DB
    conn1 = sqlite3.connect(f"file:{db_name}?mode=memory&cache=shared")
    cursor1 = conn1.cursor()
    cursor1.execute("BEGIN TRANSACTION;")
    cursor1.execute("SELECT * FROM users;")
    # SHARED lock acquired, and not released due to BEGIN TRANSACTION

st.write(conn.cursor().execute("SELECT * FROM users;").fetchall())

st.write(
    """
    Next, we perform an `INSERT`. This  acquires a `RESERVED` lock, followed by a `PENDING` lock.
    """
)

with st.echo():
    cursor1.execute("INSERT INTO users (name) VALUES (?);", ("User 2",))
    # PENDING lock held

st.write(
    """
    With a `PENDING` lock held, new connections cannot acquire `SHARED` locks and hence cannot read from the database.

    The current connection `conn1` holding the `PENDING` lock, however, is free to read from the database:
    """
)

with st.echo():
    st.write(conn1.cursor().execute("SELECT * FROM users;").fetchall())

st.write(
    """
    Other connections (e.g. `conn`) cannot obtain a `SHARED` lock, and hence cannot read:
    """
)

with st.echo():
    try:
        st.write(conn.cursor().execute("SELECT * FROM users;").fetchall())
    except Exception as e:
        st.write(e)

st.write(
    """
    However, after `conn1` commits, all locks are released, and `conn` is again free to read:
    """
)

with st.echo():
    conn1.commit()
    # An EXCLUSIVE lock was acquired
    # Journal is written to database
    # All locks are released, and the database is now in the UNLOCKED state

    result = conn.cursor().execute("SELECT * FROM users;").fetchall()
    st.write(result)

st.page_link("pages/2_2_Threads,_Different_Connections.py", label="Next: 2 Threads, Different Connections", icon=":material/arrow_forward:")
