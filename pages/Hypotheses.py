import streamlit as st

st.title("Hypotheses")

st.write(
    """
    Next, we will explore the issue of the prolonged `database is locked` errors in the Streamlit app.

    The Streamlit app in question contained the following code:

    ```python
    # db_1.py
    # Connection shared by multiple threads in other pages
    conn = sqlite3.connect(...)

    # db_2.py
    # Connection shared by multiple threads in other pages
    conn = sqlite3.connect(...)
    
    # in various Streamlit pages
    conn.execute("some SQL statement")

    # in a particular page
    c = conn.cursor()
    def create_user_if_not_exists(email):
        id = c.execute('''
            INSERT INTO users (email)
                VALUES (?)
                ON CONFLICT(email)
                DO UPDATE SET email=excluded.email
            RETURNING id;
        ''', (email,)).fetchone()[0]
        conn.commit()
        return id
    ```

    The following hypotheses come to mind:

    1. A connection started a write transaction (obtained a `RESERVED` lock), but an exception occured before the call to `commit()` could be made, resulting in the database subsequently not accessible for reads and writes.
    2. 2 connections using a shared `Connection` acquired either a `SHARED` or `EXCLUSIVE` lock, but somehow failed to call `commit()`, thus never releasing the locks.
    """
)

with st.expander("What about deadlock?"):
    st.write(
        """
        Another hypothesis that might come to mind is the situation where 2 threads simultaneously obtain `SHARED` locks through a read operation, then both attempt to obtain a `RESERVED` lock for writing, but fail as each is waiting for the other to give up their `SHARED` lock, resulting in [deadlock](https://en.wikipedia.org/wiki/Deadlock).

        This scenario is quite unlikely, however.

        The execution of the write operation happens in a transaction (either via a `BEGIN` statement or one issued by the Python `sqlite3` driver in `LEGACY_TRANSACTION_MODE`).
        
        SQLite [**does not retry**](https://fractaledmind.github.io/2023/12/11/sqlite-on-rails-improving-concurrency/) or invoke the [busy_handler](https://sqlite.org/c3ref/busy_timeout.html) in the middle of a transaction, as that would break [serializable isolation](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable) guarantees.

        Therefore, the likely outcome of the above scenario would be both threads
        """
    )
