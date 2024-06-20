import streamlit as st

st.title("The App")

st.write(
    """
    The Streamlit app in question contained the following code:

    `db_1.py`

    ```python
    # Connection shared by multiple threads in other pages
    conn = sqlite3.connect(...)
    ```

    `db_2.py`

    ```python
    # Connection shared by multiple threads in other pages
    conn = sqlite3.connect(...)
    ```

    In various Streamlit pages:
    
    ```python
    from db_1 import conn
    # or from db_2 import conn
    conn.execute("some SQL statement")
    ```

    A particular page:

    ```python
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
    """
)

st.image(
    "error.jpg",
    caption="The error that occured often, and lasted until the service was restarted",
)

st.write(
    """
    The following hypotheses were proposed:

    1. 2 threads using **different** connections simultaneously read the database, then attempted to write (resulting in [deadlock](https://en.wikipedia.org/wiki/Deadlock)).
    1. 2 connections using the **same** connection acquired either a `SHARED` or `EXCLUSIVE` lock, but somehow failed to call `commit()`, thus never releasing the locks.
    """
)

st.page_link(
    "pages/2_2_Threads,_Different_Connections.py",
    label="Next: 2 Threads, Different Connections",
    icon=":material/arrow_forward:",
)