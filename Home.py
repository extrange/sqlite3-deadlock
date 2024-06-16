import streamlit as st

st.set_page_config(
    page_title="SQLite, Locking and Deadlock",
    menu_items={"about": "[Source code](https://github.com/extrange/sqlite3-deadlock)"},
)

st.title("SQLite, Locking and Deadlock")

st.write(
    """
    [![Repo](https://badgen.net/badge/icon/GitHub?icon=github&label)](https://github.com/extrange/sqlite3-deadlock)
    """
)


st.write(
    """
    [SQLite](https://www.sqlite.org/) is a common database for various applications, and is often used in conjunction with Streamlit.

    However, as SQLite uses file-level locks instead of other concurrency mechanisms such as row level locking or [multi-version concurrency control](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) (PostgreSQL), the following code example can be problematic:

    ```python
    # page1.py
    conn = sqlite3.connect("app.db")
    # Rest of application code

    # page2.py
    conn = sqlite3.connect("app.db")
    # Rest of application code
    ```

    Because of how Streamlit sometimes different threads for page requests, concurrent database access is attempted at times, resulting in `OperationalError: database is locked` errors.

    In the following pages, we will explore why this happens in detail, starting from how SQLite manages locking.

    _Note: Code examples are executed on the backend on demand, for example:_
    """
)

with st.echo():
    import sqlite3
    import time

    sqlite3.sqlite_version
    st.write(f"The time now is {time.ctime()}")

st.page_link(
    "pages/1_SQLite_Locking_in_Detail.py",
    label="Next: SQLite Locking in Detail",
    icon=":material/arrow_forward:",
)
