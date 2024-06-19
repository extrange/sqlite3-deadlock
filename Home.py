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

    However, as SQLite uses file-level locks instead of other concurrency mechanisms such as row level locking or [multi-version concurrency control](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) (PostgreSQL), concurrent access can encounter locks, resulting in `OperationalError: database is locked` errors.

    This repository aims to explore why a particular Streamlit app using SQLite randomly encountered very prolonged `database is locked` errors, effectively preventing user access.

    _Note: Code examples are executed on the backend on demand, for example:_
    """
)

with st.echo():
    import sqlite3
    import time

    st.write(f"`{sqlite3.sqlite_version}`")
    st.write(f"`The time now is {time.ctime()}`")

st.page_link(
    "pages/1_The_App.py",
    label="Next: SQLite Locking in Detail",
    icon=":material/arrow_forward:",
)
