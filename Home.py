import streamlit as st

st.set_page_config(page_title="SQLite, Locking and Deadlock", menu_items={"about": "[Source code](https://github.com/extrange/sqlite3-deadlock)"})
st.title("SQLite, Locking and Deadlock")

st.write(
    """
    _[Source code for this repo](https://github.com/extrange/sqlite3-deadlock)_
    
    [SQLite](https://www.sqlite.org/) is a common database for various applications, and can be used in conjunction with Streamlit.

    The following code example is sometimes seen with Streamlit:

    ```python
    # page1.py
    conn = sqlite3.connect("app.db")
    # Rest of application code

    # page2.py
    conn = sqlite3.connect("app.db")
    # Rest of application code
    ```

    With Streamlit, the potential for deadlock arises, because of how Streamlit spawns separate threads for each page request, effectively resulting in `OperationalError: database is locked` errors.

    In the following pages, we will explore why this happens in detail, starting from how SQLite manages locking.

    _Code examples are executed on the backend on demand, for example:_
    """
)

with st.echo():
    import sqlite3
    import time

    sqlite3.sqlite_version
    st.write(f"The time now is {time.ctime()}")

st.page_link("pages/1_SQLite_Locking_in_Detail.py", label="Next: SQLite Locking in Detail", icon=":material/arrow_forward:")