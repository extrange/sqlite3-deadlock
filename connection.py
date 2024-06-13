import sqlite3

conn_from_another_file = sqlite3.Connection(
    f"file:connection_in_another_file?mode=memory&cache=shared", check_same_thread=False
)
