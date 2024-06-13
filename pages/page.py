import streamlit as st
from auth_utils import wrap, create_row
import threading
import time
import sqlite3
import os

# SQLite does not retry lock acquisition in the middle of a transaction
# E.g. in a deferred transaction - reading first, but not writing yet
# https://blog.pecar.me/django-sqlite-dblock

# Streamlit app layout
# @wrap
def main():
    st.title("Another page")
    st.write(f'Thread ID: {str(threading.get_ident())[-5:]}')
    st.write(f'Process ID: {os.getpid()}')

    if st.button("Perform upsert into DB"):
        create_row(1)

if __name__ == "__main__":
    main()