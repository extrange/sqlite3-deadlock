import streamlit as st
from auth_utils import requires_login


st.set_page_config(
    page_title="sqlite3 deadlock reproduction",
)


@requires_login
def main():
    st.write("# sqlite3 deadlock reproduction")


if __name__ == "__main__":
    main()
