import streamlit as st
from auth_utils import requires_login

# Streamlit app layout
@requires_login
def main():
    st.title("Medical Text Summarization Tool: MOCKED")
    
    st.text("Wrapped with @requires_login")

if __name__ == "__main__":
    main()