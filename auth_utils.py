import os
import streamlit as st
import json
import sqlite3
import functools
from datetime import datetime
from uuid import uuid4
from msal_streamlit_authentication import msal_authentication

# TODO: look up need for connection pool. A single connection might not scale if there are concurrent users
# Concurrent updates through connections shared across threads is only safe if SQLite is running in serialised mode
# https://www.sqlite.org/threadsafe.html#:~:text=Multi%2Dthread.,threads%20at%20the%20same%20time.
# https://docs.python.org/3/library/sqlite3.html#sqlite3.threadsafety
conn = sqlite3.connect('data/db/app.db', check_same_thread=False)
c = conn.cursor()

# TODO: Migrate existing users who were onboarded based on adid
# TODO: Table migrations: 1. Rename existing user table, create new user table with (id, email)
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


def get_quiz_result(user_id):
    c.execute(
        'SELECT completed FROM quiz_completions WHERE user_id=?', (user_id,))
    quiz_row = c.fetchone()
    if quiz_row:
        return quiz_row[0] == 1
    return False


def set_quiz_completion(user_id):
    c.execute(
        'SELECT completed FROM quiz_completions WHERE user_id=?', (user_id,))
    quiz_row = c.fetchone()
    if quiz_row:
        c.execute(
            'UPDATE quiz_completions SET completed=? WHERE user_id=?', (1, user_id))
        conn.commit()
        return

    c.execute(
        'INSERT INTO quiz_completions (user_id, completed) VALUES (?, ?)', (user_id, 1))
    conn.commit()


def is_logged_in():
    return 'logged_in' in st.session_state and st.session_state['logged_in']


def requires_login(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if 'logged_in' not in st.session_state:
            st.session_state.logged_in = False
        if 'quiz_completed' not in st.session_state:
            st.session_state.quiz_completed = False
        if 'user_id' in st.session_state:
            st.session_state.quiz_completed = get_quiz_result(
                st.session_state['user_id'])

        if not st.session_state['logged_in']:
            if is_local_env():
                show_dev_login()
            else:
                show_ad_login_form()
        # elif not st.session_state['quiz_completed']:
        #     show_quiz()
        else:
            return func(*args, **kwargs)
    return wrapper

def is_local_env():
    running_env = os.environ.get('RUNNING_ENV')
    return running_env == 'local'

def audit_login(user_id):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute('INSERT INTO audit (user_id, action, timestamp) VALUES (?, ?, ?)',
              (user_id, 'login', timestamp))
    conn.commit()


def get_user_roles(user_id):
    c.execute('SELECT role FROM user_roles WHERE user_id=?', (user_id,))
    return [row[0] for row in c.fetchall()]


def show_dev_login():
    with st.form("login_form"):
        adid = st.text_input("AD Email")
        # password = st.text_input("Password", type='password')
        submitted = st.form_submit_button("Login")
        if submitted:
            user_id = login_user(adid)
            st.write(user_id)


def show_ad_login_form():
    login_token = msal_authentication(
        auth={
            "clientId": os.environ.get('AD_CLIENT_ID'),
            "authority": os.environ.get('AD_ENDPOINT'),
            "redirectUri": os.environ.get('REDIRECT_URL', '/'),
            "postLogoutRedirectUri": "/",
        }, # Corresponds to the 'auth' configuration for an MSAL Instance
        cache={
            "cacheLocation": "sessionStorage",
            "storeAuthStateInCookie": False # TODO: might have to set to true for edge and firefox
        }, # Corresponds to the 'cache' configuration for an MSAL Instance
        # login_request={
        #     "scopes": os.environ.get('AD_SCOPES', 'User.Read').split(',')
        # } # Not needed for login becase by default, MSAL already adds the scopes openid, profile and offline_access to every request
    )
    if login_token is not None:
        login_user(login_token['account']['username'])

def login_user(email):
    user_id = create_user_if_not_exists(email)
    st.session_state['logged_in'] = True
    st.session_state['user_id'] = user_id
    st.session_state['session_uuid'] = str(uuid4())
    audit_login(user_id)
    st.rerun()

