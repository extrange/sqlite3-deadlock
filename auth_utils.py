import functools
import sqlite3

conn = sqlite3.connect('data/db/app.db', check_same_thread=False)

# At this point, sqlite is using a rollback journal for atomicity.
# https://www.sqlite.org/lockingv3.html
#
# A write-ahead-log is also available and can be enabled with:
#
# conn.execute('pragma journal
#  as readers do not block writers and a writer 
# does not block readers. Reading and writing can proceed concurrently.""
#
# All threads use the exact same sqlite3.Connection object
c = conn.cursor() 
        
def create_row(rownum):
    print(f"Before creating row {rownum} using {conn}")
    id = c.execute('''
        INSERT INTO users (email)
            VALUES (?)
            ON CONFLICT(email)
            DO UPDATE SET email=excluded.email
        RETURNING id;
    ''', (rownum,)).fetchone()[0]
    conn.commit()
    print(f"Created user {rownum}")
    return id

def wrap(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
            create_row(1)
            return func(*args, **kwargs)
    return wrapper