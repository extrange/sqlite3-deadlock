import streamlit as st
import sqlite3
from utils import init_db
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory() as d:

    st.set_page_config(
        page_title="SQLite Locking in Detail",
    )

    st.title("SQLite Locking in Detail")

    st.write(
        """
        _Based on the official SQLite [documentation](https://www.sqlite.org/atomiccommit.html)._

        Locking is a mechanism to ensure consistency of operations, when multiple threads are involved, either reading, writing or both. It enables [**isolation**](https://en.wikipedia.org/wiki/Isolation_(database_systems)?oldformat=true#Isolation_levels), which is one of the ACID (Atomicity, Consistency, Isolation, Durability) properties.

        SQLite has several locks, which will be explored as we go through a transaction. We also discuss the various types of transactions.
        """
    )

    with st.expander("Types of Locks"):

        st.write(
            """
            | Lock State | Description |
            |------------|-------------|
            | **UNLOCKED** | No locks are held on the database. The database may be neither read nor written. Any internally cached data is considered suspect and subject to verification against the database file before being used. Other processes can read or write the database as their own locking states permit. This is the default state. |
            | **SHARED** | The database may be read but not written. Any number of processes can hold SHARED locks at the same time, hence there can be many simultaneous readers. But no other thread or process is allowed to write to the database file while one or more SHARED locks are active. |
            | **RESERVED** | A RESERVED lock means that the process is planning on writing to the database file at some point in the future but that it is currently just reading from the file. Only a single RESERVED lock may be active at one time, though multiple SHARED locks can coexist with a single RESERVED lock. RESERVED differs from PENDING in that new SHARED locks can be acquired while there is a RESERVED lock. |
            | **PENDING** | A PENDING lock means that the process holding the lock wants to write to the database as soon as possible and is just waiting on all current SHARED locks to clear so that it can get an EXCLUSIVE lock. No new SHARED locks are permitted against the database if a PENDING lock is active, though existing SHARED locks are allowed to continue. |
            | **EXCLUSIVE** | An EXCLUSIVE lock is needed in order to write to the database file. Only one EXCLUSIVE lock is allowed on the file and no other locks of any kind are allowed to coexist with an EXCLUSIVE lock. In order to maximize concurrency, SQLite works to minimize the amount of time that EXCLUSIVE locks are held. |

            _Based on the [SQLite docs](https://sqlite.org/lockingv3.html)._
            """
        )

    st.write("First, we prepare a file-backed database using a connection `conn`:")

    with st.echo():
        db = init_db(d)
        conn = sqlite3.connect(db)
        st.write(conn.execute("SELECT * FROM users;").fetchall())

    st.write(
        """
        The database is now in the `UNLOCKED` state.

        We make a new connection `conn1` to the database and perform a `SELECT`. This acquires a `SHARED` lock, but only for the duration of the read.
        """
    )

    with st.echo():
        # Connect to the DB
        conn1 = sqlite3.connect(db)

        # SHARED lock is acquired, then released
        conn1.execute("SELECT * FROM users;")

    st.write(
        f"""
        _Note that we are not in a transaction currently:_
        
        `{conn1.in_transaction=}`
        """
    )

    with st.expander("Why are we not in a transaction?"):
        st.write(
            """
            You might think that a transaction would be started automatically upon making a connection, or at least after a statement is executed, and persist until `commit()` is called.

            This is not the case, as SQLite has _implicitly_ opened and closed (committed) a transaction for us, as there was no explicit transaction started with a `BEGIN` statement.

            SQLite runs transactions (implicit or explicit) with the [`DEFERRED`](https://www.sqlite.org/lang_transaction.html#deferred_immediate_and_exclusive_transactions) behavior (the [default](https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.isolation_level)).
            
            Instead, if we start a transaction explicitly with `BEGIN DEFERRED TRANSACTION`:

            - the transaction does not actually start until the database is first accessed ([i.e. no locks are acquired until the first `SELECT` or `INSERT`/`UPDATE`](https://system.data.sqlite.org/index.html/raw/419906d4a0043afe2290c2be186556295c25c724))
            - the automatic commit (`autocommit`) that would normally occur when the last statement finishes is turned off (i.e., the transaction is not `COMMIT`-ed automatically)

            For example:
            """
        )
        test_db = init_db(d)
        test_conn = sqlite3.connect(test_db)

        st.write(f"Before: `{test_conn.in_transaction=}`")

        with st.echo():
            test_conn.execute("BEGIN DEFERRED TRANSACTION;")
            test_conn.execute("SELECT * FROM users;")

        st.write(f"After: `{test_conn.in_transaction=}`")

        test_conn.commit()

    with st.expander(
        "Python's `sqlite3` driver defaults and quirks with `INSERT`/`UPDATE`"
    ):
        st.write(
            f"""
            In Python's `sqlite3` driver the default settings are to use `DEFERRED` behavior for transactions:

            - `{conn.isolation_level=}` _blank string is the default for `DEFERRED`_
            - `{conn.autocommit=}` _if set to `-1` (`LEGACY_TRANSACTION_MODE`), the `isolation_level` setting will be respected_
            
            Data modification statements such as `INSERT`, however, are not automatically committed (unlike `SELECT`), due to the Python `sqlite3` driver [issuing](https://docs.python.org/3.7/library/sqlite3.html#controlling-transactions) `BEGIN` commands before `INSERT/UPDATE/DELETE/REPLACE` statements.
            
            As a result, they open **explicit** transactions (from the point of view of the underlying `sqlite3` library), which need to be `COMMIT`-ed manually (e.g. with a `commit()` call in Python).

            See [this Stackoverflow answer](https://stackoverflow.com/a/48391535) for more details.

            _Note: We can force the Python `sqlite3` driver to use the underlying `sqlite3` library's `autocommit` mode with [`autocommit=True`](https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.autocommit):_
            """
        )

        with st.echo():
            test_conn2 = sqlite3.connect(test_db, autocommit=True)

        st.write(f"Before `INSERT`: `{test_conn2.in_transaction=}`")

        with st.echo():
            test_conn2.execute(
                "INSERT INTO users (name) VALUES (?);", ("test user by test_conn2",)
            )
        st.write(
            f"""
            After `INSERT`: `{test_conn2.in_transaction=}`

            _Note that the transaction has been committed automatically, without an explicit `commit()` call._
            """
        )

        st.write(test_conn2.execute("SELECT * FROM users;").fetchall())

    with st.expander("`IMMEDIATE` transaction behavior"):

        st.write(
            """
            In the `IMMEDIATE` transaction mode, a `RESERVED` lock is obtained once the `BEGIN IMMEDIATE TRANSACTION` statement is executed.
            
            This will prevent other connections from writing (but not reading).
            """
        )
        test_db2 = init_db(d)

        test_conn = sqlite3.connect(test_db2)
        test_conn3 = sqlite3.connect(test_db2)

        with st.echo():
            test_conn.execute("BEGIN IMMEDIATE TRANSACTION;")

            # We attempt to INSERT from another connection, which will fail
            try:
                test_conn3.execute(
                    "INSERT INTO users (name) VALUES (?);", ("test user by test_conn3",)
                )
            except Exception as e:
                st.write(e)

        test_conn.commit()

        st.write(
            "Contrast this behavior with the `DEFERRED` mode, which does not obtain locks or start a transaction until actual database access:"
        )

        with st.echo():
            test_conn.execute("BEGIN DEFERRED TRANSACTION;")
            # Since no locks were actually acquired yet, we are free to write
            test_conn3.execute(
                "INSERT INTO users (name) VALUES (?);",
                ("test user by test_conn3 (in deferred mode)",),
            )
            test_conn3.commit()

        test_conn.commit()

        st.write(test_conn.execute("SELECT * FROM users;").fetchall())

    st.write(
        """
        Next, we perform an `INSERT`. This  acquires a `RESERVED` lock.
        """
    )

    with st.echo():
        conn1.execute("INSERT INTO users (name) VALUES (?);", ("User 2",))

    st.write(f"`{conn1.in_transaction=}`")

    st.write(
        """
        Finally, we `commit()`. A `PENDING` lock is obtained, followed by an `EXCLUSIVE` lock, and changes are written to the database. During this brief period, no other connection can read the database or obtain a `SHARED` lock.
        """
    )

    with st.echo():
        conn1.commit()
        # An EXCLUSIVE lock was acquired
        # Journal is written to database
        # All locks are released, and the database is now in the UNLOCKED state

        st.write(conn.cursor().execute("SELECT * FROM users;").fetchall())
