import sqlite3
import threading
import time

def update_profile(user1, user2):
    with sqlite3.connect("file::memory:?cache=shared") as connection:
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS profiles (user_id INT, name TEXT)")
        cursor.execute("INSERT INTO profiles VALUES (?, ?)", (user1, "User 1"))
        cursor.execute("INSERT INTO profiles VALUES (?, ?)", (user2, "User 2"))
    # Simulate profile update
    with sqlite3.connect("file::memory:?cache=shared") as connection:
        cursor = connection.cursor()
        cursor.execute("BEGIN")
        cursor.execute("SELECT name FROM profiles WHERE user_id = ?", (user1,))
        user1_name = cursor.fetchone()[0]
        time.sleep(1)  # Introduce a delay to induce deadlock
        cursor.execute("SELECT name FROM profiles WHERE user_id = ?", (user2,))
        user2_name = cursor.fetchone()[0]
        new_name = f"{user1_name} and {user2_name}"
        cursor.execute("UPDATE profiles SET name = ? WHERE user_id = ?", (new_name, user1))
        cursor.execute("UPDATE profiles SET name = ? WHERE user_id = ?", (new_name, user2))
        connection.commit()
        print(f"Profiles updated for users {user1} and {user2}")

# Simulate deadlock scenario
def simulate_deadlock():
    thread1 = threading.Thread(target=update_profile, args=(1, 2))
    thread2 = threading.Thread(target=update_profile, args=(2, 1))
    
    thread1.start()
    thread2.start()
    
    thread1.join()
    thread2.join()

simulate_deadlock()