# show_all_sqlite.py
import sqlite3

db_path = "/home/behavioralreef/mysite/behaviour.db"  # replace with your database path
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get all tables in the database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [row[0] for row in cursor.fetchall()]

if not tables:
    print("No tables found in the database.")
else:
    for table in tables:
        print(f"\n--- Table: {table} ---")

        # Get column names
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [info[1] for info in cursor.fetchall()]
        if columns:
            print(" | ".join(columns))
        else:
            print("No columns found")

        # Get all rows
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        if not rows:
            print("No data")
        else:
            for row in rows:
                print(" | ".join(str(v) for v in row))

conn.close()
