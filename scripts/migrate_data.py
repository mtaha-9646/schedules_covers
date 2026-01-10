import os
import sqlite3
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import uuid

# Configuration
DB_URL = os.getenv("DATABASE_URL", "postgresql://admin:password@localhost:5432/saas_suite")
BEHAVIOR_DB = "apps/behavior/behaviour.db"
SCHEDULES_EXCEL = "apps/schedules-covers/schedules.xlsx"
COVERS_JSON = "apps/schedules-covers/covers.json"
DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000000"

def get_pg_conn():
    return psycopg2.connect(DB_URL)

def migrate_behavior():
    print("Migrating Behavior data...")
    if not os.path.exists(BEHAVIOR_DB):
        print(f"Skipping Behavior migration: {BEHAVIOR_DB} not found.")
        return

    sqlite_conn = sqlite3.connect(BEHAVIOR_DB)
    pg_conn = get_pg_conn()
    cur = pg_conn.cursor()

    # Migrate Teachers
    teachers_df = pd.read_sql_query("SELECT * FROM teachers", sqlite_conn)
    teachers_df['tenant_id'] = DEFAULT_TENANT_ID
    
    # Simple migration logic for demo/MVP
    for _, row in teachers_df.iterrows():
        cur.execute(
            "INSERT INTO behavior.teachers (id, name, email, password, subject, grade, tenant_id) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING",
            (row['id'], row['name'], row['email'], row['password'], row.get('subject'), row.get('grade'), DEFAULT_TENANT_ID)
        )

    # Migrate Students
    students_df = pd.read_sql_query("SELECT * FROM students", sqlite_conn)
    for _, row in students_df.iterrows():
        cur.execute(
            "INSERT INTO behavior.students (id, esis, name, homeroom, tenant_id) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (esis) DO NOTHING",
            (row['id'], row['esis'], row['name'], row['homeroom'], DEFAULT_TENANT_ID)
        )

    pg_conn.commit()
    sqlite_conn.close()
    pg_conn.close()
    print("Behavior migration complete.")

def seed_control_plane():
    print("Seeding Control Plane...")
    pg_conn = get_pg_conn()
    cur = pg_conn.cursor()

    # Create Default Tenant
    cur.execute(
        "INSERT INTO control_plane.tenants (id, name, status) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
        (DEFAULT_TENANT_ID, "Default Academy", "active")
    )

    # Register Apps
    apps = [
        ("app-schedules", "Schedules & Covers", "http://localhost:5000"),
        ("app-behavior", "Behavior Management", "http://localhost:4000")
    ]
    for key, name, url in apps:
        cur.execute(
            "INSERT INTO control_plane.apps (key, name, base_url) VALUES (%s, %s, %s) ON CONFLICT (key) DO NOTHING",
            (key, name, url)
        )

    # Enable apps for default tenant
    cur.execute(
        "INSERT INTO control_plane.tenant_apps (tenant_id, app_id, enabled) "
        "SELECT %s, id, TRUE FROM control_plane.apps ON CONFLICT DO NOTHING",
        (DEFAULT_TENANT_ID,)
    )

    pg_conn.commit()
    pg_conn.close()
    print("Control Plane seeded.")

if __name__ == "__main__":
    seed_control_plane()
    # migrate_behavior() # Uncomment when running in container
    print("Done.")
