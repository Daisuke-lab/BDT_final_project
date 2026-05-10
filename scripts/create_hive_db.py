#!/usr/bin/env python3
"""
create_hive_db.py
=================
Creates the 'dbt' database in Hive if it does not already exist.

Usage:
  python3 test-scripts/create_hive_db.py
"""

from pyhive import hive

HOST     = "hive.lifespacedigital.com"
PORT     = 10000
USERNAME = "admin"
PASSWORD = "mSA3yuFiQ4PhD8z"
DATABASE = "bdt"

print(f"Connecting to Hive at {HOST}:{PORT} as '{USERNAME}' ...")

conn = hive.connect(
    host=HOST,
    port=PORT,
    username=USERNAME,
    password=PASSWORD,
    auth="CUSTOM",          # CUSTOM sends username+password over LDAP/plain
    configuration={"hive.server2.proxy.user": USERNAME},
)

cursor = conn.cursor()

print(f"Creating database '{DATABASE}' if not exists ...")
cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{DATABASE}`")
print(f"  Done.")

# Verify
cursor.execute("SHOW DATABASES")
dbs = [row[0] for row in cursor.fetchall()]
print(f"\nAll Hive databases: {dbs}")

if DATABASE in dbs:
    print(f"\n[OK] Database '{DATABASE}' exists and is ready.")
else:
    print(f"\n[WARN] Database '{DATABASE}' not found after creation — check Hive permissions.")

cursor.close()
conn.close()
