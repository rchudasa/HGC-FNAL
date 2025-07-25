import asyncpg
import asyncio
import csv
import os
import argparse
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()
db_password = os.getenv("DB_PASSWORD")

# Connection settings for the initial "postgres" database
PG_USER = "postgres"          # Change if you use a different user
PG_HOST = "localhost"
PG_PORT = 5432

NEW_DB_NAME = "test_db"

async def create_database():
    # Connect to the default 'postgres' database
    conn = await asyncpg.connect(
        user=PG_USER,
        password=db_password,
        host=PG_HOST,
        port=PG_PORT,
        database="postgres"
    )

    # Create the new database
    try:
        await conn.execute(f'CREATE DATABASE {NEW_DB_NAME}')
        print(f"Database '{NEW_DB_NAME}' created successfully.")
    except asyncpg.DuplicateDatabaseError:
        print(f"Database '{NEW_DB_NAME}' already exists.")
    finally:
        await conn.close()

async def setup_table():
    # Connect to the new database
    conn = await asyncpg.connect(
        user=PG_USER,
        password=db_password,
        host=PG_HOST,
        port=PG_PORT,
        database=NEW_DB_NAME
    )

    # Create a simple table and insert a row
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT,
            email TEXT UNIQUE
        )
    """)

    await conn.execute("""
        INSERT INTO users (name, email)
        VALUES ($1, $2)
        ON CONFLICT (email) DO NOTHING
    """, "Alice", "alice@example.com")

    print("Table 'users' created and one row inserted.")
    await conn.close()

async def main():
    await create_database()
    await setup_table()

if __name__ == "__main__":
    asyncio.run(main())