import asyncpg
import asyncio
import argparse
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
db_password = os.getenv("DB_PASSWORD")
async def create_local_database(db_config, db_name):
    """Create the local PostgreSQL database if it doesn't exist."""
    # Connect to the default 'postgres' database to create the new database
    try:
        conn = await asyncpg.connect(
            user=db_config['user'],
            password=db_password,
            host=db_config['host'],
            port=db_config['port'],
            database='postgres'  # Default database for administrative tasks
        )
        # Create database (IF NOT EXISTS ensures no error if it already exists)
        await conn.execute(f"CREATE DATABASE {db_name}")
        print(f"Database '{db_name}' created successfully.")
        await conn.close()
    except asyncpg.exceptions.DuplicateDatabaseError:
        print(f"Database '{db_name}' already exists.")
    except Exception as e:
        print(f"Error creating database: {e}")
        raise

async def create_local_table(conn):
    """Create the module_tests table in the local database."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS module_tests (
            id SERIAL PRIMARY KEY,
            module_name VARCHAR(100),
            test_type VARCHAR(50),
            test_data JSONB,
            imported_at TIMESTAMP
        );
    """)

async def fetch_cmu_data(module_name):
    """Fetch data from CMU's hgcdb database for a specific module."""
    mac_dict = {'CMU': {'host': 'cmsmac04.phys.cmu.edu', 'dbname': 'hgcdb'}}
    conn = await asyncpg.connect(
        user='viewer',
        database=mac_dict['CMU']['dbname'],
        host=mac_dict['CMU']['host']
    )
    
    # Fetch data for the specified module from the three tables
    query_iv = "SELECT *, 'iv' AS test_type FROM module_iv_test WHERE module_name = $1 ORDER BY mod_ivtest_no;"
    query_ped = "SELECT *, 'pedestal' AS test_type FROM module_pedestal_test WHERE module_name = $1 ORDER BY mod_pedtest_no;"
    query_qcs = "SELECT *, 'qc_summary' AS test_type FROM module_qc_summary WHERE module_name = $1 ORDER BY mod_qc_no;"
    
    iv_rows = await conn.fetch(query_iv, module_name)
    ped_rows = await conn.fetch(query_ped, module_name)
    qcs_rows = await conn.fetch(query_qcs, module_name)
    await conn.close()
    
    # Combine rows
    return [row for row in iv_rows + ped_rows + qcs_rows]

async def upload_to_local_db(data, local_db_config, db_name):
    """Upload data to the module_tests table in the local database."""
    # Connect to the newly created database
    conn = await asyncpg.connect(
        user=local_db_config['user'],
        password=local_db_config['password'],
        host=local_db_config['host'],
        port=local_db_config['port'],
        database=db_name
    )
    await create_local_table(conn)
    
    for row in data:
        await conn.execute(
            """
            INSERT INTO module_tests (module_name, test_type, test_data, imported_at)
            VALUES ($1, $2, $3, $4)
            """,
            row['module_name'],
            row['test_type'],
            {k: v for k, v in row.items() if k not in ('module_name', 'test_type')},
            datetime.now()
        )
    await conn.close()

async def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Fetch CMU module data and store in a local PostgreSQL database.")
    parser.add_argument('-mn', '--module_name', required=True, help="Module name to fetch data for (e.g., MODULE001)")
    args = parser.parse_args()
    module_name = args.module_name.upper()  # Normalize to uppercase, consistent with original script
    
    # Local database configuration (update with your actual credentials)
    local_db_config = {
        'user': 'postgres',  # Replace with your PostgreSQL superuser or a user with database creation privileges
        'host': 'localhost',
        'port': 5432
    }
    db_name = 'module_data'  # Name of the database to create
    
    # Create the local database
    await create_local_database(local_db_config, db_name)
    
    # Fetch CMU data for the specified module
    print(f"Fetching data for module {module_name} from CMU database...")
    cmu_data = await fetch_cmu_data(module_name)
    
    # Upload CMU data to local database
    if cmu_data:
        print(f"Uploading {len(cmu_data)} rows to module_tests in {db_name}...")
        await upload_to_local_db(cmu_data, local_db_config, db_name)
        print("Data uploaded successfully.")
    else:
        print(f"No data found for module {module_name} in CMU database.")

if __name__ == '__main__':
    asyncio.run(main())