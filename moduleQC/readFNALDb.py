import asyncpg
import asyncio
import argparse
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()
db_password = os.getenv("DB_PASSWORD")

async def read_module_tests(db_config, db_name, module_name=None):
    """Read data from the module_tests table in the local database."""
    try:
        conn = await asyncpg.connect(
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port'],
            database=db_name
        )
        
        # Build the query with optional filters
        query = """
            SELECT id, module_name, status, status_desc, ratio_i_at_vs, ratio_at_vs,
                   rel_hum, temp_c, date_test, meas_v, meas_i, imported_at
            FROM module_tests
        """
        conditions = []
        params = []
        
        if module_name:
            conditions.append("module_name = $1")
            params.append(module_name)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY imported_at DESC;"
        
        rows = await conn.fetch(query, *params)
        
        if rows:
            print(f"Found {len(rows)} rows in module_tests:")
            for row in rows:
                print("\nRecord:")
                print(f"  ID: {row['id']}")
                print(f"  Module Name: {row['module_name']}")
                print(f"  Status: {row['status']}")
                print(f"  Status Description: {row['status_desc']}")
                print(f"  Ratio I at VS: {row['ratio_i_at_vs']}")
                print(f"  Ratio at VS: {row['ratio_at_vs']}")
                print(f"  Relative Humidity: {row['rel_hum']}")
                print(f"  Temperature (°C): {row['temp_c']}")
                print(f"  Test Date: {row['date_test']}")
                print(f"  Measured Voltage: {row['meas_v']}")
                print(f"  Measured Current: {row['meas_i']}")
                print(f"  Imported At: {row['imported_at']}")
        else:
            print(f"No data found in module_tests for module_name={module_name or 'any'}.")
        
        await conn.close()
    except Exception as e:
        print(f"Error reading database: {e}")

async def main():
    parser = argparse.ArgumentParser(description="Read data from the module_tests table in the local PostgreSQL database.")
    parser.add_argument('-mn', '--module_name', help="Module name to filter by (e.g., MODULE001)", default=None)
    args = parser.parse_args()
    module_name = args.module_name.upper() if args.module_name else None
    
    db_config = {
        'user': 'postgres',
        'password': db_password,
        'host': 'localhost',
        'port': 5432
    }
    db_name = 'hgcdb_fnal'
    
    await read_module_tests(db_config, db_name, module_name)

if __name__ == '__main__':
    asyncio.run(main())