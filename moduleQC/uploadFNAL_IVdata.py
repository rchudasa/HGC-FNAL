import asyncpg
import asyncio
import argparse
from datetime import datetime
from dotenv import load_dotenv
import os
import csv
from pathlib import Path
import re
import pandas as pd

# Load environment variables for secure database access
load_dotenv()
db_password = os.getenv("DB_PASSWORD")

# Default values for temperature and relative humidity
DEFAULT_TEMPERATURE = 25.0
DEFAULT_RH = 50.0

async def create_local_database(db_config, db_name):
    """Create the local PostgreSQL database if it doesn't exist."""
    try:
        # Connect to the default 'postgres' database for administrative tasks
        conn = await asyncpg.connect(
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port'],
            database='postgres'
        )
        # Create the database if it doesn't exist
        await conn.execute(f"CREATE DATABASE {db_name}")
        print(f"Database '{db_name}' created successfully.")
        await conn.close()
    except asyncpg.exceptions.DuplicateDatabaseError:
        print(f"Database '{db_name}' already exists.")
    except Exception as e:
        print(f"Error creating database: {e}")
        raise

async def verify_and_update_table_schema(conn):
    """Verify and update the module_tests table schema to include only required columns."""
    # Define the expected columns and their data types
    expected_columns = {
        'module_name': 'TEXT',
        'test_type': 'TEXT',
        'meas_v': 'REAL[]',
        'meas_i': 'REAL[]',
        'rel_hum': 'TEXT',
        'temp_c': 'TEXT',
        'date_test': 'DATE',
        'test_timestamp': 'TIMESTAMP',
        'imported_at': 'TIMESTAMP', 
        'comments' : 'TEXT'  # Optional comments column
    }

    # Query existing columns in the module_tests table
    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'module_tests';
    """
    existing_columns = {row['column_name']: row['data_type'] for row in await conn.fetch(query)}

    # Create the table if it doesn't exist
    if not existing_columns:
        print("Creating module_tests table...")
        await conn.execute("""
            CREATE TABLE module_tests (
                id SERIAL PRIMARY KEY,
                module_name TEXT,
                test_type TEXT,
                meas_v REAL[],
                meas_i REAL[],
                rel_hum TEXT,
                temp_c TEXT,
                date_test DATE,
                test_timestamp TIMESTAMP,
                imported_at TIMESTAMP, 
                comments TEXT
            );
        """)
        return

    # Add missing columns or modify types if necessary
    for col_name, col_type in expected_columns.items():
        if col_name not in existing_columns:
            print(f"Adding missing column {col_name} to module_tests...")
            await conn.execute(f"ALTER TABLE module_tests ADD COLUMN {col_name} {col_type};")
        elif existing_columns[col_name].lower() != col_type.lower():
            print(f"Modifying column {col_name} type to {col_type}...")
            await conn.execute(f"ALTER TABLE module_tests ALTER COLUMN {col_name} TYPE {col_type};")

def get_environmental_data(filepath):
    """Prompt user for temperature and RH for the given file, returning as strings."""
    print(f"\nProcessing file: {filepath}")
    try:
        # Prompt for temperature and use default if input is empty
        temp_input = input(f"Enter temperature (°C) for {filepath} [default: {DEFAULT_TEMPERATURE}]: ")
        temperature = str(float(temp_input)) if temp_input.strip() else str(DEFAULT_TEMPERATURE)
        # Prompt for relative humidity and use default if input is empty
        rh_input = input(f"Enter relative humidity (%) for {filepath} [default: {DEFAULT_RH}]: ")
        rh = str(float(rh_input)) if rh_input.strip() else str(DEFAULT_RH)
        comments = input(f"Enter any comments for {filepath} (optional): ")
        return temperature, rh, comments
    except ValueError:
        print(f"Invalid input for {filepath}. Using defaults: {DEFAULT_TEMPERATURE}°C, {DEFAULT_RH}%")
        return str(DEFAULT_TEMPERATURE), str(DEFAULT_RH)

def parse_timestamp_from_filename(filename):
    """Extract test date and time from filename (e.g., MODULE001_20250115_143022.txt)."""
    # Regex to match YYYYMMDD_HHMMSS (e.g., 20250115_143022)
    match = re.search(r'(\d{8})_(\d{6})', filename)
    if match:
        date_str, time_str = match.groups()
        try:
            # Parse date and time into a datetime object
            timestamp = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
            return timestamp
        except ValueError:
            print(f"Invalid date/time format in filename: {filename}")
    print(f"Could not parse date/time from filename: {filename}. Using current timestamp.")
    return datetime.now()

def read_text_file(file_path):
    df = pd.read_csv(file_path, sep='\s+', header=None, names=['Bias voltage', 'Leakage current'])
    df['Bias voltage']=df['Bias voltage'].abs()
    df['Leakage current']=df['Leakage current'].abs()

    return df['Bias voltage'].tolist(), df['Leakage current'].tolist()

async def parse_iv_file(filepath, temperature, relative_humidity, addComments):
    """Parse IV data from a text file, including timestamp from filename."""
    # Extract module name from filename (before date/time)
    module_name_temp = Path(filepath).stem.split('_')[1]
    module_name = module_name_temp.replace("-", "")
    # Parse test date and time from filename
    test_timestamp = parse_timestamp_from_filename(Path(filepath).name)
    date_test = test_timestamp.date()
    tests = []
    current_test = {'meas_v': [], 'meas_i': []}
    test_counter = 1

    current_test['meas_v'], current_test['meas_i'] = read_text_file(filepath)

    #print("Module Name:", module_name, current_test['meas_v'], current_test['meas_i'])
    print("Module Name:", module_name)

    # Save the last test if it has data
    if current_test['meas_v']:
        tests.append({
            'module_name': module_name,
            'test_type': 'iv',
            'meas_v': current_test['meas_v'],
            'meas_i': current_test['meas_i'],
            'rel_hum': relative_humidity,
            'temp_c': temperature,
            'date_test': date_test,
            'test_timestamp': test_timestamp,
            'imported_at': datetime.now(), 
            'comments': addComments if addComments else None
        })

    return tests

async def upload_to_local_db(data, local_db_config, db_name):
    """Upload parsed IV data to the module_tests table in the local database."""
    # Connect to the local database
    conn = await asyncpg.connect(
        user=local_db_config['user'],
        password=local_db_config['password'],
        host=local_db_config['host'],
        port=local_db_config['port'],
        database=db_name
    )

    # Ensure the table schema is correct
    await verify_and_update_table_schema(conn)

    # Insert each test record into the table
    for row in data:
        await conn.execute(
            """
            INSERT INTO module_tests (
                module_name, test_type, meas_v, meas_i, rel_hum, temp_c, date_test, test_timestamp, imported_at, comments
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            row['module_name'],
            row['test_type'],
            row['meas_v'],
            row['meas_i'],
            row['rel_hum'],
            row['temp_c'],
            row['date_test'],
            row['test_timestamp'],
            row['imported_at'], 
            row['comments'] if 'comments' in row else None
        )
    await conn.close()

async def main():
    # Parse command-line arguments for directory and optional module name
    parser = argparse.ArgumentParser(description="Upload IV test data from text files to a local PostgreSQL database.")
    parser.add_argument('-d', '--directory', required=True, help="Directory containing IV test data text files")
    parser.add_argument('-mn', '--module_name', default=None, help="Optional module name to process specific files (e.g., MODULE001)")
    args = parser.parse_args()
    data_directory = args.directory
    module_name = args.module_name.upper() if args.module_name else None

    # Local database configuration
    local_db_config = {
        'user': 'postgres',
        'password': db_password,
        'host': 'localhost',
        'port': 5432
    }
    db_name = 'hgcdb_fnal'

    # Create the database if it doesn't exist
    await create_local_database(local_db_config, db_name)

    # Process each text file in the directory
    for filename in sorted(os.listdir(data_directory)):
        if (filename.endswith('.txt')):
            print(f"Processing file: {filename}")
            filepath = os.path.join(data_directory, filename)
            # Get temperature and RH for the file
            temperature, relative_humidity,addComments = get_environmental_data(filepath)
            print(f"Processing {filepath} with Temperature: {temperature}°C, RH: {relative_humidity}%")
            # Parse IV data from the file
            tests = await parse_iv_file(filepath, temperature, relative_humidity, addComments)
            if tests:
                print(f"Uploading {len(tests)} tests for module {module_name} to {db_name}...")
                # Upload the data to the database
                await upload_to_local_db(tests, local_db_config, db_name)
                print(f"Data uploaded successfully for {filepath}.")
            else:
                print(f"No valid data found in {filepath}")

if __name__ == '__main__':
    asyncio.run(main())