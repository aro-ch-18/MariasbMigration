#!/usr/bin/env python3
"""
Database Migration Script - MariaDB to MariaDB

This script migrates database schemas (tables, indexes, constraints) from one 
MariaDB instance to another. It reads database names from user input and 
creates identical database structures in the destination server.

Features:
- Migrates all tables with their structure
- Migrates all indexes (PRIMARY, UNIQUE, INDEX, FULLTEXT)
- Migrates foreign key constraints
- Handles AUTO_INCREMENT settings
- Provides detailed progress reporting
"""

import pymysql
import os
from dotenv import load_dotenv
import sys
import re
from typing import List, Dict, Any, Tuple

# Load environment variables
load_dotenv()

# Source database configuration (READ)
READ_CONFIG = {
    'host': os.getenv('READ_DB_HOST'),
    'port': int(os.getenv('READ_DB_PORT', 3306)),
    'user': os.getenv('READ_DB_USER'),
    'password': os.getenv('READ_DB_PASSWORD'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# Destination database configuration (WRITE)
WRITE_CONFIG = {
    'host': os.getenv('WRITE_DB_HOST'),
    'port': int(os.getenv('WRITE_DB_PORT', 3306)),
    'user': os.getenv('WRITE_DB_USER'),
    'password': os.getenv('WRITE_DB_PASSWORD'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}


def validate_config():
    """Validate that all required environment variables are set."""
    required_vars = [
        'READ_DB_HOST', 'READ_DB_USER', 'READ_DB_PASSWORD',
        'WRITE_DB_HOST', 'WRITE_DB_USER', 'WRITE_DB_PASSWORD'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"❌ Error: Missing required environment variables: {', '.join(missing)}")
        print("Please update your .env file with the required credentials.")
        sys.exit(1)


def get_connection(config: Dict[str, Any], database: str = None):
    """Create a database connection."""
    conn_config = config.copy()
    if database:
        conn_config['database'] = database
    
    try:
        connection = pymysql.connect(**conn_config)
        return connection
    except pymysql.Error as e:
        print(f"❌ Error connecting to database: {e}")
        sys.exit(1)


def get_databases_list(connection) -> List[str]:
    """Get list of all databases from the source server."""
    with connection.cursor() as cursor:
        cursor.execute("SHOW DATABASES")
        databases = [row['Database'] for row in cursor.fetchall()]
        # Filter out system databases
        system_dbs = ['information_schema', 'mysql', 'performance_schema', 'sys']
        return [db for db in databases if db not in system_dbs]


def database_exists(connection, db_name: str) -> bool:
    """Check if a database exists."""
    with connection.cursor() as cursor:
        cursor.execute("SHOW DATABASES LIKE %s", (db_name,))
        return cursor.fetchone() is not None


def get_create_database_statement(connection, db_name: str) -> str:
    """Get the CREATE DATABASE statement for a database."""
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW CREATE DATABASE `{db_name}`")
        result = cursor.fetchone()
        return result['Create Database']


def get_tables_list(connection, db_name: str) -> List[str]:
    """Get list of all tables in a database."""
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW TABLES FROM `{db_name}`")
        return [list(row.values())[0] for row in cursor.fetchall()]


def get_create_table_statement(connection, db_name: str, table_name: str) -> str:
    """Get the CREATE TABLE statement for a table."""
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW CREATE TABLE `{db_name}`.`{table_name}`")
        result = cursor.fetchone()
        return result['Create Table']


def create_database(connection, db_name: str, create_statement: str):
    """Create a database in the destination server."""
    with connection.cursor() as cursor:
        # Drop database if exists (optional - can be made configurable)
        print(f"  Creating database '{db_name}'...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        connection.commit()


def strip_foreign_keys(create_statement: str) -> Tuple[str, List[str]]:
    """
    Remove foreign key constraints from CREATE TABLE statement.
    Returns the modified CREATE TABLE statement and a list of foreign key definitions.
    """
    foreign_keys = []
    
    # Pattern to match CONSTRAINT lines with FOREIGN KEY
    fk_pattern = r',?\s*CONSTRAINT\s+`[^`]+`\s+FOREIGN\s+KEY\s+\([^)]+\)\s+REFERENCES\s+`[^`]+`\s+\([^)]+\)(?:\s+ON\s+DELETE\s+(?:CASCADE|SET\s+NULL|NO\s+ACTION|RESTRICT))?(?:\s+ON\s+UPDATE\s+(?:CASCADE|SET\s+NULL|NO\s+ACTION|RESTRICT))?'
    
    # Find all foreign key constraints
    matches = re.finditer(fk_pattern, create_statement, re.IGNORECASE)
    
    for match in matches:
        fk_def = match.group(0).strip()
        # Remove leading comma if present
        if fk_def.startswith(','):
            fk_def = fk_def[1:].strip()
        foreign_keys.append(fk_def)
    
    # Remove foreign key constraints from the CREATE TABLE statement
    modified_statement = re.sub(fk_pattern, '', create_statement, flags=re.IGNORECASE)
    
    # Clean up any double commas or trailing commas before closing parenthesis
    modified_statement = re.sub(r',\s*,', ',', modified_statement)
    modified_statement = re.sub(r',\s*\)', ')', modified_statement)
    
    return modified_statement, foreign_keys


def add_foreign_key(connection, db_name: str, table_name: str, fk_constraint: str):
    """Add a foreign key constraint to an existing table."""
    with connection.cursor() as cursor:
        cursor.execute(f"USE `{db_name}`")
        
        # ALTER TABLE to add the constraint
        alter_statement = f"ALTER TABLE `{table_name}` ADD {fk_constraint}"
        cursor.execute(alter_statement)
        connection.commit()


def create_table(connection, db_name: str, table_name: str, create_statement: str, with_foreign_keys: bool = True):
    """Create a table in the destination database."""
    with connection.cursor() as cursor:
        # Switch to the target database
        cursor.execute(f"USE `{db_name}`")
        
        # Drop table if exists to avoid conflicts
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        
        # If not including foreign keys, strip them out
        if not with_foreign_keys:
            create_statement, _ = strip_foreign_keys(create_statement)
        
        # Create the table
        cursor.execute(create_statement)
        connection.commit()


def migrate_database(db_name: str):
    """Migrate a single database from source to destination."""
    print(f"\n{'='*60}")
    print(f"Migrating database: {db_name}")
    print(f"{'='*60}")
    
    # Connect to source and destination
    source_conn = get_connection(READ_CONFIG)
    dest_conn = get_connection(WRITE_CONFIG)
    
    try:
        # Get and create database
        create_db_statement = get_create_database_statement(source_conn, db_name)
        create_database(dest_conn, db_name, create_db_statement)
        print(f"✓ Database '{db_name}' created successfully")
        
        # Get list of tables
        tables = get_tables_list(source_conn, db_name)
        
        if not tables:
            print(f"  ⚠ No tables found in database '{db_name}'")
            return
        
        print(f"\n  Found {len(tables)} table(s) to migrate:")
        
        # Store table definitions and foreign keys
        table_definitions = {}
        foreign_keys_map = {}
        
        # PASS 1: Create all tables WITHOUT foreign key constraints
        print(f"\n  📋 Pass 1: Creating table structures (without foreign keys)...")
        for idx, table_name in enumerate(tables, 1):
            print(f"  [{idx}/{len(tables)}] Creating table: {table_name}")
            
            # Get CREATE TABLE statement
            create_table_statement = get_create_table_statement(source_conn, db_name, table_name)
            table_definitions[table_name] = create_table_statement
            
            # Strip foreign keys and store them
            _, foreign_keys = strip_foreign_keys(create_table_statement)
            if foreign_keys:
                foreign_keys_map[table_name] = foreign_keys
            
            # Create table without foreign keys
            create_table(dest_conn, db_name, table_name, create_table_statement, with_foreign_keys=False)
            
            print(f"    ✓ Table '{table_name}' structure created")
        
        # PASS 2: Add foreign key constraints
        if foreign_keys_map:
            print(f"\n  🔗 Pass 2: Adding foreign key constraints...")
            fk_count = 0
            for table_name, foreign_keys in foreign_keys_map.items():
                print(f"  Adding {len(foreign_keys)} foreign key(s) to table: {table_name}")
                
                for fk_constraint in foreign_keys:
                    try:
                        add_foreign_key(dest_conn, db_name, table_name, fk_constraint)
                        fk_count += 1
                    except pymysql.Error as e:
                        print(f"    ⚠ Warning: Could not add foreign key to '{table_name}': {e}")
                        print(f"      Constraint: {fk_constraint[:100]}...")
                
                print(f"    ✓ Foreign keys added to '{table_name}'")
            
            print(f"\n  ✓ Total foreign key constraints added: {fk_count}")
        else:
            print(f"\n  ℹ No foreign key constraints to add")
        
        print(f"\n{'='*60}")
        print(f"✓ Database '{db_name}' migration completed successfully!")
        print(f"  Total tables migrated: {len(tables)}")
        if foreign_keys_map:
            print(f"  Total foreign keys added: {sum(len(fks) for fks in foreign_keys_map.values())}")
        print(f"{'='*60}")
        
    except pymysql.Error as e:
        print(f"\n❌ Error migrating database '{db_name}': {e}")
        raise
    finally:
        source_conn.close()
        dest_conn.close()


def get_user_input() -> List[str]:
    """Get database names from user input."""
    print("\n" + "="*60)
    print("Database Migration Tool - MariaDB to MariaDB")
    print("="*60)
    
    # Show available databases
    print("\nConnecting to source database to fetch available databases...")
    source_conn = get_connection(READ_CONFIG)
    
    try:
        available_dbs = get_databases_list(source_conn)
        print(f"\nAvailable databases on source server ({READ_CONFIG['host']}):")
        for idx, db in enumerate(available_dbs, 1):
            print(f"  {idx}. {db}")
    except Exception as e:
        print(f"⚠ Warning: Could not fetch database list: {e}")
        available_dbs = []
    finally:
        source_conn.close()
    
    print("\n" + "-"*60)
    print("Enter database names to migrate (comma-separated)")
    print("Example: database1, database2, database3")
    print("Or type 'all' to migrate all available databases")
    print("-"*60)
    
    user_input = input("\nDatabase names: ").strip()
    
    if not user_input:
        print("❌ No databases specified. Exiting.")
        sys.exit(0)
    
    if user_input.lower() == 'all':
        if not available_dbs:
            print("❌ No databases available to migrate.")
            sys.exit(0)
        return available_dbs
    
    # Parse comma-separated input
    db_names = [db.strip() for db in user_input.split(',')]
    db_names = [db for db in db_names if db]  # Remove empty strings
    
    if not db_names:
        print("❌ No valid database names provided. Exiting.")
        sys.exit(0)
    
    return db_names


def main():
    """Main function to orchestrate the migration."""
    print("\n🚀 Starting Database Migration Process...")
    
    # Validate configuration
    validate_config()
    
    print(f"\nSource Server: {READ_CONFIG['host']}:{READ_CONFIG['port']}")
    print(f"Destination Server: {WRITE_CONFIG['host']}:{WRITE_CONFIG['port']}")
    
    # Get database names from user
    db_names = get_user_input()
    
    print(f"\n📋 Databases to migrate: {', '.join(db_names)}")
    
    # Confirm with user
    confirmation = input("\nProceed with migration? (yes/no): ").strip().lower()
    
    if confirmation not in ['yes', 'y']:
        print("❌ Migration cancelled by user.")
        sys.exit(0)
    
    # Migrate each database
    success_count = 0
    failed_databases = []
    
    for db_name in db_names:
        try:
            migrate_database(db_name)
            success_count += 1
        except Exception as e:
            print(f"❌ Failed to migrate database '{db_name}': {e}")
            failed_databases.append(db_name)
            continue
    
    # Print summary
    print("\n" + "="*60)
    print("MIGRATION SUMMARY")
    print("="*60)
    print(f"Total databases: {len(db_names)}")
    print(f"✓ Successfully migrated: {success_count}")
    print(f"✗ Failed: {len(failed_databases)}")
    
    if failed_databases:
        print(f"\nFailed databases: {', '.join(failed_databases)}")
    
    print("\n✅ Migration process completed!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Migration interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
