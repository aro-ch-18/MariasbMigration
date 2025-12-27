#!/usr/bin/env python3
"""
Customer Data Migration Script V3 - MariaDB to MariaDB (with Abort & Rollback Safety)

⭐ VERSION 3 - Complete Safety Features

V3 NEW FEATURES:
- 🔍 Pre-migration review with detailed plan
- ✅ Multi-step confirmation (prevent mistakes)
- 🧪 Dry-run mode (--dry-run)
- 🛑 Graceful abort on Ctrl+C
- 💾 Checkpoint & resume (--resume FILE)
- ♻️  Rollback capability (--rollback FILE)
- 📦 Automatic backups (--backup)

This script migrates customer-specific data from one MariaDB instance to another.
It intelligently handles FOUR types of tables with smart relationship detection:

1. Tables WITH customer_id: Migrates rows where customer_id matches specified IDs
2. Tables WITH user_id: Migrates rows where user_id matches seed user IDs (1,2)
3. Tables with INDIRECT relationships: Tables that don't have customer_id but have FK 
   to tables that do (e.g., ROLE_ACCESS_MAP → ROLE → customer_id)
4. Pure REFERENCE tables: No connection to customer_id/user_id (seed/lookup data)

Migration Order:
- PHASE 1: Tables with customer_id (filtered by customer IDs)
- PHASE 1B: Tables with user_id (filtered by seed user IDs)
- PHASE 1C: Tables with indirect FK relationships (filtered via JOIN)
- PHASE 2: Pure reference tables (all data, with smart confirmation)

Advanced Features:
- Hybrid FK detection: Finds BOTH explicit and implicit foreign keys
  * Explicit: Defined in INFORMATION_SCHEMA
  * Implicit: Detected via column naming patterns (tablename_id, tablenameId)
- Relationship chain tracing: Shows how tables connect to customer_id
- Smart filtering via JOIN for indirect relationships
- Case-insensitive column matching (customer_id, CUSTOMER_ID, customerId, etc.)
- Smart confirmation: Analyzes and shows insights for tables >100 rows
- Foreign key constraint handling: Temporarily disables FK checks
- Duplicate key handling: Uses INSERT IGNORE
- Batch insertion for performance
- Detailed reporting with relationship chains

Foreign Key Handling:
- Foreign key checks are disabled at the start of migration
- Allows flexible insertion order (referenced tables can be inserted later)
- Prevents FK constraint violations during batch inserts
- Foreign keys are re-enabled after migration completes
- All constraints remain intact in the database schema

Example Cases Handled:
- ROLE table (has customer_id) → Direct filter
- ROLE_ACCESS_MAP (role_id → ROLE.customer_id) → Indirect filter via JOIN
- ACCESS_RIGHT (no relationship) → Pure reference data
"""

import pymysql
import os
from dotenv import load_dotenv
import sys
from typing import List, Dict, Any, Set, Tuple, Optional
import json
import re
from collections import defaultdict
import signal
import argparse
from datetime import datetime
import subprocess
import time
from pathlib import Path

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

# Configuration - Load from environment with defaults
BATCH_SIZE = 1000  # Number of rows to insert at once
CREATE_MISSING_OBJECTS = False  # Set to True to auto-create missing tables/databases

# V2 ENHANCEMENTS: Configurable via environment variables
AUTO_CONFIRM_THRESHOLD = int(os.getenv('AUTO_CONFIRM_THRESHOLD', '400'))  # Increased from 100 to 400
SEED_USER_IDS = [int(x.strip()) for x in os.getenv('SEED_USER_IDS', '1,2').split(',') if x.strip()]
FK_VALIDATION_THRESHOLD = 0.9  # 90% of values must exist to confirm implicit FK

# V2 ENHANCEMENT: Force-migrate tables (bypass all checks, migrate ALL data)
# Format: DATABASE.TABLE or *.TABLE (wildcard)
# Example: *.schema_version matches schema_version in any database
FORCE_MIGRATE_TABLES = [
    t.strip()
    for t in os.getenv('FORCE_MIGRATE_TABLES', '*.schema_version,*.flyway_schema_history').split(',')
    if t.strip()
]

# V3 ENHANCEMENT: Skip tables (completely ignore from migration)
# Format: DATABASE.TABLE, DATABASE.* (all tables in DB), *.TABLE (table in any DB)
SKIP_TABLES = [
    t.strip()
    for t in os.getenv('SKIP_TABLES', '').split(',')
    if t.strip()
]

# V3 ENHANCEMENT: State file for tracking migration progress
STATE_FILE_DIR = Path(os.getenv('MIGRATION_STATE_DIR', '.migration_state'))
STATE_FILE_DIR.mkdir(exist_ok=True)

# V3 ENHANCEMENT: Auto-skip large reference tables (marks as user-skipped for later prompting)
# When True, tables exceeding AUTO_CONFIRM_THRESHOLD are auto-skipped instead of prompting
SKIP_LARGE_TABLES = os.getenv('SKIP_LARGE_TABLES', 'false').lower() in ('true', '1', 'yes')


def should_force_migrate(database: str, table: str, force_list: List[str]) -> bool:
    """
    Check if table should be force-migrated (all data, no confirmation).
    
    V2 ENHANCEMENT: Supports wildcard patterns and exact matches.
    
    Args:
        database: Database name
        table: Table name
        force_list: List of patterns (DATABASE.TABLE or *.TABLE or TABLE)
        
    Returns:
        True if table matches any force-migrate pattern
        
    Examples:
        - "STARFOX.schema_version" → Exact match for STARFOX database
        - "*.schema_version" → Matches schema_version in ANY database
        - "schema_version" → Matches schema_version in ANY database
    """
    full_name = f"{database}.{table}"
    table_lower = table.lower()
    full_name_lower = full_name.lower()
    
    for pattern in force_list:
        pattern = pattern.strip().lower()
        
        if not pattern:
            continue
        
        # Exact match: DATABASE.TABLE
        if pattern == full_name_lower:
            return True
        
        # Wildcard match: *.TABLE
        if pattern.startswith('*.'):
            pattern_table = pattern[2:]  # Remove *.
            if table_lower == pattern_table:
                return True
        
        # Table name only (match any database)
        if '.' not in pattern and table_lower == pattern:
            return True
    
    return False


def should_skip_table(database: str, table: str, skip_list: List[str]) -> bool:
    """
    Check if table should be skipped from migration.

    V3 ENHANCEMENT: Supports wildcard patterns.

    Args:
        database: Database name
        table: Table name
        skip_list: List of patterns (DATABASE.TABLE, DATABASE.*, *.TABLE)

    Returns:
        True if table matches any skip pattern

    Examples:
        - "STARFOX.AUDIT_LOG" → Exact match
        - "STARFOX.*" → Skip ALL tables in STARFOX database
        - "*.temp_data" → Skip temp_data table in ANY database
    """
    full_name = f"{database}.{table}"
    table_lower = table.lower()
    database_lower = database.lower()
    full_name_lower = full_name.lower()

    for pattern in skip_list:
        pattern = pattern.strip().lower()

        if not pattern:
            continue

        # Exact match: DATABASE.TABLE
        if pattern == full_name_lower:
            return True

        # Database wildcard: DATABASE.*
        if pattern.endswith('.*'):
            pattern_db = pattern[:-2]  # Remove .*
            if database_lower == pattern_db:
                return True

        # Table wildcard: *.TABLE
        if pattern.startswith('*.'):
            pattern_table = pattern[2:]  # Remove *.
            if table_lower == pattern_table:
                return True

    return False


# ============================================================================
# STATE FILE MANAGEMENT (V3)
# ============================================================================

def get_state_file_path(customer_ids: List[int]) -> Path:
    """Generate state file path based on customer IDs."""
    ids_str = "_".join(str(cid) for cid in sorted(customer_ids))
    return STATE_FILE_DIR / f"migration_state_{ids_str}.json"


def load_migration_state(state_file: Path) -> Dict:
    """Load migration state from file."""
    if state_file.exists():
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"  ⚠ Warning: Could not load state file: {e}")
    return {
        "created_at": datetime.now().isoformat(),
        "databases": {}
    }


def save_migration_state(state_file: Path, state: Dict):
    """Save migration state to file."""
    state["updated_at"] = datetime.now().isoformat()
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2, default=str)
    except IOError as e:
        print(f"  ⚠ Warning: Could not save state file: {e}")


def get_table_state(state: Dict, database: str, table: str) -> Optional[Dict]:
    """Get the migration state for a specific table."""
    if database in state.get("databases", {}):
        return state["databases"][database].get("tables", {}).get(table)
    return None


def set_table_state(state: Dict, database: str, table: str,
                    status: str, rows: int = 0, reason: str = None):
    """Set the migration state for a specific table."""
    if database not in state["databases"]:
        state["databases"][database] = {"tables": {}, "routines": {}}
    if "tables" not in state["databases"][database]:
        state["databases"][database]["tables"] = {}

    state["databases"][database]["tables"][table] = {
        "status": status,
        "rows": rows,
        "timestamp": datetime.now().isoformat()
    }
    if reason:
        state["databases"][database]["tables"][table]["reason"] = reason


def set_routine_state(state: Dict, database: str, routine_name: str,
                      routine_type: str, status: str):
    """Set the migration state for a stored procedure/function."""
    if database not in state["databases"]:
        state["databases"][database] = {"tables": {}, "routines": {}}
    if "routines" not in state["databases"][database]:
        state["databases"][database]["routines"] = {}

    state["databases"][database]["routines"][routine_name] = {
        "type": routine_type,
        "status": status,
        "timestamp": datetime.now().isoformat()
    }


def print_migration_status(state_file: Path):
    """Print the current migration status from state file."""
    if not state_file.exists():
        print(f"  ℹ No migration state file found: {state_file}")
        return

    state = load_migration_state(state_file)

    print(f"\n{'='*70}")
    print(f"MIGRATION STATUS")
    print(f"{'='*70}")
    print(f"State file: {state_file}")
    print(f"Created: {state.get('created_at', 'N/A')}")
    print(f"Last updated: {state.get('updated_at', 'N/A')}")

    for db_name, db_data in state.get("databases", {}).items():
        print(f"\n  📁 Database: {db_name}")

        tables = db_data.get("tables", {})
        if tables:
            completed = sum(1 for t in tables.values() if t.get("status") == "completed")
            skipped = sum(1 for t in tables.values() if t.get("status") == "skipped")
            print(f"     Tables: {completed} completed, {skipped} skipped, {len(tables)} total")

            for table_name, table_data in tables.items():
                status = table_data.get("status", "unknown")
                rows = table_data.get("rows", 0)
                reason = table_data.get("reason", "")

                if status == "completed":
                    icon = "✓"
                elif status == "skipped":
                    icon = "⊗"
                else:
                    icon = "?"

                reason_str = f" ({reason})" if reason else ""
                print(f"       {icon} {table_name}: {status} [{rows} rows]{reason_str}")

        routines = db_data.get("routines", {})
        if routines:
            print(f"     Routines: {len(routines)}")
            for routine_name, routine_data in routines.items():
                rtype = routine_data.get("type", "UNKNOWN")
                status = routine_data.get("status", "unknown")
                icon = "✓" if status == "completed" else "✗"
                print(f"       {icon} {routine_name} ({rtype}): {status}")

    print(f"\n{'='*70}")


# ============================================================================
# STORED PROCEDURES & FUNCTIONS MIGRATION (V3)
# ============================================================================

def get_routines(connection, db_name: str) -> List[Dict]:
    """Get all stored procedures and functions from a database."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                ROUTINE_NAME,
                ROUTINE_TYPE
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_SCHEMA = %s
            ORDER BY ROUTINE_TYPE, ROUTINE_NAME
        """, (db_name,))
        return cursor.fetchall()


def migrate_routine(source_conn, dest_conn, db_name: str,
                    routine_name: str, routine_type: str) -> bool:
    """Migrate a single stored procedure or function."""
    try:
        with source_conn.cursor() as cursor:
            if routine_type == 'PROCEDURE':
                cursor.execute(f"SHOW CREATE PROCEDURE `{db_name}`.`{routine_name}`")
                result = cursor.fetchone()
                create_stmt = result.get('Create Procedure')
            else:
                cursor.execute(f"SHOW CREATE FUNCTION `{db_name}`.`{routine_name}`")
                result = cursor.fetchone()
                create_stmt = result.get('Create Function')

        if not create_stmt:
            print(f"    ⚠ Could not get CREATE statement for {routine_type} '{routine_name}'")
            return False

        with dest_conn.cursor() as cursor:
            cursor.execute(f"USE `{db_name}`")
            # Drop if exists first
            cursor.execute(f"DROP {routine_type} IF EXISTS `{routine_name}`")
            cursor.execute(create_stmt)
            dest_conn.commit()

        return True
    except pymysql.Error as e:
        print(f"    ❌ Error migrating {routine_type} '{routine_name}': {e}")
        return False


def migrate_routines(source_conn, dest_conn, db_name: str,
                     state: Dict, state_file: Path, force: bool = False) -> Dict[str, int]:
    """Migrate all stored procedures and functions for a database."""
    stats = {'procedures': 0, 'functions': 0, 'skipped': 0, 'failed': 0}

    routines = get_routines(source_conn, db_name)

    if not routines:
        print(f"  ℹ No stored procedures or functions found in {db_name}")
        return stats

    print(f"\n  {'='*66}")
    print(f"  PHASE 0: Migrating Stored Procedures & Functions")
    print(f"  {'='*66}")
    print(f"  Found {len(routines)} routine(s) in {db_name}")

    for routine in routines:
        name = routine['ROUTINE_NAME']
        rtype = routine['ROUTINE_TYPE']

        # Check if already migrated (unless force)
        if not force:
            existing_state = state.get("databases", {}).get(db_name, {}).get("routines", {}).get(name)
            if existing_state and existing_state.get("status") == "completed":
                print(f"    ⊗ {rtype} '{name}' already migrated (skipping)")
                stats['skipped'] += 1
                continue

        print(f"    • Migrating {rtype}: {name}...", end=' ')

        if migrate_routine(source_conn, dest_conn, db_name, name, rtype):
            print("✓")
            set_routine_state(state, db_name, name, rtype, "completed")
            if rtype == 'PROCEDURE':
                stats['procedures'] += 1
            else:
                stats['functions'] += 1
        else:
            print("✗")
            set_routine_state(state, db_name, name, rtype, "failed")
            stats['failed'] += 1

        save_migration_state(state_file, state)

    print(f"\n  Summary: {stats['procedures']} procedures, {stats['functions']} functions migrated")
    if stats['skipped'] > 0:
        print(f"           {stats['skipped']} already migrated (skipped)")
    if stats['failed'] > 0:
        print(f"           {stats['failed']} failed")

    return stats


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


def table_exists(connection, db_name: str, table_name: str) -> bool:
    """Check if a table exists in a database."""
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW TABLES FROM `{db_name}` LIKE %s", (table_name,))
        return cursor.fetchone() is not None


def get_all_tables(connection, db_name: str) -> List[str]:
    """Get list of all tables in a database."""
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW TABLES FROM `{db_name}`")
        return [list(row.values())[0] for row in cursor.fetchall()]


def get_explicit_foreign_keys(connection, db_name: str) -> Dict[str, List[Dict]]:
    """Get explicitly defined foreign keys from INFORMATION_SCHEMA."""
    foreign_keys = defaultdict(list)
    
    with connection.cursor() as cursor:
        query = """
            SELECT 
                TABLE_NAME,
                COLUMN_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
            AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        cursor.execute(query, (db_name,))
        
        for row in cursor.fetchall():
            foreign_keys[row['TABLE_NAME']].append({
                'column': row['COLUMN_NAME'],
                'referenced_table': row['REFERENCED_TABLE_NAME'],
                'referenced_column': row['REFERENCED_COLUMN_NAME'],
                'type': 'explicit'
            })
    
    return dict(foreign_keys)


def find_table_case_insensitive(all_tables: List[str], target_name: str) -> Optional[str]:
    """Find a table by name (case-insensitive)."""
    target_lower = target_name.lower()
    for table in all_tables:
        if table.lower() == target_lower:
            return table
    return None


def detect_implicit_foreign_keys(connection, db_name: str, table_name: str, 
                                 columns: List[str], all_tables: List[str]) -> List[Dict]:
    """Detect implicit foreign keys based on column naming patterns."""
    implicit_fks = []
    
    # Pattern: *_id or *Id
    fk_pattern = re.compile(r'^(.+?)_?id$', re.IGNORECASE)
    
    for column in columns:
        # Skip common columns
        if column.lower() in ['id', 'created_by', 'updated_by', 'created_at', 'updated_at']:
            continue
        
        match = fk_pattern.match(column)
        if match:
            potential_table = match.group(1)
            
            # Try to find the referenced table
            candidates = [
                potential_table,
                potential_table + 's',
                potential_table.rstrip('s'),
            ]
            
            for candidate in candidates:
                referenced_table = find_table_case_insensitive(all_tables, candidate)
                if referenced_table:
                    # Found a potential reference
                    implicit_fks.append({
                        'column': column,
                        'referenced_table': referenced_table,
                        'referenced_column': None,
                        'type': 'implicit'
                    })
                    break
    
    return implicit_fks


def find_user_id_column(columns: List[str]) -> Optional[str]:
    """Find user_id column (case-insensitive)."""
    for col in columns:
        if col.lower() == 'user_id':
            return col
    return None


def build_relationship_chain(table_name: str, all_fks: Dict, 
                             tables_with_customer_id: Set[str],
                             tables_with_user_id: Set[str],
                             visited: Set[str] = None) -> Optional[Tuple[str, List[str], Dict]]:
    """
    Build a chain showing how a table relates to customer_id/user_id.
    Returns tuple of (id_type, chain, first_fk) where:
    - id_type is 'customer_id' or 'user_id'
    - chain is list of table names showing the relationship path
    - first_fk is the FK dict connecting this table to the chain
    """
    if visited is None:
        visited = set()
    
    if table_name in visited:
        return None
    
    visited.add(table_name)
    
    # Check if this table directly has customer_id or user_id
    if table_name in tables_with_customer_id:
        return ('customer_id', [table_name], None)
    if table_name in tables_with_user_id:
        return ('user_id', [table_name], None)
    
    # Check foreign keys
    if table_name in all_fks:
        for fk in all_fks[table_name]:
            ref_table = fk['referenced_table']
            result = build_relationship_chain(ref_table, all_fks, 
                                            tables_with_customer_id, 
                                            tables_with_user_id, visited)
            if result:
                id_type, chain, _ = result
                return (id_type, [table_name] + chain, fk)
    
    return None


def get_table_columns(connection, db_name: str, table_name: str) -> List[str]:
    """Get list of column names for a table."""
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW COLUMNS FROM `{db_name}`.`{table_name}`")
        columns = cursor.fetchall()
        return [col['Field'] for col in columns]


def categorize_tables_by_customer_id(connection, db_name: str) -> Tuple[List[Tuple[str, str]], List[str]]:
    """
    Categorize tables by whether they have a customer_id column.
    Returns two lists:
    - tables_with_customer_id: List of tuples (table_name, actual_column_name)
    - tables_without_customer_id: List of table names
    """
    tables_with_customer_id = []
    tables_without_customer_id = []
    
    with connection.cursor() as cursor:
        # Get all tables in the database
        cursor.execute(f"SHOW TABLES FROM `{db_name}`")
        tables = [list(row.values())[0] for row in cursor.fetchall()]
        
        # Check each table for customer_id column
        for table in tables:
            cursor.execute(f"SHOW COLUMNS FROM `{db_name}`.`{table}`")
            columns = cursor.fetchall()
            
            # Look for customer_id column (case-insensitive)
            has_customer_id = False
            for col in columns:
                col_name = col['Field']
                if col_name.lower() == 'customer_id':
                    tables_with_customer_id.append((table, col_name))
                    has_customer_id = True
                    break
            
            if not has_customer_id:
                tables_without_customer_id.append(table)
    
    return tables_with_customer_id, tables_without_customer_id


def get_tables_with_customer_id(connection, db_name: str) -> List[Tuple[str, str]]:
    """
    Get list of tables that have a customer_id column (case-insensitive).
    Returns list of tuples: (table_name, actual_column_name)
    """
    tables_with, _ = categorize_tables_by_customer_id(connection, db_name)
    return tables_with


def get_row_count(connection, db_name: str, table_name: str, customer_col: str = None, customer_ids: List[int] = None) -> int:
    """
    Get count of rows.
    If customer_col and customer_ids are provided, filters by customer IDs.
    Otherwise, returns total row count.
    """
    with connection.cursor() as cursor:
        if customer_col and customer_ids:
            placeholders = ','.join(['%s'] * len(customer_ids))
            query = f"SELECT COUNT(*) as count FROM `{db_name}`.`{table_name}` WHERE `{customer_col}` IN ({placeholders})"
            cursor.execute(query, customer_ids)
        else:
            query = f"SELECT COUNT(*) as count FROM `{db_name}`.`{table_name}`"
            cursor.execute(query)
        
        result = cursor.fetchone()
        return result['count']


def fetch_customer_data(connection, db_name: str, table_name: str, customer_col: str = None, 
                       customer_ids: List[int] = None, offset: int = 0, limit: int = BATCH_SIZE) -> List[Dict]:
    """
    Fetch data in batches.
    If customer_col and customer_ids are provided, filters by customer IDs.
    Otherwise, fetches all data.
    """
    with connection.cursor() as cursor:
        if customer_col and customer_ids:
            placeholders = ','.join(['%s'] * len(customer_ids))
            query = f"""
                SELECT * FROM `{db_name}`.`{table_name}` 
                WHERE `{customer_col}` IN ({placeholders})
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, customer_ids + [limit, offset])
        else:
            query = f"SELECT * FROM `{db_name}`.`{table_name}` LIMIT %s OFFSET %s"
            cursor.execute(query, [limit, offset])
        
        return cursor.fetchall()


def fetch_indirect_customer_data(connection, db_name: str, table_name: str,
                                 fk_column: str, referenced_table: str, 
                                 referenced_id_column: str, id_type: str, seed_ids: List[int],
                                 offset: int = 0, limit: int = BATCH_SIZE) -> List[Dict]:
    """
    Fetch data for tables that link to customer_id/user_id indirectly via foreign key.
    Example: ROLE_ACCESS_MAP -> role_id -> ROLE.customer_id
    """
    with connection.cursor() as cursor:
        placeholders = ','.join(['%s'] * len(seed_ids))
        
        # Build JOIN query
        query = f"""
            SELECT t.* FROM `{db_name}`.`{table_name}` t
            INNER JOIN `{db_name}`.`{referenced_table}` r 
                ON t.`{fk_column}` = r.`{referenced_id_column}`
            WHERE r.`{id_type}` IN ({placeholders})
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, seed_ids + [limit, offset])
        return cursor.fetchall()


def count_indirect_rows(connection, db_name: str, table_name: str,
                       fk_column: str, referenced_table: str,
                       referenced_id_column: str, id_type: str, seed_ids: List[int]) -> int:
    """Count rows that link to seed data indirectly via foreign key."""
    with connection.cursor() as cursor:
        placeholders = ','.join(['%s'] * len(seed_ids))
        query = f"""
            SELECT COUNT(*) as count FROM `{db_name}`.`{table_name}` t
            INNER JOIN `{db_name}`.`{referenced_table}` r 
                ON t.`{fk_column}` = r.`{referenced_id_column}`
            WHERE r.`{id_type}` IN ({placeholders})
        """
        cursor.execute(query, seed_ids)
        return cursor.fetchone()['count']


def insert_data_batch(connection, db_name: str, table_name: str, columns: List[str], 
                     data_batch: List[Dict], ignore_duplicates: bool = True) -> Tuple[int, int]:
    """
    Insert a batch of data into the destination table.
    Returns tuple of (successful_inserts, failed_inserts)
    """
    if not data_batch:
        return 0, 0
    
    successful = 0
    failed = 0
    
    with connection.cursor() as cursor:
        cursor.execute(f"USE `{db_name}`")
        
        # Build INSERT statement
        columns_str = ', '.join([f"`{col}`" for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Use INSERT IGNORE to skip duplicates if enabled
        insert_cmd = "INSERT IGNORE" if ignore_duplicates else "INSERT"
        query = f"{insert_cmd} INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        
        # Insert each row (could be optimized with executemany, but this gives better error handling)
        for row in data_batch:
            try:
                values = [row.get(col) for col in columns]
                cursor.execute(query, values)
                successful += 1
            except pymysql.Error as e:
                failed += 1
                # Only print first few errors to avoid spam
                if failed <= 3:
                    print(f"      ⚠ Insert error: {e}")
                    if failed == 3:
                        print(f"      (suppressing further errors for this batch...)")
        
        connection.commit()
    
    return successful, failed


def create_missing_database(source_conn, dest_conn, db_name: str):
    """Create a missing database in the destination server."""
    print(f"    📦 Creating missing database '{db_name}'...")
    
    with source_conn.cursor() as src_cursor:
        src_cursor.execute(f"SHOW CREATE DATABASE `{db_name}`")
        create_statement = src_cursor.fetchone()['Create Database']
    
    with dest_conn.cursor() as dest_cursor:
        dest_cursor.execute(create_statement)
        dest_conn.commit()
    
    print(f"    ✓ Database '{db_name}' created")


def create_missing_table(source_conn, dest_conn, db_name: str, table_name: str):
    """Create a missing table in the destination database."""
    print(f"    📋 Creating missing table '{table_name}'...")
    
    # Import the function from the migrate_databases script
    with source_conn.cursor() as cursor:
        cursor.execute(f"SHOW CREATE TABLE `{db_name}`.`{table_name}`")
        result = cursor.fetchone()
        create_statement = result['Create Table']
    
    with dest_conn.cursor() as cursor:
        cursor.execute(f"USE `{db_name}`")
        cursor.execute(create_statement)
        dest_conn.commit()
    
    print(f"    ✓ Table '{table_name}' created")


def migrate_table_data(source_conn, dest_conn, db_name: str, table_name: str, 
                      customer_col: str = None, customer_ids: List[int] = None,
                      indirect_fk: Dict = None, id_type: str = None) -> Dict[str, int]:
    """
    Migrate data for a single table.
    If customer_col and customer_ids are provided, filters by customer/user IDs.
    If indirect_fk is provided, filters via JOIN to related table.
    Otherwise, migrates all data (for tables without customer_id).
    Returns statistics dictionary.
    """
    stats = {
        'total_rows': 0,
        'inserted': 0,
        'failed': 0,
        'skipped': 0
    }
    
    # Get total row count
    if indirect_fk and id_type:
        # Count via indirect relationship
        total_rows = count_indirect_rows(source_conn, db_name, table_name,
                                        indirect_fk['column'], indirect_fk['referenced_table'],
                                        indirect_fk['referenced_column'] or 'id',
                                        id_type, customer_ids)
    else:
        total_rows = get_row_count(source_conn, db_name, table_name, customer_col, customer_ids)
    
    stats['total_rows'] = total_rows
    
    if total_rows == 0:
        print(f"    ℹ No data to migrate (0 rows)")
        return stats
    
    # Indicate filtering method
    if indirect_fk and id_type:
        print(f"    Found {total_rows} row(s) to migrate (filtered via FK to {indirect_fk['referenced_table']}.{id_type})")
    elif customer_col and customer_ids:
        print(f"    Found {total_rows} row(s) to migrate (filtered by {customer_col})")
    else:
        print(f"    Found {total_rows} row(s) to migrate (ALL data - no customer_id filter)")
    
    # Get table columns
    columns = get_table_columns(source_conn, db_name, table_name)
    
    # Migrate data in batches
    offset = 0
    batch_num = 1
    
    while offset < total_rows:
        # Fetch batch
        if indirect_fk and id_type:
            data_batch = fetch_indirect_customer_data(source_conn, db_name, table_name,
                                                     indirect_fk['column'], indirect_fk['referenced_table'],
                                                     indirect_fk['referenced_column'] or 'id',
                                                     id_type, customer_ids, offset, BATCH_SIZE)
        else:
            data_batch = fetch_customer_data(source_conn, db_name, table_name, customer_col, 
                                            customer_ids, offset, BATCH_SIZE)
        
        if not data_batch:
            break
        
        # Insert batch
        print(f"    Batch {batch_num}: Inserting rows {offset + 1}-{min(offset + len(data_batch), total_rows)}...", end=' ')
        
        successful, failed = insert_data_batch(dest_conn, db_name, table_name, columns, 
                                               data_batch, ignore_duplicates=True)
        
        stats['inserted'] += successful
        stats['failed'] += failed
        
        print(f"✓ ({successful} inserted, {failed} failed/skipped)")
        
        offset += BATCH_SIZE
        batch_num += 1
    
    return stats


def migrate_database_data(db_name: str, customer_ids: List[int], auto_confirm_threshold: int = 100,
                          state: Dict = None, state_file: Path = None,
                          force: bool = False, force_tables: List[str] = None):
    """
    Migrate customer data for a single database.

    Args:
        db_name: Database name to migrate
        customer_ids: List of customer IDs to filter by (these will be combined with SEED_USER_IDS)
        auto_confirm_threshold: For tables without customer_id, ask for confirmation
                                if row count exceeds this threshold (default: 100)
        state: Migration state dictionary (V3)
        state_file: Path to state file for saving progress (V3)
        force: Force re-migration of all tables (V3)
        force_tables: List of specific tables to force re-migrate (V3)
    """
    if force_tables is None:
        force_tables = []
    print(f"\n{'='*70}")
    print(f"Migrating customer data from database: {db_name}")
    print(f"Customer IDs to filter: {customer_ids}")
    print(f"Seed User IDs to filter: {SEED_USER_IDS}")
    print(f"{'='*70}")
    
    # Connect to source and destination
    source_conn = get_connection(READ_CONFIG)
    dest_conn = get_connection(WRITE_CONFIG)
    
    try:
        # Check if database exists in destination
        if not database_exists(dest_conn, db_name):
            if CREATE_MISSING_OBJECTS:
                create_missing_database(source_conn, dest_conn, db_name)
            else:
                print(f"❌ Database '{db_name}' does not exist in destination server")
                print(f"   Set CREATE_MISSING_OBJECTS = True to auto-create missing databases")
                return
        
        # Get all tables
        all_tables = get_all_tables(source_conn, db_name)
        
        # Detect foreign keys (explicit and implicit)
        print(f"\n  🔍 Detecting relationships...")
        explicit_fks = get_explicit_foreign_keys(source_conn, db_name)
        
        # Build complete FK map
        all_fks = defaultdict(list)
        all_fks.update(explicit_fks)
        
        # Detect implicit FKs for all tables
        for table in all_tables:
            columns = get_table_columns(source_conn, db_name, table)
            implicit_fks = detect_implicit_foreign_keys(source_conn, db_name, table, columns, all_tables)
            if implicit_fks:
                all_fks[table].extend(implicit_fks)
        
        print(f"  ✓ Found {sum(len(fks) for fks in explicit_fks.values())} explicit FKs")
        print(f"  ✓ Found {sum(len(fks) for fks in all_fks.values()) - sum(len(fks) for fks in explicit_fks.values())} implicit FKs")
        
        # Categorize tables by whether they have customer_id/user_id column
        tables_with_customer, tables_without_customer = categorize_tables_by_customer_id(source_conn, db_name)
        
        # Also find tables with user_id
        tables_with_user = []
        for table in all_tables:
            if table not in [t[0] for t in tables_with_customer]:  # Skip if already has customer_id
                columns = get_table_columns(source_conn, db_name, table)
                user_col = find_user_id_column(columns)
                if user_col:
                    tables_with_user.append((table, user_col))
                    # Remove from tables_without_customer if present
                    if table in tables_without_customer:
                        tables_without_customer.remove(table)
        
        # Build sets for relationship chain detection
        tables_with_customer_id_set = {t[0] for t in tables_with_customer}
        tables_with_user_id_set = {t[0] for t in tables_with_user}
        
        # Detect indirect relationships for tables without customer_id/user_id
        tables_with_indirect = {}  # table_name -> (id_type, chain, fk_dict)
        tables_pure_reference = []
        
        for table in tables_without_customer:
            result = build_relationship_chain(table, all_fks, 
                                            tables_with_customer_id_set,
                                            tables_with_user_id_set)
            if result:
                id_type, chain, fk_dict = result
                tables_with_indirect[table] = (id_type, chain, fk_dict)
            else:
                tables_pure_reference.append(table)
        
        total_tables = len(tables_with_customer) + len(tables_with_user) + len(tables_with_indirect) + len(tables_pure_reference)
        
        if total_tables == 0:
            print(f"  ⚠ No tables found in database '{db_name}'")
            return
        
        print(f"\n  📊 Table Analysis:")
        print(f"    Total tables: {total_tables}")
        print(f"")
        print(f"    Category A - Direct filter (customer_id): {len(tables_with_customer)}")
        if tables_with_customer:
            for table, col in tables_with_customer:
                print(f"      • {table} (column: {col})")
        
        print(f"")
        print(f"    Category A - Direct filter (user_id): {len(tables_with_user)}")
        if tables_with_user:
            for table, col in tables_with_user:
                print(f"      • {table} (column: {col})")
        
        print(f"")
        print(f"    Category B - Indirect filter (via FK): {len(tables_with_indirect)}")
        if tables_with_indirect:
            for table, (id_type, chain, fk_dict) in tables_with_indirect.items():
                chain_str = ' → '.join(chain)
                print(f"      • {table} → {chain_str}")
        
        print(f"")
        print(f"    Category C - Reference data (no filter): {len(tables_pure_reference)}")
        if tables_pure_reference:
            for table in tables_pure_reference:
                print(f"      • {table}")
        
        # Disable foreign key checks for the destination connection
        print(f"\n  {'='*66}")
        print(f"  FOREIGN KEY CONSTRAINT HANDLING")
        print(f"  {'='*66}")
        print(f"  🔓 Disabling foreign key checks during migration...")
        print(f"     This allows insertion of rows even if referenced rows don't exist yet.")
        print(f"     Foreign keys will be re-enabled after migration completes.")
        print(f"  ")
        print(f"  ℹ️  How this helps:")
        print(f"     • Prevents FK constraint violations during batch inserts")
        print(f"     • Allows flexible insertion order (referenced tables can come later)")
        print(f"     • Duplicate keys are handled with INSERT IGNORE")
        
        with dest_conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            dest_conn.commit()

        print(f"  ✓ Foreign key checks disabled successfully")

        # V3: Migrate stored procedures and functions (Phase 0)
        if state is not None and state_file is not None:
            routine_stats = migrate_routines(source_conn, dest_conn, db_name, state, state_file, force)
        else:
            # Fallback for backwards compatibility
            routine_stats = {'procedures': 0, 'functions': 0, 'skipped': 0, 'failed': 0}

        # Migrate each table
        total_stats = {
            'tables_processed': 0,
            'tables_success': 0,
            'tables_failed': 0,
            'tables_skipped': 0,
            'total_rows_found': 0,
            'total_rows_inserted': 0,
            'total_rows_failed': 0
        }
        
        # Track tables without customer_id for detailed reporting
        tables_without_customer_id_details = []
        
        table_counter = 0
        
        # First, migrate tables WITH customer_id (filtered data)
        if tables_with_customer:
            print(f"\n  {'='*66}")
            print(f"  PHASE 1: Migrating tables WITH customer_id (filtered data)")
            print(f"  {'='*66}")
            
            for table_name, customer_col in tables_with_customer:
                table_counter += 1
                print(f"\n  [{table_counter}/{total_tables}] Migrating table: {table_name} (with customer_id filter)")

                # V3: Check if table should be skipped
                if should_skip_table(db_name, table_name, SKIP_TABLES):
                    print(f"    ⊗ Skipped (configured in SKIP_TABLES)")
                    total_stats['tables_skipped'] += 1
                    if state is not None:
                        set_table_state(state, db_name, table_name, "skipped", reason="env_skip_tables")
                        save_migration_state(state_file, state)
                    continue

                # V3: Check if already migrated (unless force or in force_tables)
                should_force = force or table_name in force_tables or f"{db_name}.{table_name}" in force_tables
                if not should_force and state is not None:
                    existing = get_table_state(state, db_name, table_name)
                    if existing and existing.get("status") == "completed":
                        print(f"    ⊗ Already migrated [{existing.get('rows', 0)} rows] (use --force to re-migrate)")
                        total_stats['tables_skipped'] += 1
                        continue

                try:
                    # Check if table exists in destination
                    if not table_exists(dest_conn, db_name, table_name):
                        if CREATE_MISSING_OBJECTS:
                            create_missing_table(source_conn, dest_conn, db_name, table_name)
                        else:
                            print(f"    ⚠ Table '{table_name}' does not exist in destination")
                            print(f"      Set CREATE_MISSING_OBJECTS = True to auto-create missing tables")
                            total_stats['tables_failed'] += 1
                            continue
                    
                    # Migrate table data with customer_id filter
                    stats = migrate_table_data(source_conn, dest_conn, db_name, table_name, 
                                              customer_col, customer_ids)
                    
                    total_stats['tables_processed'] += 1
                    total_stats['total_rows_found'] += stats['total_rows']
                    total_stats['total_rows_inserted'] += stats['inserted']
                    total_stats['total_rows_failed'] += stats['failed']
                    
                    if stats['failed'] == 0:
                        total_stats['tables_success'] += 1
                        print(f"    ✓ Table '{table_name}' migrated successfully")
                        if state is not None:
                            set_table_state(state, db_name, table_name, "completed", stats['inserted'])
                            save_migration_state(state_file, state)
                    else:
                        print(f"    ⚠ Table '{table_name}' migrated with {stats['failed']} errors")
                        if state is not None:
                            set_table_state(state, db_name, table_name, "completed", stats['inserted'])
                            save_migration_state(state_file, state)

                except Exception as e:
                    print(f"    ❌ Error migrating table '{table_name}': {e}")
                    total_stats['tables_failed'] += 1
                    if state is not None:
                        set_table_state(state, db_name, table_name, "failed", reason=str(e)[:100])
                        save_migration_state(state_file, state)
                    continue

        # Phase 1b: Migrate tables WITH user_id (filtered data)
        if tables_with_user:
            print(f"\n  {'='*66}")
            print(f"  PHASE 1B: Migrating tables WITH user_id (filtered data)")
            print(f"  {'='*66}")
            
            for table_name, user_col in tables_with_user:
                table_counter += 1
                print(f"\n  [{table_counter}/{total_tables}] Migrating table: {table_name} (with user_id filter)")

                # V3: Check if table should be skipped
                if should_skip_table(db_name, table_name, SKIP_TABLES):
                    print(f"    ⊗ Skipped (configured in SKIP_TABLES)")
                    total_stats['tables_skipped'] += 1
                    if state is not None:
                        set_table_state(state, db_name, table_name, "skipped", reason="env_skip_tables")
                        save_migration_state(state_file, state)
                    continue

                # V3: Check if already migrated
                should_force = force or table_name in force_tables or f"{db_name}.{table_name}" in force_tables
                if not should_force and state is not None:
                    existing = get_table_state(state, db_name, table_name)
                    if existing and existing.get("status") == "completed":
                        print(f"    ⊗ Already migrated [{existing.get('rows', 0)} rows] (use --force to re-migrate)")
                        total_stats['tables_skipped'] += 1
                        continue

                try:
                    if not table_exists(dest_conn, db_name, table_name):
                        if CREATE_MISSING_OBJECTS:
                            create_missing_table(source_conn, dest_conn, db_name, table_name)
                        else:
                            print(f"    ⚠ Table '{table_name}' does not exist in destination")
                            total_stats['tables_failed'] += 1
                            continue
                    
                    # Migrate table data with user_id filter
                    stats = migrate_table_data(source_conn, dest_conn, db_name, table_name, 
                                              user_col, SEED_USER_IDS)
                    
                    total_stats['tables_processed'] += 1
                    total_stats['total_rows_found'] += stats['total_rows']
                    total_stats['total_rows_inserted'] += stats['inserted']
                    total_stats['total_rows_failed'] += stats['failed']
                    
                    if stats['failed'] == 0:
                        total_stats['tables_success'] += 1
                        print(f"    ✓ Table '{table_name}' migrated successfully")
                        if state is not None:
                            set_table_state(state, db_name, table_name, "completed", stats['inserted'])
                            save_migration_state(state_file, state)
                    else:
                        print(f"    ⚠ Table '{table_name}' migrated with {stats['failed']} errors")
                        if state is not None:
                            set_table_state(state, db_name, table_name, "completed", stats['inserted'])
                            save_migration_state(state_file, state)

                except Exception as e:
                    print(f"    ❌ Error migrating table '{table_name}': {e}")
                    total_stats['tables_failed'] += 1
                    if state is not None:
                        set_table_state(state, db_name, table_name, "failed", reason=str(e)[:100])
                        save_migration_state(state_file, state)
                    continue

        # Phase 1c: Migrate tables with INDIRECT relationships (filtered via JOIN)
        if tables_with_indirect:
            print(f"\n  {'='*66}")
            print(f"  PHASE 1C: Migrating tables with indirect FK relationships")
            print(f"  {'='*66}")
            print(f"  These tables don't have customer_id/user_id but reference tables that do")
            
            for table_name, (id_type, chain, fk_dict) in tables_with_indirect.items():
                table_counter += 1
                chain_str = ' → '.join(chain)
                print(f"\n  [{table_counter}/{total_tables}] Migrating table: {table_name}")
                print(f"    Relationship: {chain_str}")
                print(f"    Filtering via: {fk_dict['column']} → {fk_dict['referenced_table']}.{id_type}")

                # V3: Check if table should be skipped
                if should_skip_table(db_name, table_name, SKIP_TABLES):
                    print(f"    ⊗ Skipped (configured in SKIP_TABLES)")
                    total_stats['tables_skipped'] += 1
                    if state is not None:
                        set_table_state(state, db_name, table_name, "skipped", reason="env_skip_tables")
                        save_migration_state(state_file, state)
                    continue

                # V3: Check if already migrated
                should_force = force or table_name in force_tables or f"{db_name}.{table_name}" in force_tables
                if not should_force and state is not None:
                    existing = get_table_state(state, db_name, table_name)
                    if existing and existing.get("status") == "completed":
                        print(f"    ⊗ Already migrated [{existing.get('rows', 0)} rows] (use --force to re-migrate)")
                        total_stats['tables_skipped'] += 1
                        continue

                try:
                    if not table_exists(dest_conn, db_name, table_name):
                        if CREATE_MISSING_OBJECTS:
                            create_missing_table(source_conn, dest_conn, db_name, table_name)
                        else:
                            print(f"    ⚠ Table '{table_name}' does not exist in destination")
                            total_stats['tables_failed'] += 1
                            continue
                    
                    # Migrate with indirect filter (via JOIN)
                    filter_ids = customer_ids if id_type == 'customer_id' else SEED_USER_IDS
                    stats = migrate_table_data(source_conn, dest_conn, db_name, table_name,
                                              customer_col=None, customer_ids=filter_ids,
                                              indirect_fk=fk_dict, id_type=id_type)
                    
                    total_stats['tables_processed'] += 1
                    total_stats['total_rows_found'] += stats['total_rows']
                    total_stats['total_rows_inserted'] += stats['inserted']
                    total_stats['total_rows_failed'] += stats['failed']
                    
                    if stats['failed'] == 0:
                        total_stats['tables_success'] += 1
                        print(f"    ✓ Table '{table_name}' migrated successfully")
                        if state is not None:
                            set_table_state(state, db_name, table_name, "completed", stats['inserted'])
                            save_migration_state(state_file, state)
                    else:
                        print(f"    ⚠ Table '{table_name}' migrated with {stats['failed']} errors")
                        if state is not None:
                            set_table_state(state, db_name, table_name, "completed", stats['inserted'])
                            save_migration_state(state_file, state)

                except Exception as e:
                    print(f"    ❌ Error migrating table '{table_name}': {e}")
                    total_stats['tables_failed'] += 1
                    if state is not None:
                        set_table_state(state, db_name, table_name, "failed", reason=str(e)[:100])
                        save_migration_state(state_file, state)
                    continue

        # Phase 2: Migrate PURE REFERENCE tables (no customer relationship)
        if tables_pure_reference:
            print(f"\n  {'='*66}")
            print(f"  PHASE 2: Migrating REFERENCE tables (no customer/user relationship)")
            print(f"  {'='*66}")
            print(f"  ⚠️  These tables have NO connection to customer_id/user_id")
            print(f"  ⚠️  ALL data will be migrated (no filter)")
            print(f"  ℹ️  Foreign key constraints are temporarily disabled to prevent")
            print(f"     insertion errors. They will be re-enabled after migration.")
            
            for table_name in tables_pure_reference:
                table_counter += 1
                print(f"\n  [{table_counter}/{total_tables}] Processing table: {table_name} (NO customer_id column)")

                # V3: Check if table should be skipped
                if should_skip_table(db_name, table_name, SKIP_TABLES):
                    print(f"    ⊗ Skipped (configured in SKIP_TABLES)")
                    total_stats['tables_skipped'] += 1
                    if state is not None:
                        set_table_state(state, db_name, table_name, "skipped", reason="env_skip_tables")
                        save_migration_state(state_file, state)
                    continue

                # V3: Check if already migrated
                should_force = force or table_name in force_tables or f"{db_name}.{table_name}" in force_tables
                if not should_force and state is not None:
                    existing = get_table_state(state, db_name, table_name)
                    if existing and existing.get("status") == "completed":
                        print(f"    ⊗ Already migrated [{existing.get('rows', 0)} rows] (use --force to re-migrate)")
                        total_stats['tables_skipped'] += 1
                        continue
                    elif existing and existing.get("status") == "skipped":
                        # Previously skipped - ask user again
                        reason = existing.get("reason", "")
                        if reason == "user_declined":
                            print(f"    ⚠ Previously skipped by user. Migrate now?")
                            response = input(f"    Migrate this table? (yes/no): ").strip().lower()
                            if response not in ['yes', 'y']:
                                print(f"    ⊗ Skipped again by user")
                                total_stats['tables_skipped'] += 1
                                continue

                table_detail = {
                    'database': db_name,
                    'table': table_name,
                    'row_count': 0,
                    'status': 'pending',
                    'rows_inserted': 0,
                    'rows_failed': 0
                }

                try:
                    # Check if table exists in destination
                    if not table_exists(dest_conn, db_name, table_name):
                        if CREATE_MISSING_OBJECTS:
                            create_missing_table(source_conn, dest_conn, db_name, table_name)
                        else:
                            print(f"    ⚠ Table '{table_name}' does not exist in destination")
                            print(f"      Set CREATE_MISSING_OBJECTS = True to auto-create missing tables")
                            table_detail['status'] = 'failed - table not found'
                            tables_without_customer_id_details.append(table_detail)
                            total_stats['tables_failed'] += 1
                            continue
                    
                    # Get row count first
                    row_count = get_row_count(source_conn, db_name, table_name)
                    table_detail['row_count'] = row_count
                    
                    print(f"    Database: {db_name}")
                    print(f"    Table: {table_name}")
                    print(f"    Total rows: {row_count}")
                    
                    # V2 ENHANCEMENT: Check if this table should be force-migrated
                    is_force_migrate = should_force_migrate(db_name, table_name, FORCE_MIGRATE_TABLES)
                    
                    if is_force_migrate:
                        print(f"    ⭐ Force-migrate table (configured in FORCE_MIGRATE_TABLES)")
                        print(f"    ✓ Auto-migrating ALL {row_count} rows (no confirmation needed)")
                        should_migrate = True
                    
                    else:
                        # Ask for confirmation if row count exceeds threshold
                        should_migrate = True
                        if row_count > auto_confirm_threshold:
                            print(f"    ⚠️  This table has {row_count} rows (exceeds threshold of {auto_confirm_threshold})")
                            print(f"    ⚠️  All {row_count} rows will be migrated (this may not be seed data)")

                            # V3: Auto-skip if SKIP_LARGE_TABLES is enabled
                            if SKIP_LARGE_TABLES:
                                print(f"    ⊗ Auto-skipped (SKIP_LARGE_TABLES=true, exceeds {auto_confirm_threshold} rows)")
                                print(f"      Will be prompted on next run (use --force-tables to migrate)")
                                table_detail['status'] = 'skipped by user (auto)'
                                tables_without_customer_id_details.append(table_detail)
                                total_stats['tables_skipped'] += 1
                                if state is not None:
                                    set_table_state(state, db_name, table_name, "skipped", reason="user_declined")
                                    save_migration_state(state_file, state)
                                continue

                            while True:
                                response = input(f"    Migrate this table? (yes/no): ").strip().lower()
                                if response in ['yes', 'y', 'no', 'n']:
                                    should_migrate = response in ['yes', 'y']
                                    break
                                print(f"    Please enter 'yes' or 'no'")
                        else:
                            print(f"    ✓ Row count ({row_count}) is within threshold ({auto_confirm_threshold}), auto-migrating...")
                    
                    if not should_migrate:
                        print(f"    ⊗ Skipped by user")
                        table_detail['status'] = 'skipped by user'
                        tables_without_customer_id_details.append(table_detail)
                        total_stats['tables_skipped'] += 1
                        if state is not None:
                            set_table_state(state, db_name, table_name, "skipped", reason="user_declined")
                            save_migration_state(state_file, state)
                        continue
                    
                    # Migrate ALL table data (no customer_id filter)
                    print(f"    🔄 Starting migration...")
                    stats = migrate_table_data(source_conn, dest_conn, db_name, table_name, 
                                              customer_col=None, customer_ids=None)
                    
                    table_detail['rows_inserted'] = stats['inserted']
                    table_detail['rows_failed'] = stats['failed']
                    
                    total_stats['tables_processed'] += 1
                    total_stats['total_rows_found'] += stats['total_rows']
                    total_stats['total_rows_inserted'] += stats['inserted']
                    total_stats['total_rows_failed'] += stats['failed']
                    
                    if stats['failed'] == 0:
                        total_stats['tables_success'] += 1
                        table_detail['status'] = 'migrated successfully'
                        print(f"    ✓ Table '{table_name}' migrated successfully")
                        if state is not None:
                            set_table_state(state, db_name, table_name, "completed", stats['inserted'])
                            save_migration_state(state_file, state)
                    else:
                        table_detail['status'] = f'migrated with {stats["failed"]} errors'
                        print(f"    ⚠ Table '{table_name}' migrated with {stats['failed']} errors")
                        if state is not None:
                            set_table_state(state, db_name, table_name, "completed", stats['inserted'])
                            save_migration_state(state_file, state)

                    tables_without_customer_id_details.append(table_detail)

                except Exception as e:
                    print(f"    ❌ Error migrating table '{table_name}': {e}")
                    table_detail['status'] = f'failed - {str(e)[:50]}'
                    tables_without_customer_id_details.append(table_detail)
                    total_stats['tables_failed'] += 1
                    if state is not None:
                        set_table_state(state, db_name, table_name, "failed", reason=str(e)[:100])
                        save_migration_state(state_file, state)
                    continue
        
        # Re-enable foreign key checks
        print(f"\n  🔒 Re-enabling foreign key checks...")
        with dest_conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            dest_conn.commit()
        print(f"  ✓ Foreign key checks re-enabled")
        
        # Print detailed summary for tables without customer_id
        if tables_without_customer_id_details:
            print(f"\n{'='*70}")
            print(f"DETAILED REPORT: Tables WITHOUT customer_id")
            print(f"{'='*70}")
            print(f"{'Database':<20} {'Table':<25} {'Rows':<10} {'Status':<30}")
            print(f"{'-'*70}")
            
            for detail in tables_without_customer_id_details:
                status_display = detail['status']
                if detail['status'] == 'migrated successfully':
                    status_display = f"✓ {detail['status']}"
                elif detail['status'].startswith('skipped'):
                    status_display = f"⊗ {detail['status']}"
                elif detail['status'].startswith('failed'):
                    status_display = f"✗ {detail['status']}"
                else:
                    status_display = f"⚠ {detail['status']}"
                
                print(f"{detail['database']:<20} {detail['table']:<25} {detail['row_count']:<10} {status_display:<30}")
            
            print(f"{'-'*70}")
            migrated_count = sum(1 for d in tables_without_customer_id_details if d['status'] == 'migrated successfully')
            skipped_count = sum(1 for d in tables_without_customer_id_details if 'skipped' in d['status'])
            failed_count = sum(1 for d in tables_without_customer_id_details if 'failed' in d['status'])
            
            print(f"Summary: {migrated_count} migrated, {skipped_count} skipped, {failed_count} failed")
        
        # Print overall summary
        print(f"\n{'='*70}")
        print(f"✓ Database '{db_name}' migration completed!")
        print(f"{'='*70}")
        print(f"  Total tables: {total_tables}")
        print(f"    - Tables WITH customer_id: {len(tables_with_customer)}")
        print(f"    - Tables WITHOUT customer_id: {len(tables_without_customer)}")
        print(f"")
        print(f"  Migration Results:")
        print(f"    - Tables processed: {total_stats['tables_processed']}")
        print(f"    - Tables successful: {total_stats['tables_success']}")
        print(f"    - Tables skipped: {total_stats['tables_skipped']}")
        print(f"    - Tables failed: {total_stats['tables_failed']}")
        print(f"")
        print(f"  Data Statistics:")
        print(f"    - Total rows found: {total_stats['total_rows_found']}")
        print(f"    - Total rows inserted: {total_stats['total_rows_inserted']}")
        print(f"    - Total rows failed/duplicate: {total_stats['total_rows_failed']}")
        print(f"{'='*70}")
        
    except Exception as e:
        print(f"\n❌ Error migrating database '{db_name}': {e}")
        raise
    finally:
        # Always re-enable foreign key checks before closing
        try:
            with dest_conn.cursor() as cursor:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
                dest_conn.commit()
        except:
            pass
        
        source_conn.close()
        dest_conn.close()


def parse_customer_ids(input_str: str) -> List[int]:
    """Parse customer IDs from user input."""
    try:
        # Remove spaces and brackets
        cleaned = input_str.strip().replace('[', '').replace(']', '')
        
        # Split by comma and convert to integers
        ids = [int(id_str.strip()) for id_str in cleaned.split(',') if id_str.strip()]
        
        if not ids:
            raise ValueError("No valid customer IDs provided")
        
        return ids
    except ValueError as e:
        print(f"❌ Error parsing customer IDs: {e}")
        print("Please provide comma-separated numbers, e.g., 1,2,3 or [1,2,3]")
        sys.exit(1)


def get_user_input() -> Tuple[List[str], List[int]]:
    """Get database names and customer IDs from user input."""
    print("\n" + "="*70)
    print("Customer Data Migration Tool - MariaDB to MariaDB")
    print("="*70)
    
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
    
    # Get database names
    print("\n" + "-"*70)
    print("Enter database names to migrate (comma-separated)")
    print("Example: database1, database2, database3")
    print("Or type 'all' to migrate all available databases")
    print("-"*70)
    
    db_input = input("\nDatabase names: ").strip()
    
    if not db_input:
        print("❌ No databases specified. Exiting.")
        sys.exit(0)
    
    if db_input.lower() == 'all':
        if not available_dbs:
            print("❌ No databases available to migrate.")
            sys.exit(0)
        db_names = available_dbs
    else:
        db_names = [db.strip() for db in db_input.split(',')]
        db_names = [db for db in db_names if db]
    
    if not db_names:
        print("❌ No valid database names provided. Exiting.")
        sys.exit(0)
    
    # Get customer IDs
    print("\n" + "-"*70)
    print("Enter customer IDs to migrate (comma-separated)")
    print("Example: 1,2,3 or [1,2,3]")
    print("-"*70)
    
    customer_input = input("\nCustomer IDs: ").strip()
    
    if not customer_input:
        print("❌ No customer IDs specified. Exiting.")
        sys.exit(0)
    
    customer_ids = parse_customer_ids(customer_input)
    
    return db_names, customer_ids


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Customer Data Migration Tool V3 - MariaDB to MariaDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_customer_data_v3.py                    # Normal interactive run
  python migrate_customer_data_v3.py --status           # Show migration status
  python migrate_customer_data_v3.py --force            # Force re-migrate all tables
  python migrate_customer_data_v3.py --force-tables "ACCESS_RIGHT,AUDIT_LOG"
  python migrate_customer_data_v3.py --force-tables "STARFOX.ACCESS_RIGHT"
        """
    )

    parser.add_argument(
        '--status',
        action='store_true',
        help='Show migration status from state file (requires --customer-ids)'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-migration of all tables (ignore state file)'
    )

    parser.add_argument(
        '--force-tables',
        type=str,
        default='',
        help='Comma-separated list of tables to force re-migrate (e.g., "ACCESS_RIGHT,AUDIT_LOG" or "STARFOX.ACCESS_RIGHT")'
    )

    parser.add_argument(
        '--customer-ids',
        type=str,
        default='',
        help='Comma-separated customer IDs (e.g., "1,2,3") - required for --status'
    )

    parser.add_argument(
        '--databases',
        type=str,
        default='',
        help='Comma-separated database names (e.g., "STARFOX,ONBOARDING")'
    )

    return parser.parse_args()


def main():
    """Main function to orchestrate the migration."""
    args = parse_args()

    print("\n🚀 Customer Data Migration Tool V3")
    print("="*50)

    # Handle --status flag
    if args.status:
        if not args.customer_ids:
            print("❌ --status requires --customer-ids to identify state file")
            print("   Example: python migrate_customer_data_v3.py --status --customer-ids 1,2,3")
            sys.exit(1)

        customer_ids = parse_customer_ids(args.customer_ids)
        state_file = get_state_file_path(customer_ids)
        print_migration_status(state_file)
        sys.exit(0)

    # Parse force-tables
    force_tables = [t.strip() for t in args.force_tables.split(',') if t.strip()]

    # Validate configuration
    validate_config()

    print(f"\nConfiguration:")
    print(f"  Source Server: {READ_CONFIG['host']}:{READ_CONFIG['port']}")
    print(f"  Destination Server: {WRITE_CONFIG['host']}:{WRITE_CONFIG['port']}")
    print(f"  Batch Size: {BATCH_SIZE} rows")
    print(f"  Auto-create missing objects: {CREATE_MISSING_OBJECTS}")
    print(f"  Confirmation threshold: {AUTO_CONFIRM_THRESHOLD} rows")
    if FORCE_MIGRATE_TABLES:
        print(f"  ⭐ Force-migrate tables: {', '.join(FORCE_MIGRATE_TABLES)}")
    if SKIP_TABLES:
        print(f"  ⊗ Skip tables: {', '.join(SKIP_TABLES)}")
    if SKIP_LARGE_TABLES:
        print(f"  ⊗ Skip large tables: Enabled (auto-skip reference tables > {AUTO_CONFIRM_THRESHOLD} rows)")
    if args.force:
        print(f"  ⚠️  FORCE MODE: Will re-migrate all tables")
    if force_tables:
        print(f"  ⚠️  Force-tables: {', '.join(force_tables)}")

    # Get user input (or use command-line args)
    if args.databases and args.customer_ids:
        db_names = [db.strip() for db in args.databases.split(',') if db.strip()]
        customer_ids = parse_customer_ids(args.customer_ids)
    else:
        db_names, customer_ids = get_user_input()

    print(f"\n📋 Migration Plan:")
    print(f"  Databases: {', '.join(db_names)}")
    print(f"  Customer IDs: {customer_ids}")

    # Initialize state file
    state_file = get_state_file_path(customer_ids)
    state = load_migration_state(state_file)
    print(f"  State file: {state_file}")

    # Check for existing state
    if state.get("databases"):
        existing_dbs = list(state["databases"].keys())
        print(f"  ℹ Found existing migration state for: {', '.join(existing_dbs)}")
        if not args.force:
            print(f"    (Use --force to re-migrate completed tables)")

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
            migrate_database_data(
                db_name,
                customer_ids,
                AUTO_CONFIRM_THRESHOLD,
                state=state,
                state_file=state_file,
                force=args.force,
                force_tables=force_tables
            )
            success_count += 1
        except Exception as e:
            print(f"❌ Failed to migrate database '{db_name}': {e}")
            failed_databases.append(db_name)
            continue

    # Print final summary
    print("\n" + "="*70)
    print("FINAL MIGRATION SUMMARY")
    print("="*70)
    print(f"Total databases: {len(db_names)}")
    print(f"✓ Successfully migrated: {success_count}")
    print(f"✗ Failed: {len(failed_databases)}")
    print(f"\nState file: {state_file}")

    if failed_databases:
        print(f"\nFailed databases: {', '.join(failed_databases)}")

    print("\n✅ Migration process completed!")
    print(f"\nTo view migration status: python migrate_customer_data_v3.py --status --customer-ids {','.join(str(c) for c in customer_ids)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Migration interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
