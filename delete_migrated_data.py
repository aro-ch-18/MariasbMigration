#!/usr/bin/env python3
"""
Database Cleanup Script - Drop Migrated Databases from Target

Purpose:
  Drop databases from TARGET that exist in SOURCE.
  Used for cleanup, testing, or reverting migrations.

Safety Features:
  - Multi-step confirmation (must type 'DROP DATABASES')
  - Dry-run mode (--dry-run)
  - Optional backup before dropping (--backup)
  - Shows exactly what will be dropped before executing

Usage:
  # Preview what would be dropped (safe)
  python delete_migrated_data.py --dry-run

  # Drop with backup (recommended)
  python delete_migrated_data.py --backup

  # Drop specific databases only
  python delete_migrated_data.py --databases STARFOX,ONBOARDING

  # Drop all matching databases
  python delete_migrated_data.py --all
"""

import pymysql
import os
from dotenv import load_dotenv
import sys
from typing import List, Dict
import argparse
from datetime import datetime
import subprocess

# Load environment variables
load_dotenv()

# Source database configuration (READ - to know which DBs to drop)
READ_CONFIG = {
    'host': os.getenv('READ_DB_HOST'),
    'port': int(os.getenv('READ_DB_PORT', 3306)),
    'user': os.getenv('READ_DB_USER'),
    'password': os.getenv('READ_DB_PASSWORD'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# Target database configuration (WRITE - where DBs will be DROPPED!)
WRITE_CONFIG = {
    'host': os.getenv('WRITE_DB_HOST'),
    'port': int(os.getenv('WRITE_DB_PORT', 3306)),
    'user': os.getenv('WRITE_DB_USER'),
    'password': os.getenv('WRITE_DB_PASSWORD'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

SYSTEM_DATABASES = ['information_schema', 'performance_schema', 'mysql', 'sys']


def validate_config():
    """Validate required environment variables."""
    required_vars = [
        'READ_DB_HOST', 'READ_DB_USER', 'READ_DB_PASSWORD',
        'WRITE_DB_HOST', 'WRITE_DB_USER', 'WRITE_DB_PASSWORD'
    ]

    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        print(f"Error: Missing required environment variables: {', '.join(missing)}")
        print("Please update your .env file with the required credentials.")
        sys.exit(1)


def get_databases(connection) -> List[str]:
    """Get all non-system databases."""
    with connection.cursor() as cursor:
        cursor.execute("SHOW DATABASES")
        databases = [row['Database'] for row in cursor.fetchall()
                    if row['Database'] not in SYSTEM_DATABASES]
    return databases


def get_database_info(connection, db_name: str) -> Dict:
    """Get table count and approximate size for a database."""
    info = {'tables': 0, 'size_mb': 0}
    try:
        with connection.cursor() as cursor:
            # Get table count
            cursor.execute(f"SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_schema = %s", (db_name,))
            result = cursor.fetchone()
            info['tables'] = result['cnt'] if result else 0

            # Get approximate size
            cursor.execute("""
                SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) as size_mb
                FROM information_schema.tables
                WHERE table_schema = %s
            """, (db_name,))
            result = cursor.fetchone()
            info['size_mb'] = result['size_mb'] if result and result['size_mb'] else 0
    except Exception as e:
        print(f"  Warning: Could not get info for {db_name}: {e}")
    return info


def analyze_drop_scope(source_conn, target_conn, databases: List[str]) -> Dict:
    """Analyze what databases exist in both source and target."""
    source_dbs = set(get_databases(source_conn))
    target_dbs = set(get_databases(target_conn))

    # Filter to requested databases
    if databases:
        requested = set(databases)
        # Only include databases that exist in both source and target
        to_drop = requested & source_dbs & target_dbs
        not_in_source = requested - source_dbs
        not_in_target = requested - target_dbs
    else:
        to_drop = source_dbs & target_dbs
        not_in_source = set()
        not_in_target = set()

    # Get info for each database to drop
    drop_info = []
    for db_name in sorted(to_drop):
        info = get_database_info(target_conn, db_name)
        drop_info.append({
            'name': db_name,
            'tables': info['tables'],
            'size_mb': info['size_mb']
        })

    return {
        'databases': drop_info,
        'total_databases': len(drop_info),
        'total_tables': sum(d['tables'] for d in drop_info),
        'total_size_mb': sum(d['size_mb'] for d in drop_info),
        'not_in_source': sorted(not_in_source),
        'not_in_target': sorted(not_in_target)
    }


def show_drop_plan(analysis: Dict):
    """Display what will be dropped."""
    print("\n" + "="*70)
    print("DROP PLAN REVIEW")
    print("="*70)
    print(f"\nTarget: {WRITE_CONFIG['host']}:{WRITE_CONFIG['port']}")
    print("\nThe following databases will be DROPPED:\n")

    print(f"{'Database':<30} {'Tables':<10} {'Size (MB)':<15}")
    print("-"*55)

    for db in analysis['databases']:
        print(f"{db['name']:<30} {db['tables']:<10} {db['size_mb']:<15.2f}")

    print("-"*55)
    print(f"{'TOTAL':<30} {analysis['total_tables']:<10} {analysis['total_size_mb']:<15.2f}")

    if analysis['not_in_source']:
        print(f"\nSkipped (not in source): {', '.join(analysis['not_in_source'])}")
    if analysis['not_in_target']:
        print(f"Skipped (not in target): {', '.join(analysis['not_in_target'])}")

    print("="*70)


def get_confirmation(analysis: Dict) -> bool:
    """Multi-step confirmation before dropping."""
    print("\n" + "!"*70)
    print("WARNING: This will PERMANENTLY DROP databases from the target server!")
    print("!"*70)

    # Show plan
    show_drop_plan(analysis)

    # Step 1: Confirm understanding
    print("\nStep 1/2: Do you understand this will DROP these databases?")
    response = input("Type 'yes' to continue: ").strip().lower()
    if response != 'yes':
        print("Cancelled.")
        return False

    # Step 2: Final confirmation
    print(f"\nStep 2/2: Type 'DROP DATABASES' to confirm dropping {analysis['total_databases']} database(s):")
    response = input("> ").strip()
    if response != 'DROP DATABASES':
        print("Cancelled. You must type exactly: DROP DATABASES")
        return False

    return True


def backup_databases(databases: List[str], backup_dir: str = 'backups') -> str:
    """Create backup before dropping."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = os.path.join(backup_dir, f'backup_{timestamp}')
    os.makedirs(backup_path, exist_ok=True)

    print(f"\nCreating backup in: {backup_path}")

    for db_name in databases:
        backup_file = os.path.join(backup_path, f'{db_name}.sql')
        print(f"  Backing up {db_name}...", end='', flush=True)

        cmd = [
            'mysqldump',
            '-h', WRITE_CONFIG['host'],
            '-P', str(WRITE_CONFIG['port']),
            '-u', WRITE_CONFIG['user'],
            f'-p{WRITE_CONFIG["password"]}',
            '--databases', db_name
        ]

        try:
            with open(backup_file, 'w') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE,
                                       text=True, timeout=600)

            if result.returncode == 0:
                size = os.path.getsize(backup_file)
                print(f" Done ({size / 1024 / 1024:.1f} MB)")
            else:
                print(f" Warning: {result.stderr[:100]}")
        except Exception as e:
            print(f" Error: {e}")

    print(f"Backup completed: {backup_path}\n")
    return backup_path


def drop_databases(target_conn, databases: List[Dict]):
    """Drop the databases."""
    print("\nDropping databases...")

    dropped = 0
    failed = []

    for db in databases:
        db_name = db['name']
        print(f"  DROP DATABASE `{db_name}`...", end='', flush=True)

        try:
            with target_conn.cursor() as cursor:
                cursor.execute(f"DROP DATABASE `{db_name}`")
                target_conn.commit()
            print(" Done")
            dropped += 1
        except Exception as e:
            print(f" Error: {e}")
            failed.append(db_name)

    print(f"\nCompleted: {dropped} dropped, {len(failed)} failed")
    if failed:
        print(f"Failed: {', '.join(failed)}")


def main():
    parser = argparse.ArgumentParser(
        description='Drop migrated databases from target server',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python delete_migrated_data.py --dry-run              # Preview only
  python delete_migrated_data.py --databases STARFOX    # Drop specific DB
  python delete_migrated_data.py --all --backup         # Drop all with backup
        '''
    )

    parser.add_argument('--dry-run', action='store_true',
                       help='Preview what would be dropped (no changes)')
    parser.add_argument('--backup', action='store_true',
                       help='Create backup before dropping')
    parser.add_argument('--databases', type=str,
                       help='Comma-separated list of databases to drop')
    parser.add_argument('--all', action='store_true',
                       help='Drop all databases that exist in source')
    parser.add_argument('--no-confirm', action='store_true',
                       help='Skip confirmation (DANGEROUS)')

    args = parser.parse_args()

    print("\n" + "="*70)
    print("DATABASE CLEANUP SCRIPT")
    print("="*70)
    print(f"Source: {READ_CONFIG['host']}:{READ_CONFIG['port']}")
    print(f"Target: {WRITE_CONFIG['host']}:{WRITE_CONFIG['port']} (databases will be DROPPED here)")
    print("="*70)

    validate_config()

    # Determine which databases to drop
    if args.databases:
        databases = [db.strip() for db in args.databases.split(',')]
    elif args.all:
        databases = []  # Empty means all matching
    else:
        # Interactive mode
        source_conn = pymysql.connect(**READ_CONFIG)
        try:
            available = get_databases(source_conn)
            print("\nDatabases in source:")
            for idx, db in enumerate(available, 1):
                print(f"  {idx}. {db}")

            print("\nEnter database names (comma-separated) or 'all':")
            user_input = input("> ").strip()

            if user_input.lower() == 'all':
                databases = []
            else:
                databases = [db.strip() for db in user_input.split(',')]
        finally:
            source_conn.close()

    # Connect and analyze
    source_conn = pymysql.connect(**READ_CONFIG)
    target_conn = pymysql.connect(**WRITE_CONFIG)

    try:
        print("\nAnalyzing...")
        analysis = analyze_drop_scope(source_conn, target_conn, databases)

        if analysis['total_databases'] == 0:
            print("\nNo databases to drop.")
            if databases:
                print(f"Requested databases not found in both source and target.")
            return

        # Dry run
        if args.dry_run:
            show_drop_plan(analysis)
            print("\nDRY-RUN: No databases were dropped.")
            print("Remove --dry-run to execute.")
            return

        # Get confirmation
        if not args.no_confirm:
            if not get_confirmation(analysis):
                return

        # Backup if requested
        if args.backup:
            db_names = [d['name'] for d in analysis['databases']]
            backup_databases(db_names)

        # Execute drop
        drop_databases(target_conn, analysis['databases'])

        print("\nCleanup completed!")

    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nCancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
