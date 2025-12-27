#!/usr/bin/env python3
"""
Delete Migrated Data Script - MariaDB Target Database Cleanup

⚠️  DANGER: This script DELETES data from the target database!

Purpose:
- Read table structure from SOURCE database
- DELETE matching data from TARGET database
- Use for cleanup, testing, or reverting migrations

Safety Features:
- 🔴 Multi-step confirmation (must type 'DELETE DATA')
- 🧪 Dry-run mode (--dry-run)
- 📦 Automatic backup before deletion (--backup)
- 📝 Deletion logging for audit trail
- 🎯 Filter by customer_id (prevents deleting all data)
- ⚠️  Shows deletion plan before executing

Usage Examples:
  # Preview what would be deleted (safe)
  python delete_migrated_data.py --dry-run

  # Delete with backup (recommended)
  python delete_migrated_data.py --backup

  # Delete without backup (dangerous!)
  python delete_migrated_data.py

  # Delete specific tables only
  python delete_migrated_data.py --tables users,orders

  # Delete from specific databases only
  python delete_migrated_data.py --databases STARFOX,AUTH_PROXY
"""

import pymysql
import os
from dotenv import load_dotenv
import sys
from typing import List, Dict, Any, Optional
import argparse
from datetime import datetime
import json
import subprocess

# Load environment variables
load_dotenv()

# Source database configuration (READ - for table discovery)
READ_CONFIG = {
    'host': os.getenv('READ_DB_HOST'),
    'port': int(os.getenv('READ_DB_PORT', 3306)),
    'user': os.getenv('READ_DB_USER'),
    'password': os.getenv('READ_DB_PASSWORD'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# Target database configuration (WRITE - where data will be DELETED!)
WRITE_CONFIG = {
    'host': os.getenv('WRITE_DB_HOST'),
    'port': int(os.getenv('WRITE_DB_PORT', 3306)),
    'user': os.getenv('WRITE_DB_USER'),
    'password': os.getenv('WRITE_DB_PASSWORD'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}


def validate_config():
    """Validate required environment variables."""
    required_vars = [
        'READ_DB_HOST', 'READ_DB_USER', 'READ_DB_PASSWORD',
        'WRITE_DB_HOST', 'WRITE_DB_USER', 'WRITE_DB_PASSWORD'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"❌ Error: Missing required environment variables: {', '.join(missing)}")
        print("Please update your .env file with the required credentials.")
        sys.exit(1)


def get_all_databases(connection) -> List[str]:
    """Get all databases from source (excluding system databases)."""
    with connection.cursor() as cursor:
        cursor.execute("SHOW DATABASES")
        databases = [row['Database'] for row in cursor.fetchall() 
                    if row['Database'] not in ['information_schema', 'performance_schema', 
                                               'mysql', 'sys']]
    return databases


def get_all_tables(connection, database: str) -> List[str]:
    """Get all tables from a database."""
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW TABLES FROM `{database}`")
        result = cursor.fetchall()
        if result:
            # Get the first column value (table name)
            tables = [list(row.values())[0] for row in result]
            return tables
        return []


def has_customer_id_column(connection, database: str, table: str) -> bool:
    """Check if table has customer_id column."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"DESCRIBE `{database}`.`{table}`")
            columns = [row['Field'].lower() for row in cursor.fetchall()]
            return 'customer_id' in columns
    except:
        return False


def count_rows(connection, database: str, table: str, customer_ids: Optional[List[int]] = None) -> int:
    """Count rows in table (optionally filtered by customer_id)."""
    try:
        with connection.cursor() as cursor:
            if customer_ids:
                placeholders = ','.join(['%s'] * len(customer_ids))
                query = f"SELECT COUNT(*) as cnt FROM `{database}`.`{table}` WHERE customer_id IN ({placeholders})"
                cursor.execute(query, customer_ids)
            else:
                query = f"SELECT COUNT(*) as cnt FROM `{database}`.`{table}`"
                cursor.execute(query)
            
            result = cursor.fetchone()
            return result['cnt'] if result else 0
    except Exception as e:
        print(f"    ⚠️  Could not count rows in {database}.{table}: {e}")
        return 0


def analyze_deletion_scope(source_conn, target_conn, databases: List[str], 
                           customer_ids: Optional[List[int]] = None,
                           specific_tables: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Analyze what would be deleted.
    
    Args:
        source_conn: Source database connection (for table discovery)
        target_conn: Target database connection (for row counting)
        databases: List of databases to analyze
        customer_ids: Optional customer IDs to filter
        specific_tables: Optional list of specific tables to delete
        
    Returns:
        Analysis dict with deletion plan
    """
    analysis = {
        'databases': [],
        'total_tables': 0,
        'total_rows': 0
    }
    
    for db_name in databases:
        db_analysis = {
            'name': db_name,
            'tables': []
        }
        
        try:
            # Get tables from source
            tables = get_all_tables(source_conn, db_name)
            
            # Filter by specific tables if provided
            if specific_tables:
                tables = [t for t in tables if t in specific_tables]
            
            for table in tables:
                has_customer_id = has_customer_id_column(target_conn, db_name, table)
                
                # Count rows in TARGET
                if has_customer_id and customer_ids:
                    row_count = count_rows(target_conn, db_name, table, customer_ids)
                    filter_used = f"customer_id IN ({','.join(map(str, customer_ids))})"
                else:
                    row_count = count_rows(target_conn, db_name, table)
                    filter_used = "ALL ROWS (no customer_id filter)"
                
                if row_count > 0:
                    table_info = {
                        'name': table,
                        'row_count': row_count,
                        'has_customer_id': has_customer_id,
                        'filter_used': filter_used
                    }
                    
                    db_analysis['tables'].append(table_info)
                    analysis['total_rows'] += row_count
            
            if db_analysis['tables']:
                db_analysis['table_count'] = len(db_analysis['tables'])
                analysis['databases'].append(db_analysis)
                analysis['total_tables'] += db_analysis['table_count']
        
        except Exception as e:
            print(f"⚠️  Error analyzing {db_name}: {e}")
    
    return analysis


def show_deletion_plan(analysis: Dict[str, Any]):
    """Display deletion plan for review."""
    print("\n" + "="*80)
    print("🔴 DELETION PLAN REVIEW")
    print("="*80)
    print("\n⚠️  WARNING: The following data will be PERMANENTLY DELETED from TARGET database!")
    print(f"   Target: {WRITE_CONFIG['host']}:{WRITE_CONFIG['port']}\n")
    
    for db in analysis['databases']:
        print(f"📁 Database: {db['name']} ({db['table_count']} tables)")
        
        for table in db['tables']:
            print(f"   • {table['name']:<30} {table['row_count']:>8,} rows")
            print(f"     Filter: {table['filter_used']}")
    
    print("\n" + "="*80)
    print("📊 DELETION SUMMARY")
    print("="*80)
    print(f"  Total Databases: {len(analysis['databases'])}")
    print(f"  Total Tables: {analysis['total_tables']}")
    print(f"  Total Rows to Delete: {analysis['total_rows']:,}")
    print("="*80)


def get_deletion_confirmation(analysis: Dict[str, Any]) -> bool:
    """Multi-step confirmation before deletion."""
    print("\n🔴 DANGER: This will PERMANENTLY DELETE data from the target database!")
    print("🔴 Make sure you have a backup before proceeding!")
    
    # Step 1: Initial warning
    print("\n" + "─"*80)
    response = input("Do you understand this will DELETE data? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("❌ Deletion cancelled.")
        return False
    
    # Step 2: Show plan
    show_deletion_plan(analysis)
    
    # Step 3: Confirm after seeing plan
    print("\n" + "─"*80)
    print(f"⚠️  You are about to delete {analysis['total_rows']:,} rows from {analysis['total_tables']} tables!")
    response = input("\nProceed with deletion? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("❌ Deletion cancelled.")
        return False
    
    # Step 4: Final confirmation with typing
    print("\n" + "─"*80)
    print("🔴 FINAL CONFIRMATION REQUIRED")
    print(f"Type 'DELETE DATA' to confirm deletion of {analysis['total_rows']:,} rows:")
    response = input("> ").strip()
    
    if response != 'DELETE DATA':
        print("❌ Deletion cancelled. (You must type exactly: DELETE DATA)")
        return False
    
    print("\n✅ Confirmation received. Starting deletion...")
    return True


def backup_before_deletion(databases: List[str], backup_dir: str = 'deletion_backups') -> str:
    """Create backup of target before deletion."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = os.path.join(backup_dir, f'backup_before_deletion_{timestamp}')
    os.makedirs(backup_path, exist_ok=True)
    
    print("\n📦 Creating backup of TARGET database before deletion...")
    print(f"   Location: {backup_path}")
    
    for db_name in databases:
        backup_file = os.path.join(backup_path, f'{db_name}.sql')
        
        print(f"   🔄 Backing up: {db_name}...", end='', flush=True)
        
        cmd = [
            'mysqldump',
            '-h', WRITE_CONFIG['host'],
            '-P', str(WRITE_CONFIG['port']),
            '-u', WRITE_CONFIG['user'],
            '--databases', db_name
        ]
        
        if WRITE_CONFIG.get('password'):
            cmd.insert(1, f'-p{WRITE_CONFIG["password"]}')
        
        try:
            with open(backup_file, 'w') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, 
                                       text=True, timeout=300)
            
            if result.returncode == 0:
                size = os.path.getsize(backup_file)
                print(f" ✓ ({size / 1024 / 1024:.1f} MB)")
            else:
                print(f" ⚠️  Warning: {result.stderr}")
        
        except Exception as e:
            print(f" ❌ Error: {e}")
    
    print(f"\n✅ Backup completed: {backup_path}")
    return backup_path


def execute_deletion(target_conn, analysis: Dict[str, Any], log_file: str):
    """
    Execute deletion on target database.
    
    Args:
        target_conn: Target database connection
        analysis: Deletion analysis from analyze_deletion_scope()
        log_file: File to log deletions
    """
    deletion_log = {
        'timestamp': datetime.now().isoformat(),
        'target_host': WRITE_CONFIG['host'],
        'target_port': WRITE_CONFIG['port'],
        'deletions': []
    }
    
    total_deleted = 0
    
    print("\n🗑️  Starting deletion process...")
    
    # Disable FK checks temporarily
    with target_conn.cursor() as cursor:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        target_conn.commit()
    
    try:
        for db in analysis['databases']:
            print(f"\n📁 Database: {db['name']}")
            
            for table_info in db['tables']:
                table = table_info['name']
                print(f"  🗑️  Deleting from: {table}...", end='', flush=True)
                
                try:
                    with target_conn.cursor() as cursor:
                        # Build DELETE query
                        if table_info['has_customer_id'] and 'customer_id IN' in table_info['filter_used']:
                            # Extract customer IDs from filter string
                            import re
                            match = re.search(r'customer_id IN \(([\d,]+)\)', table_info['filter_used'])
                            if match:
                                customer_ids = [int(x) for x in match.group(1).split(',')]
                                placeholders = ','.join(['%s'] * len(customer_ids))
                                query = f"DELETE FROM `{db['name']}`.`{table}` WHERE customer_id IN ({placeholders})"
                                cursor.execute(query, customer_ids)
                        else:
                            # Delete all rows
                            query = f"DELETE FROM `{db['name']}`.`{table}`"
                            cursor.execute(query)
                        
                        deleted_count = cursor.rowcount
                        target_conn.commit()
                        
                        print(f" ✓ Deleted {deleted_count:,} rows")
                        total_deleted += deleted_count
                        
                        # Log deletion
                        deletion_log['deletions'].append({
                            'database': db['name'],
                            'table': table,
                            'rows_deleted': deleted_count,
                            'filter': table_info['filter_used']
                        })
                
                except Exception as e:
                    print(f" ❌ Error: {e}")
                    deletion_log['deletions'].append({
                        'database': db['name'],
                        'table': table,
                        'error': str(e)
                    })
    
    finally:
        # Re-enable FK checks
        with target_conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            target_conn.commit()
    
    # Save log
    with open(log_file, 'w') as f:
        json.dump(deletion_log, f, indent=2)
    
    print(f"\n✅ Deletion completed!")
    print(f"   Total rows deleted: {total_deleted:,}")
    print(f"   Log file: {log_file}")


def get_user_input() -> tuple:
    """Get databases and customer IDs from user."""
    print("\n📋 Deletion Configuration:")
    
    # Get source connection to list databases
    source_conn = pymysql.connect(**READ_CONFIG)
    
    try:
        databases = get_all_databases(source_conn)
        
        print("\n📁 Available Databases:")
        for idx, db in enumerate(databases, 1):
            print(f"   {idx}. {db}")
        
        print("\nEnter database names (comma-separated) or 'all':")
        db_input = input("> ").strip()
        
        if db_input.lower() == 'all':
            selected_databases = databases
        else:
            selected_databases = [db.strip() for db in db_input.split(',')]
        
        # Get customer IDs
        print("\n🎯 Filter by Customer IDs?")
        print("   Enter customer IDs (comma-separated) or 'none' to delete ALL data:")
        customer_input = input("> ").strip()
        
        if customer_input.lower() == 'none':
            print("\n⚠️  WARNING: No customer_id filter! This will delete ALL data from tables!")
            customer_ids = None
        else:
            # Parse customer IDs
            customer_input = customer_input.strip('[]')
            customer_ids = [int(x.strip()) for x in customer_input.split(',')]
        
        return selected_databases, customer_ids
    
    finally:
        source_conn.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Delete Migrated Data from Target Database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
⚠️  WARNING: This script DELETES data!

Examples:
  # Preview what would be deleted (safe)
  python delete_migrated_data.py --dry-run
  
  # Delete with backup (recommended)
  python delete_migrated_data.py --backup
  
  # Delete without backup (dangerous)
  python delete_migrated_data.py
  
  # Delete specific tables only
  python delete_migrated_data.py --tables users,orders
        '''
    )
    
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview deletion plan without executing')
    parser.add_argument('--backup', action='store_true',
                       help='Create backup before deletion (recommended)')
    parser.add_argument('--tables', type=str,
                       help='Comma-separated list of tables to delete')
    parser.add_argument('--databases', type=str,
                       help='Comma-separated list of databases (overrides interactive input)')
    parser.add_argument('--customer-ids', type=str,
                       help='Comma-separated customer IDs to filter')
    parser.add_argument('--no-confirmation', action='store_true',
                       help='Skip confirmation prompts (VERY DANGEROUS!)')
    
    args = parser.parse_args()
    
    print("\n🗑️  Delete Migrated Data Script")
    print("="*80)
    print("⚠️  WARNING: This will DELETE data from the TARGET database!")
    print(f"   Target: {WRITE_CONFIG['host']}:{WRITE_CONFIG['port']}")
    print("="*80)
    
    # Validate config
    validate_config()
    
    # Get databases and customer IDs
    if args.databases and args.customer_ids:
        # Both provided via command-line
        databases = [db.strip() for db in args.databases.split(',')]
        customer_ids = [int(x.strip()) for x in args.customer_ids.split(',')]
    elif args.databases:
        # Only databases via command-line, get customer IDs interactively
        databases = [db.strip() for db in args.databases.split(',')]
        print("\n🎯 Filter by Customer IDs?")
        print("   Enter customer IDs (comma-separated) or 'none' to delete ALL data:")
        customer_input = input("> ").strip()
        
        if customer_input.lower() == 'none':
            print("\n⚠️  WARNING: No customer_id filter! This will delete ALL data from tables!")
            customer_ids = None
        else:
            customer_input = customer_input.strip('[]')
            customer_ids = [int(x.strip()) for x in customer_input.split(',')]
    else:
        # Get both interactively
        databases, customer_ids = get_user_input()
    
    # Get specific tables if provided
    specific_tables = [t.strip() for t in args.tables.split(',')] if args.tables else None
    
    # Connect to databases
    source_conn = pymysql.connect(**READ_CONFIG)
    target_conn = pymysql.connect(**WRITE_CONFIG)
    
    try:
        # Analyze deletion scope
        print("\n🔍 Analyzing deletion scope...")
        analysis = analyze_deletion_scope(source_conn, target_conn, databases, 
                                         customer_ids, specific_tables)
        
        if analysis['total_rows'] == 0:
            print("\n✅ No data to delete (tables are empty or don't exist in target).")
            return
        
        # Dry run
        if args.dry_run:
            show_deletion_plan(analysis)
            print("\n✅ DRY-RUN completed. No data was deleted.")
            print("   Remove --dry-run flag to execute deletion.")
            return
        
        # Get confirmation
        if not args.no_confirmation:
            if not get_deletion_confirmation(analysis):
                return
        
        # Backup
        if args.backup:
            backup_before_deletion(databases)
        
        # Execute deletion
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f"deletion_log_{timestamp}.json"
        
        execute_deletion(target_conn, analysis, log_file)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()
