# Database Migration Tools - Production Guide

## Overview

This repository contains two complementary MariaDB migration scripts:

| Script | Purpose |
|--------|---------|
| `migrate_databases.py` | **Schema Migration** - Migrates database structure (tables, indexes, foreign keys) |
| `migrate_customer_data_v3.py` | **Data Migration** - Migrates customer-specific data with intelligent filtering |

---

## Quick Start

```bash
# 1. Set up environment
cp .env.example .env
# Edit .env with your database credentials

# 2. Install dependencies
pip install pymysql python-dotenv

# 3. Run schema migration first
python migrate_databases.py

# 4. Run data migration
python migrate_customer_data_v3.py --databases STARFOX --customer-ids 1,2,3
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     MIGRATION WORKFLOW                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   SOURCE DB (READ)              DESTINATION DB (WRITE)              │
│   ┌──────────────┐              ┌──────────────┐                    │
│   │  MariaDB     │  ──────────► │  MariaDB     │                    │
│   │  (Production)│              │  (Target)    │                    │
│   └──────────────┘              └──────────────┘                    │
│                                                                      │
│   STEP 1: migrate_databases.py                                       │
│   ├── Creates databases                                              │
│   ├── Creates tables (without FKs)                                   │
│   └── Adds foreign key constraints                                   │
│                                                                      │
│   STEP 2: migrate_customer_data_v3.py                                │
│   ├── Phase 0: Stored procedures/functions                          │
│   ├── Phase 1: Tables with customer_id (filtered)                   │
│   ├── Phase 1B: Tables with user_id (filtered)                      │
│   ├── Phase 1C: Tables with indirect FK (filtered via JOIN)         │
│   └── Phase 2: Reference tables (all data)                          │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Configuration

### Environment Variables (.env file)

```bash
# ============================================================
# DATABASE CONNECTION (REQUIRED)
# ============================================================

# Source database (READ from)
READ_DB_HOST=source-db.example.com
READ_DB_PORT=3306
READ_DB_USER=readonly_user
READ_DB_PASSWORD=your_password

# Destination database (WRITE to)
WRITE_DB_HOST=dest-db.example.com
WRITE_DB_PORT=3306
WRITE_DB_USER=write_user
WRITE_DB_PASSWORD=your_password

# ============================================================
# DATA MIGRATION SETTINGS (OPTIONAL)
# ============================================================

# Row threshold for confirmation prompts (default: 400)
# Tables exceeding this will prompt for confirmation
AUTO_CONFIRM_THRESHOLD=400

# Seed user IDs for user_id filtering (default: 1,2)
SEED_USER_IDS=1,2

# Tables to force-migrate (all data, no confirmation)
# Supports: DATABASE.TABLE, *.TABLE, TABLE
FORCE_MIGRATE_TABLES=*.schema_version,*.flyway_schema_history

# Tables to completely skip (never migrate)
# Supports: DATABASE.TABLE, DATABASE.*, *.TABLE
SKIP_TABLES=STARFOX.AUDIT_LOG,*.temp_data

# Auto-skip large reference tables (default: false)
# When true, tables > AUTO_CONFIRM_THRESHOLD are auto-skipped
SKIP_LARGE_TABLES=false

# State file directory (default: .migration_state)
MIGRATION_STATE_DIR=.migration_state
```

---

## Configuration Priority (Highest to Lowest)

```
┌─────────────────────────────────────────────────────────────┐
│  PRIORITY 1: Command-Line Arguments (Highest)               │
│  --force, --force-tables, --databases, --customer-ids       │
├─────────────────────────────────────────────────────────────┤
│  PRIORITY 2: Environment Variables (.env file)              │
│  SKIP_TABLES, FORCE_MIGRATE_TABLES, SKIP_LARGE_TABLES, etc. │
├─────────────────────────────────────────────────────────────┤
│  PRIORITY 3: Hardcoded Defaults (Lowest)                    │
│  BATCH_SIZE=1000, AUTO_CONFIRM_THRESHOLD=400, etc.          │
└─────────────────────────────────────────────────────────────┘
```

### Priority Examples

| Scenario | Result |
|----------|--------|
| `--force` flag + state file says "completed" | Table is RE-migrated (CLI wins) |
| `SKIP_TABLES=*.AUDIT_LOG` + `--force-tables AUDIT_LOG` | Table is migrated (CLI wins) |
| `SKIP_TABLES` set + no CLI override | Table is skipped (env wins) |
| Nothing configured | Hardcoded defaults apply |

---

## Script 1: migrate_databases.py (Schema Migration)

### What It Does

1. Connects to source database
2. Creates target databases with proper character set
3. **Pass 1**: Creates all tables WITHOUT foreign keys
4. **Pass 2**: Adds all foreign key constraints

### Usage

```bash
python migrate_databases.py
```

### Interactive Prompts

```
Database names: STARFOX, ONBOARDING    # Enter databases
# or
Database names: all                     # Migrate all databases
```

### Two-Pass Approach Explained

```
WHY TWO PASSES?

Pass 1 (Tables without FKs):
  CREATE TABLE ROLE (...)          ✓ Created
  CREATE TABLE USER (...)          ✓ Created
  CREATE TABLE ROLE_USER (...)     ✓ Created (but FK to ROLE would fail if ROLE didn't exist)

Pass 2 (Add FKs):
  ALTER TABLE ROLE_USER ADD CONSTRAINT fk_role FOREIGN KEY (role_id) REFERENCES ROLE(id)
  ✓ Works because ROLE already exists
```

---

## Script 2: migrate_customer_data_v3.py (Data Migration)

### What It Does

Intelligently migrates data based on table relationships to customer IDs.

### Table Categories

```
┌────────────────────────────────────────────────────────────────────┐
│  CATEGORY A: Direct Filter (customer_id column)                    │
│  ────────────────────────────────────────────────────────────────  │
│  Example: CUSTOMER, ACCOUNT, ORDER                                 │
│  Filter: WHERE customer_id IN (1, 2, 3)                            │
├────────────────────────────────────────────────────────────────────┤
│  CATEGORY A: Direct Filter (user_id column)                        │
│  ────────────────────────────────────────────────────────────────  │
│  Example: USER_PREFERENCES, USER_SETTINGS                          │
│  Filter: WHERE user_id IN (1, 2)  [SEED_USER_IDS]                  │
├────────────────────────────────────────────────────────────────────┤
│  CATEGORY B: Indirect Filter (via Foreign Key)                     │
│  ────────────────────────────────────────────────────────────────  │
│  Example: ROLE_ACCESS_MAP → ROLE.customer_id                       │
│  Filter: JOIN to table with customer_id, then filter               │
├────────────────────────────────────────────────────────────────────┤
│  CATEGORY C: Reference Tables (no customer relationship)           │
│  ────────────────────────────────────────────────────────────────  │
│  Example: ACCESS_RIGHT, COUNTRY, CURRENCY                          │
│  Action: Migrate ALL rows (with confirmation if > threshold)       │
└────────────────────────────────────────────────────────────────────┘
```

### Migration Phases

```
Phase 0: Stored Procedures & Functions
         └── Migrates all procedures/functions in the database

Phase 1: Tables WITH customer_id
         └── Filtered by customer_ids parameter

Phase 1B: Tables WITH user_id
          └── Filtered by SEED_USER_IDS (default: 1, 2)

Phase 1C: Tables with Indirect FK Relationships
          └── Filtered via JOIN to related table

Phase 2: Pure Reference Tables
          └── All data (with confirmation for large tables)
```

### Command-Line Interface

```bash
# Interactive mode
python migrate_customer_data_v3.py

# Non-interactive mode
python migrate_customer_data_v3.py --databases STARFOX,ONBOARDING --customer-ids 1,2,3

# Show migration status
python migrate_customer_data_v3.py --status --customer-ids 1,2,3

# Force re-migrate all tables
python migrate_customer_data_v3.py --force --databases STARFOX --customer-ids 1,2,3

# Force specific tables only
python migrate_customer_data_v3.py --force-tables "ACCESS_RIGHT,AUDIT_LOG" --databases STARFOX --customer-ids 1,2,3

# Force with database prefix
python migrate_customer_data_v3.py --force-tables "STARFOX.ACCESS_RIGHT" --databases STARFOX --customer-ids 1,2,3
```

### CLI Arguments Reference

| Argument | Description | Example |
|----------|-------------|---------|
| `--status` | Show migration status from state file | `--status --customer-ids 1,2` |
| `--force` | Force re-migrate ALL tables | `--force` |
| `--force-tables` | Force specific tables | `--force-tables "TABLE1,TABLE2"` |
| `--customer-ids` | Customer IDs to filter | `--customer-ids 1,2,3` |
| `--databases` | Databases to migrate | `--databases STARFOX,ONBOARDING` |

---

## State File System (V3 Feature)

### Location

```
.migration_state/
└── migration_state_1_2_3.json    # Named by sorted customer IDs
```

### State File Structure

```json
{
  "created_at": "2024-01-15T10:30:00",
  "updated_at": "2024-01-15T11:45:00",
  "databases": {
    "STARFOX": {
      "tables": {
        "CUSTOMER": {
          "status": "completed",
          "rows": 150,
          "timestamp": "2024-01-15T10:35:00"
        },
        "AUDIT_LOG": {
          "status": "skipped",
          "rows": 0,
          "reason": "user_declined",
          "timestamp": "2024-01-15T10:40:00"
        }
      },
      "routines": {
        "sp_get_customer": {
          "type": "PROCEDURE",
          "status": "completed",
          "timestamp": "2024-01-15T10:32:00"
        }
      }
    }
  }
}
```

### State Status Values

| Status | Meaning | Behavior on Next Run |
|--------|---------|---------------------|
| `completed` | Successfully migrated | Skipped (unless `--force`) |
| `skipped` | User declined or env skip | Prompted again if `reason=user_declined` |
| `failed` | Migration error | Will retry |

---

## Pattern Matching for SKIP_TABLES and FORCE_MIGRATE_TABLES

### Supported Patterns

```bash
# Exact match (specific database and table)
SKIP_TABLES=STARFOX.AUDIT_LOG

# All tables in a database
SKIP_TABLES=STARFOX.*

# Same table in any database
SKIP_TABLES=*.temp_data

# Multiple patterns (comma-separated)
SKIP_TABLES=STARFOX.AUDIT_LOG,*.temp_data,LOGS.*
```

### Pattern Matching Logic

```python
# Pattern: DATABASE.TABLE (exact match)
"STARFOX.AUDIT_LOG" matches only STARFOX database, AUDIT_LOG table

# Pattern: DATABASE.* (database wildcard)
"STARFOX.*" matches ALL tables in STARFOX database

# Pattern: *.TABLE (table wildcard)
"*.AUDIT_LOG" matches AUDIT_LOG in ANY database

# Pattern: TABLE (no dot - legacy)
"AUDIT_LOG" matches AUDIT_LOG in ANY database
```

---

## Foreign Key Handling

### During Migration

```
1. FK checks DISABLED at start of data migration
   └── SET FOREIGN_KEY_CHECKS = 0

2. Data inserted in any order
   └── INSERT IGNORE (handles duplicates)

3. FK checks RE-ENABLED after migration
   └── SET FOREIGN_KEY_CHECKS = 1
```

### Why Disable FK Checks?

```
Problem without disabling:
  INSERT INTO ROLE_USER (role_id=1, user_id=1)
  ❌ Error: Cannot add or update a child row - FK constraint fails
  (because ROLE with id=1 hasn't been inserted yet)

Solution with FK checks disabled:
  INSERT INTO ROLE_USER (role_id=1, user_id=1)  ✓
  INSERT INTO ROLE (id=1, ...)                   ✓
  (order doesn't matter - constraints verified when re-enabled)
```

---

## Workflow Examples

### Example 1: Fresh Migration

```bash
# 1. Migrate schema first
python migrate_databases.py
# Enter: STARFOX, ONBOARDING
# Confirm: yes

# 2. Migrate customer data
python migrate_customer_data_v3.py --databases STARFOX,ONBOARDING --customer-ids 1,2,3
# Confirm: yes
```

### Example 2: Resume Interrupted Migration

```bash
# Migration was interrupted - just run again
python migrate_customer_data_v3.py --databases STARFOX --customer-ids 1,2,3

# State file tracks progress - completed tables are skipped
# Output: "Already migrated [150 rows] (use --force to re-migrate)"
```

### Example 3: Check Status Before Continuing

```bash
# View what's been migrated
python migrate_customer_data_v3.py --status --customer-ids 1,2,3

# Output:
# MIGRATION STATUS
# State file: .migration_state/migration_state_1_2_3.json
# Database: STARFOX
#   Tables: 15 completed, 3 skipped, 18 total
#   ✓ CUSTOMER: completed [150 rows]
#   ⊗ AUDIT_LOG: skipped (user_declined)
```

### Example 4: Force Re-migrate Specific Tables

```bash
# Previously skipped AUDIT_LOG, now want to migrate it
python migrate_customer_data_v3.py \
  --force-tables "AUDIT_LOG" \
  --databases STARFOX \
  --customer-ids 1,2,3
```

### Example 5: Skip Large Reference Tables

```bash
# Set environment to auto-skip large tables
export SKIP_LARGE_TABLES=true
export AUTO_CONFIRM_THRESHOLD=100

python migrate_customer_data_v3.py --databases STARFOX --customer-ids 1,2,3

# Tables > 100 rows without customer_id are auto-skipped
# They'll be prompted on next run with --force-tables
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `Missing required environment variables` | .env not configured | Set all READ_DB_* and WRITE_DB_* vars |
| `Database does not exist in destination` | Schema not migrated | Run `migrate_databases.py` first |
| `Table does not exist in destination` | Table missing | Set `CREATE_MISSING_OBJECTS = True` or run schema migration |
| `Insert error: Duplicate entry` | Data already exists | Expected with INSERT IGNORE - duplicates skipped |
| `FK constraint fails` | FK checks not disabled | Check script is disabling FK checks |

### Debug Mode

```bash
# Check your environment
python -c "from dotenv import load_dotenv; import os; load_dotenv(); print(os.getenv('READ_DB_HOST'))"

# Test connection
python -c "
import pymysql
from dotenv import load_dotenv
import os
load_dotenv()
conn = pymysql.connect(
    host=os.getenv('READ_DB_HOST'),
    port=int(os.getenv('READ_DB_PORT', 3306)),
    user=os.getenv('READ_DB_USER'),
    password=os.getenv('READ_DB_PASSWORD')
)
print('Connected successfully!')
conn.close()
"
```

---

## Safety Features

### Built-in Protections

1. **State Tracking**: Prevents re-insertion on subsequent runs
2. **INSERT IGNORE**: Handles duplicate primary keys gracefully
3. **FK Checks Toggle**: Prevents constraint violations during migration
4. **Confirmation Prompts**: Large tables require explicit approval
5. **Graceful Abort**: Ctrl+C saves state and exits cleanly

### What's NOT Automatic

- ❌ Rollback on failure (use `delete_migrated_data.py` separately)
- ❌ Schema changes (run `migrate_databases.py` for that)
- ❌ Cross-database foreign keys
- ❌ Triggers and events (only procedures/functions are migrated)

---

## Best Practices

### Before Production Migration

1. **Test on staging first** - Never run directly on production
2. **Backup destination** - In case rollback is needed
3. **Verify credentials** - Test connections before starting
4. **Review SKIP_TABLES** - Ensure large audit tables are skipped

### During Migration

1. **Monitor progress** - Watch for errors in output
2. **Don't interrupt** - Let batches complete
3. **Check state file** - Verify tables are being tracked

### After Migration

1. **Verify row counts** - Compare source vs destination
2. **Test foreign keys** - Ensure relationships are intact
3. **Check procedures** - Verify stored procedures work

---

## File Reference

| File | Purpose |
|------|---------|
| `migrate_databases.py` | Schema migration script |
| `migrate_customer_data_v3.py` | Data migration script (V3) |
| `delete_migrated_data.py` | Cleanup/rollback script |
| `.env` | Environment configuration |
| `.migration_state/` | State tracking directory |
| `PROD_README.md` | This documentation |

---

## Version History

| Version | Features |
|---------|----------|
| V1 | Basic customer_id filtering |
| V2 | Force-migrate tables, implicit FK detection |
| V3 | State tracking, SKIP_TABLES, stored procedures, CLI flags |

---

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the state file for migration status
3. Check database connection with debug commands
