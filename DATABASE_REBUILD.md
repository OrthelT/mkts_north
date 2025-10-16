# Database Rebuild Guide

This guide explains how to rebuild the database from scratch to fix sync history issues.

## Problem

When a Turso database has a long sync history, the `libsql.sync()` operation may try to replay the entire history, causing performance issues or timeouts. The solution is to start fresh with a clean database.

## Solution: Reset Turso Database (RECOMMENDED)

This is the simplest and most reliable approach.

### Steps

1. **Run the reset script:**
   ```bash
   python reset_turso_database.py
   ```

   This script will:
   - Delete all tables from the Turso database
   - Recreate the schema
   - Delete your local database file
   - Prepare for fresh data population

2. **Populate with fresh data:**
   ```bash
   uv run mkts-north
   ```

   This will:
   - Create a fresh local database
   - Fetch market orders from ESI API
   - Calculate market stats
   - Calculate doctrine stats
   - Write everything to Turso
   - Sync should now work correctly

3. **Verify:**
   ```bash
   uv run mkts-north --check_tables
   ```

### Why This Works

- Turso database is completely reset (no history to replay)
- Local database is deleted (forces fresh sync)
- Normal data flow writes directly to Turso
- Sync operations are now clean and fast

## Alternative: Full Local Rebuild (ADVANCED)

If you prefer to build the database locally first and then upload it, use this approach.

### Steps

1. **Run the rebuild script:**
   ```bash
   python rebuild_database.py
   ```

   This script will:
   - Backup existing local database
   - Create fresh local database with schema
   - Fetch and populate data locally
   - Export to SQL format
   - Upload to Turso using Turso CLI

2. **Manual upload (if automatic upload fails):**
   ```bash
   # Export database to SQL
   sqlite3 wcmktnorth.db .dump > wcmktnorth.db.sql

   # Upload to Turso (replace 'your-db-name' with actual database name)
   turso db shell your-db-name < wcmktnorth.db.sql
   ```

3. **Verify:**
   ```bash
   uv run mkts-north --check_tables
   ```

### Requirements

- Turso CLI installed: `curl -sSfL https://get.tur.so/install.sh | bash`
- sqlite3 command-line tool

## Troubleshooting

### Sync Still Slow After Reset

If sync is still slow after resetting:

1. Check Turso database size:
   ```bash
   turso db show your-db-name
   ```

2. Consider creating a completely new Turso database:
   ```bash
   turso db create wcmktnorth-new
   # Update .env with new database URL and token
   # Run reset script again
   ```

### Upload Fails

If automatic upload to Turso fails:

1. Export the database manually:
   ```bash
   sqlite3 wcmktnorth.db .dump > wcmktnorth.db.sql
   ```

2. Upload using Turso CLI:
   ```bash
   turso db shell your-db-name < wcmktnorth.db.sql
   ```

3. Or use Turso dashboard to upload the file

### Verification Fails

If database verification shows mismatches:

1. Delete local database:
   ```bash
   rm wcmktnorth.db
   ```

2. Force sync from Turso:
   ```python
   from mkts_backend.config.config import DatabaseConfig
   db = DatabaseConfig("wcmkt")
   db.sync()
   ```

## Prevention

To avoid future sync history issues:

1. **Regular cleanup:** Periodically reset the database if sync becomes slow
2. **Monitor sync performance:** Watch for increasing sync times
3. **Use TTL for history:** Consider implementing time-based cleanup for historical data
4. **Turso replication:** Use Turso's replica features instead of libsql sync if possible

## Notes

- The reset process will delete ALL data in the database
- Make sure you have backups before running reset scripts
- Market data can be re-fetched from ESI API
- Doctrine configurations may need to be restored separately

## Support

If you encounter issues with the rebuild process:

1. Check logs in the console output
2. Verify Turso credentials in `.env` file
3. Ensure you have proper ESI authentication tokens
4. Check that all required dependencies are installed
