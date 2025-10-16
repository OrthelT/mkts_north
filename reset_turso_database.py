#!/usr/bin/env python3
"""
Turso Database Reset Script

This script provides a simpler approach to fixing sync history issues:
1. Reset the Turso database (delete all tables)
2. Recreate the schema
3. Populate with fresh data using normal data flow

This avoids local sync issues by working directly with Turso.

Usage:
    python reset_turso_database.py
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from mkts_backend.config.logging_config import configure_logging
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.db.models import Base
from sqlalchemy import create_engine, text
import time

logger = configure_logging(__name__)

def reset_turso_database():
    """Delete all data from Turso database"""
    logger.info("=" * 80)
    logger.info("RESETTING TURSO DATABASE")
    logger.info("=" * 80)

    try:
        db = DatabaseConfig("wcmkt")
        engine = db.remote_engine

        # Get list of all tables
        logger.info("Getting list of tables...")
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT name FROM sqlite_master
                WHERE type='table'
                AND name NOT LIKE 'sqlite_%'
                AND name NOT LIKE '_litestream_%'
                AND name NOT LIKE 'libsql_%'
            """))
            tables = [row[0] for row in result.fetchall()]

        logger.info(f"Found {len(tables)} tables: {tables}")

        # Drop all tables
        logger.info("Dropping all tables...")
        with engine.connect() as conn:
            with conn.begin():
                for table in tables:
                    logger.info(f"  Dropping table: {table}")
                    conn.execute(text(f"DROP TABLE IF EXISTS {table}"))

        logger.info("All tables dropped")

        # Recreate schema
        logger.info("Recreating schema...")
        Base.metadata.create_all(engine)
        logger.info("Schema recreated")

        # Verify
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT name FROM sqlite_master
                WHERE type='table'
                AND name NOT LIKE 'sqlite_%'
            """))
            tables = [row[0] for row in result.fetchall()]

        logger.info(f"Tables after recreation: {tables}")

        engine.dispose()

        logger.info("=" * 80)
        logger.info("Turso database reset complete")
        logger.info("=" * 80)

        return True

    except Exception as e:
        logger.error(f"Failed to reset Turso database: {e}", exc_info=True)
        return False

def delete_local_database():
    """Delete the local database file to force fresh sync"""
    db_path = Path("wcmktnorth.db")

    if db_path.exists():
        logger.info(f"Deleting local database: {db_path}")
        db_path.unlink()
        logger.info("Local database deleted")

        # Also delete any journal files
        for journal_file in ["wcmktnorth.db-shm", "wcmktnorth.db-wal"]:
            journal_path = Path(journal_file)
            if journal_path.exists():
                journal_path.unlink()
                logger.info(f"Deleted journal file: {journal_file}")
    else:
        logger.info("No local database to delete")

def populate_data():
    """Run the normal CLI to populate data"""
    logger.info("=" * 80)
    logger.info("POPULATING DATA")
    logger.info("=" * 80)

    logger.info("Now you can run the normal CLI command to populate data:")
    logger.info("  uv run mkts-north")
    logger.info("")
    logger.info("This will:")
    logger.info("  1. Create fresh local database")
    logger.info("  2. Fetch market orders from ESI")
    logger.info("  3. Calculate market stats")
    logger.info("  4. Calculate doctrine stats")
    logger.info("  5. Write everything to Turso")
    logger.info("")
    logger.info("The sync should now work correctly without replaying history.")

def main():
    """Main reset process"""
    start_time = time.perf_counter()

    logger.info("=" * 80)
    logger.info("TURSO DATABASE RESET PROCESS")
    logger.info("=" * 80)
    logger.info("")
    logger.info("WARNING: This will delete ALL data in the Turso database!")
    logger.info("Make sure you have backups if needed.")
    logger.info("")

    # Confirm with user
    response = input("Are you sure you want to continue? (yes/no): ")
    if response.lower() not in ["yes", "y"]:
        logger.info("Reset cancelled")
        return False

    try:
        # Step 1: Reset Turso database
        logger.info("\nStep 1: Resetting Turso database")
        if not reset_turso_database():
            logger.error("✗ Failed to reset Turso database")
            return False
        logger.info("✓ Turso database reset")

        # Step 2: Delete local database
        logger.info("\nStep 2: Deleting local database")
        delete_local_database()
        logger.info("✓ Local database deleted")

        # Step 3: Instructions for populating
        logger.info("\nStep 3: Next steps")
        populate_data()

        elapsed = time.perf_counter() - start_time
        logger.info("=" * 80)
        logger.info(f"RESET COMPLETE in {elapsed:.1f}s")
        logger.info("=" * 80)

        return True

    except Exception as e:
        logger.error(f"Reset failed: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
