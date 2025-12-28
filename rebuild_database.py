#!/usr/bin/env python3
"""
Database Rebuild Script

This script rebuilds the database from scratch to avoid sync history issues.
It creates a fresh local database, populates it with initial data, and uploads to Turso.

Usage:
    python rebuild_database.py
"""

import os
import sys
import shutil
import time
from pathlib import Path
from datetime import datetime

# Add src to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent / "src"))

from mkts_backend.config.logging_config import configure_logging
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.config.esi_config import ESIConfig
from mkts_backend.db.models import Base
from mkts_backend.cli import (
    process_market_orders,
    process_history,
    process_market_stats,
    process_doctrine_stats,
)
from mkts_backend.utils.utils import init_databases
from sqlalchemy import create_engine, text, delete

logger = configure_logging(__name__)

def backup_existing_database():
    """Backup the existing database file if it exists"""
    db_path = Path("wcmktnorth2.db")

    if not db_path.exists():
        logger.info("No existing database to backup")
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = Path(f"wcmktnorth2_backup_{timestamp}.db")

    logger.info(f"Backing up existing database to {backup_path}")
    shutil.copy2(db_path, backup_path)
    logger.info(f"Backup created: {backup_path}")

    return backup_path

def delete_local_database():
    """Delete the local database file"""
    db_path = Path("wcmktnorth2.db")

    if db_path.exists():
        logger.info(f"Deleting local database: {db_path}")
        db_path.unlink()
        logger.info("Local database deleted")
    else:
        logger.info("No local database to delete")

def create_fresh_database():
    """Create a fresh database with all tables using SQLAlchemy models"""
    logger.info("Creating fresh database with schema")

    # Create a fresh local database
    db_url = "sqlite+libsql:///wcmktnorth2.db"
    engine = create_engine(db_url)

    # Create all tables from Base metadata
    Base.metadata.create_all(engine)

    logger.info("Database schema created")

    # Verify tables were created
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
        tables = [row[0] for row in result.fetchall()]
        logger.info(f"Created tables: {tables}")

    engine.dispose()
    return True

def populate_initial_data():
    """Populate the database with initial market data"""
    logger.info("=" * 80)
    logger.info("Populating database with initial market data")
    logger.info("=" * 80)

    # Initialize databases (this will sync the watchlist and other static data)
    # We need to be careful here - we want to populate LOCAL database only
    # until we're ready to upload to Turso
    logger.info("Initializing databases...")

    # Import required modules for local-only operations
    import pandas as pd
    import json
    from mkts_backend.esi.esi_requests import fetch_market_orders
    from mkts_backend.db.db_handlers import update_market_orders, upsert_database, log_update
    from mkts_backend.db.models import MarketStats, Doctrines
    from mkts_backend.processing.data_processing import calculate_market_stats, calculate_doctrine_stats
    from mkts_backend.utils.utils import validate_columns, convert_datetime_columns

    # Temporarily patch upsert_database to use local engine only
    # Save original function
    original_upsert = update_market_orders.__globals__['upsert_database']

    def local_upsert_database(table, df, remote: bool = False):
        """Modified upsert that only writes to local database"""
        from sqlalchemy.orm import Session
        from sqlalchemy import select, insert, func, or_, delete
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        db = DatabaseConfig("wcmkt")
        local_engine = db.engine if not remote else db.remote_engine
        session = Session(bind=local_engine)

        t = table.__table__
        pk_cols = list(t.primary_key.columns)
        pk_col = pk_cols[0] if len(pk_cols) == 1 else pk_cols

        WIPE_REPLACE_TABLES = ["marketstats", "doctrines"]
        is_wipe_replace = table.__tablename__ in WIPE_REPLACE_TABLES

        data = df.to_dict(orient="records")
        MAX_PARAMETER_BYTES = 256 * 1024
        BYTES_PER_PARAMETER = 8
        MAX_PARAMETERS = MAX_PARAMETER_BYTES // BYTES_PER_PARAMETER
        column_count = len(df.columns)
        chunk_size = min(2000, MAX_PARAMETERS // column_count)

        try:
            with session.begin():
                if is_wipe_replace:
                    session.execute(delete(table))
                    for idx in range(0, len(data), chunk_size):
                        chunk = data[idx : idx + chunk_size]
                        stmt = insert(t).values(chunk)
                        session.execute(stmt)
                else:
                    # Upsert logic
                    non_pk_cols = [c for c in t.columns if c not in pk_cols]
                    data_cols = [c for c in non_pk_cols if c.name not in ['timestamp', 'last_update', 'created_at', 'updated_at']]

                    for idx in range(0, len(data), chunk_size):
                        chunk = data[idx : idx + chunk_size]
                        base = sqlite_insert(t).values(chunk)
                        excluded = base.excluded
                        set_mapping = {c.name: excluded[c.name] for c in non_pk_cols}

                        if data_cols:
                            changed_pred = or_(*[c.is_distinct_from(excluded[c.name]) for c in data_cols])
                        else:
                            changed_pred = True

                        if isinstance(pk_col, list):
                            stmt = base.on_conflict_do_update(
                                index_elements=pk_col, set_=set_mapping, where=changed_pred
                            )
                        else:
                            stmt = base.on_conflict_do_update(
                                index_elements=[pk_col], set_=set_mapping, where=changed_pred
                            )
                        session.execute(stmt)

            logger.info(f"Local upsert complete: {table.__tablename__}")
        except Exception as e:
            logger.error(f"Local upsert failed: {e}")
            raise
        finally:
            session.close()
            local_engine.dispose()

        return True

    # Patch the function
    import mkts_backend.db.db_handlers as db_handlers
    db_handlers.upsert_database = local_upsert_database

    try:
        # Initialize databases to populate watchlist
        init_databases()
        logger.info("Databases initialized")

        # Create ESI config
        esi = ESIConfig("primary")

        # Fetch market orders
        logger.info("Fetching market orders...")
        orders = fetch_market_orders(esi, order_type="all", test_mode=False)
        if not orders:
            logger.error("Failed to fetch market orders")
            return False

        logger.info(f"Fetched {len(orders)} market orders")

        # Process orders locally
        from mkts_backend.db.db_handlers import update_market_orders
        status = update_market_orders(orders)
        if not status:
            logger.error("Failed to update market orders locally")
            return False
        logger.info("Market orders stored locally")

        # Calculate market stats (without sync)
        logger.info("Calculating market stats...")
        market_stats_df = calculate_market_stats()
        if len(market_stats_df) == 0:
            logger.error("Failed to calculate market stats")
            return False

        valid_market_stats_columns = MarketStats.__table__.columns.keys()
        market_stats_df = validate_columns(market_stats_df, valid_market_stats_columns)

        status = local_upsert_database(MarketStats, market_stats_df)
        if not status:
            logger.error("Failed to store market stats locally")
            return False
        logger.info("Market stats calculated and stored locally")

        # Calculate doctrine stats (without sync)
        logger.info("Calculating doctrine stats...")
        doctrine_stats_df = calculate_doctrine_stats()
        doctrine_stats_df = convert_datetime_columns(doctrine_stats_df, ["timestamp"])

        status = local_upsert_database(Doctrines, doctrine_stats_df)
        if not status:
            logger.error("Failed to store doctrine stats locally")
            return False
        logger.info("Doctrine stats calculated and stored locally")

        return True

    finally:
        # Restore original function
        db_handlers.upsert_database = original_upsert

    return True

def upload_to_turso():
    """Upload the fresh database to Turso using Turso CLI"""
    logger.info("=" * 80)
    logger.info("Uploading database to Turso")
    logger.info("=" * 80)

    try:
        from dotenv import load_dotenv
        import subprocess

        load_dotenv()

        # Get Turso database name from URL
        turso_url = os.getenv("TURSO_WCMKTNORTH2_URL")
        if not turso_url:
            logger.error("Turso URL not found in environment")
            return False

        # Extract database name from URL (e.g., libsql://dbname-org.turso.io -> dbname)
        # The URL format is: libsql://[db-name]-[org-name].turso.io
        turso_url_clean = turso_url.replace("libsql://", "").replace("https://", "")
        db_name = turso_url_clean.split("-")[0] if "-" in turso_url_clean else turso_url_clean.split(".")[0]

        logger.info(f"Detected Turso database name: {db_name}")
        logger.info("=" * 80)
        logger.info("IMPORTANT: To upload the database to Turso, run these commands:")
        logger.info("=" * 80)
        logger.info(f"turso db shell {db_name} < wcmktnorth2.db.sql")
        logger.info("OR")
        logger.info(f"turso db upload {db_name} wcmktnorth2.db")
        logger.info("=" * 80)
        logger.info("Note: You may need to export the database to SQL format first:")
        logger.info("sqlite3 wcmktnorth2.db .dump > wcmktnorth2.db.sql")
        logger.info("=" * 80)

        # Try using turso CLI if available
        logger.info("Attempting to upload using Turso CLI...")

        # First, check if turso CLI is available
        try:
            result = subprocess.run(
                ["turso", "--version"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode != 0:
                logger.warning("Turso CLI not found or not working")
                logger.info("Please install Turso CLI: curl -sSfL https://get.tur.so/install.sh | bash")
                return False
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.warning(f"Turso CLI not available: {e}")
            logger.info("Please install Turso CLI: curl -sSfL https://get.tur.so/install.sh | bash")
            return False

        # Export database to SQL
        logger.info("Exporting database to SQL format...")
        result = subprocess.run(
            ["sqlite3", "wcmktnorth2.db", ".dump"],
            capture_output=True,
            text=True,
            timeout=300
        )
        if result.returncode != 0:
            logger.error(f"Failed to export database: {result.stderr}")
            return False

        sql_dump = result.stdout
        with open("wcmktnorth2.db.sql", "w") as f:
            f.write(sql_dump)
        logger.info("Database exported to wcmktnorth2.db.sql")

        # Upload to Turso
        logger.info(f"Uploading to Turso database '{db_name}'...")
        logger.info("This may take several minutes for large databases...")

        result = subprocess.run(
            ["turso", "db", "shell", db_name],
            input=sql_dump,
            capture_output=True,
            text=True,
            timeout=600
        )

        if result.returncode != 0:
            logger.error(f"Failed to upload to Turso: {result.stderr}")
            logger.info("You can manually upload with:")
            logger.info(f"  turso db shell {db_name} < wcmktnorth2.db.sql")
            return False

        logger.info("Database uploaded to Turso successfully")
        return True

    except subprocess.TimeoutExpired:
        logger.error("Upload timed out - database may be too large")
        logger.info("Try uploading manually with:")
        logger.info(f"  turso db shell {db_name} < wcmktnorth2.db.sql")
        return False
    except Exception as e:
        logger.error(f"Failed to upload to Turso: {e}")
        logger.info("You can manually upload the database using Turso CLI")
        return False

def verify_database():
    """Verify the database was created and populated correctly"""
    logger.info("=" * 80)
    logger.info("Verifying database")
    logger.info("=" * 80)

    try:
        db = DatabaseConfig("wcmkt")

        # Check local database
        logger.info("Checking local database...")
        with db.engine.connect() as conn:
            tables_to_check = ["marketorders", "marketstats", "doctrines", "watchlist"]
            for table in tables_to_check:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                logger.info(f"  {table}: {count} rows")

        # Check remote database
        logger.info("Checking remote database...")
        with db.remote_engine.connect() as conn:
            for table in tables_to_check:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                logger.info(f"  {table}: {count} rows")

        # Validate sync
        logger.info("Validating sync...")
        validation = db.validate_sync()
        if validation:
            logger.info("✓ Database sync validated successfully")
        else:
            logger.warning("⚠ Database sync validation failed")

        db.engine.dispose()
        db.remote_engine.dispose()

        return validation

    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return False

def main():
    """Main rebuild process"""
    start_time = time.perf_counter()

    logger.info("=" * 80)
    logger.info("DATABASE REBUILD PROCESS")
    logger.info("=" * 80)

    try:
        # Step 1: Backup existing database
        logger.info("\nStep 1: Backing up existing database")
        backup_path = backup_existing_database()
        if backup_path:
            logger.info(f"✓ Backup created: {backup_path}")

        # Step 2: Delete local database
        logger.info("\nStep 2: Deleting local database")
        delete_local_database()
        logger.info("✓ Local database deleted")

        # Step 3: Create fresh database
        logger.info("\nStep 3: Creating fresh database with schema")
        if not create_fresh_database():
            logger.error("✗ Failed to create fresh database")
            return False
        logger.info("✓ Fresh database created")

        # Step 4: Populate with initial data
        logger.info("\nStep 4: Populating database with initial data")
        if not populate_initial_data():
            logger.error("✗ Failed to populate database")
            return False
        logger.info("✓ Database populated")

        # Step 5: Upload to Turso
        logger.info("\nStep 5: Uploading database to Turso")
        if not upload_to_turso():
            logger.error("✗ Failed to upload to Turso")
            return False
        logger.info("✓ Database uploaded to Turso")

        # Step 6: Verify
        logger.info("\nStep 6: Verifying database")
        if not verify_database():
            logger.warning("⚠ Database verification had issues")
        else:
            logger.info("✓ Database verified")

        elapsed = time.perf_counter() - start_time
        logger.info("=" * 80)
        logger.info(f"DATABASE REBUILD COMPLETE in {elapsed:.1f}s")
        logger.info("=" * 80)

        return True

    except Exception as e:
        logger.error(f"Database rebuild failed: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
