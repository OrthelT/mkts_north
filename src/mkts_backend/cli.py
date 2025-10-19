import sys
import json
import time
import pandas as pd
from mkts_backend.config.gsheets_config import GoogleSheetConfig
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.db_queries import get_table_length
from mkts_backend.db.db_handlers import (
    upsert_database,
    update_history,
    update_market_orders,
    update_jita_history,
    log_update,
)
from mkts_backend.db.models import MarketStats, Doctrines, Base
from mkts_backend.utils.utils import (
    validate_columns,
    convert_datetime_columns,
    init_databases,
)
from mkts_backend.processing.data_processing import (
    calculate_market_stats,
    calculate_doctrine_stats,
)
from sqlalchemy import text
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.config.esi_config import ESIConfig
from mkts_backend.esi.esi_requests import fetch_market_orders
from mkts_backend.esi.async_history import run_async_history, run_async_jita_history
from mkts_backend.utils.db_utils import check_updates, add_missing_items_to_watchlist
from mkts_backend.utils.parse_items import parse_items

logger = configure_logging(__name__)

def check_tables():
    tables = ["doctrines", "marketstats", "marketorders", "market_history"]
    db = DatabaseConfig("wcmkt")
    tables = db.get_table_list()

    for table in tables:
        print(f"Table: {table}")
        print("=" * 80)
        with db.engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table} LIMIT 10"))
            for row in result:
                print(row)
            print("\n")
        conn.close()
    db.engine.dispose()

def display_cli_help():
    print("Usage: mkts-backend [--history|--include-history] [--check_tables] [add_watchlist --type_id=<list[int]>] [parse-items --input=<file> --output=<file>]")
    print("Options:")
    print("  --history | --include-history: Include history processing")
    print("  --check_tables: Check the tables in the database")
    print("  add_watchlist --type_id=<list[int]>: Add items to watchlist by type IDs (comma-separated)")
    print("    --local: Use local database instead of remote (default: remote)")
    print("  parse-items --input=<file> --output=<file>: Parse Eve structure data and create CSV with pricing from database")

def process_add_watchlist(type_ids_str: str, remote: bool = True):
    """
    Process the add_watchlist command to add items to the watchlist.

    Args:
        type_ids_str: Comma-separated string of type IDs
        remote: Whether to use remote database
    """
    try:
        # Parse comma-separated type IDs
        type_ids = [int(tid.strip()) for tid in type_ids_str.split(',') if tid.strip()]

        if not type_ids:
            logger.error("No valid type IDs provided")
            print("Error: No valid type IDs provided")
            return False

        logger.info(f"Adding {len(type_ids)} items to watchlist: {type_ids}")
        print(f"Adding {len(type_ids)} items to watchlist: {type_ids}")

        # Initialize databases
        init_databases()

        # Add items to watchlist
        result = add_missing_items_to_watchlist(type_ids, remote=remote)

        print(result)
        logger.info(f"Add watchlist result: {result}")

        return True

    except ValueError as e:
        logger.error(f"Invalid type ID format: {e}")
        print(f"Error: Invalid type ID format. Please provide comma-separated integers. {e}")
        return False
    except Exception as e:
        logger.error(f"Error adding items to watchlist: {e}")
        print(f"Error: {e}")
        return False

def process_market_orders(esi: ESIConfig, order_type: str = "sell", test_mode: bool = False, remote: bool = True) -> bool:
    """Fetches market orders from ESI and updates the database"""
    save_path = "data/market_orders_new.json"
    data = fetch_market_orders(esi, order_type=order_type, test_mode=test_mode)
    if data:
        with open(save_path, "w") as f:
            json.dump(data, f)
        logger.info(f"ESI returned {len(data)} market orders. Saved to {save_path}")
        status = update_market_orders(data, remote=remote)
        if status:
            log_update("marketorders",remote=remote)
            logger.info(f"Orders updated:{get_table_length('marketorders')} items")
            return True
        else:
            logger.error(
                "Failed to update market orders. ESI call succeeded but something went wrong updating the database"
            )
            return False
    else:
        logger.error("no data returned from ESI call.")
        return False

def process_history():
    logger.info("History mode enabled")
    logger.info("Processing history")
    data = run_async_history()
    if data:
        with open("data/market_history_new.json", "w") as f:
            json.dump(data, f)
        status = update_history(data, remote=True)
        if status:
            log_update("market_history",remote=True)
            logger.info(f"History updated:{get_table_length('market_history')} items")
            return True
        else:
            logger.error("Failed to update market history")
            return False

def process_jita_history():
    """Process Jita (The Forge) history data"""
    logger.info("Processing Jita history from The Forge region")

    jita_records = run_async_jita_history()
    if jita_records:
        logger.info(f"Retrieved {len(jita_records)} Jita history records")
        status = update_jita_history(jita_records, remote=True)
        if status:
            log_update("jita_history", remote=True)
            logger.info(f"Jita history updated: {len(jita_records)} records")
            return True
        else:
            logger.error("Failed to update Jita history")
            return False
    else:
        logger.error("No Jita history data retrieved")
        return False

def process_market_stats(remote: bool = True):
    logger.info("Calculating market stats")
    logger.info("syncing database")
    db = DatabaseConfig("wcmkt")
    if remote:
        db.sync()
        logger.info("database synced")
        logger.info("validating database")
        validation_test = db.validate_sync()
        if validation_test:
            logger.info("database validated")
        else:
            logger.error("database validation failed")
            raise Exception("database validation failed in market stats")

    try:
        market_stats_df = calculate_market_stats()
    except Exception as e:
        logger.error(f"Failed to calculate market stats: {e}")
        return False
    try:
        logger.info("Validating market stats columns")
        valid_market_stats_columns = MarketStats.__table__.columns.keys()
        market_stats_df = validate_columns(market_stats_df, valid_market_stats_columns)
        if len(market_stats_df) > 0:
            logger.info(f"Market stats validated: {len(market_stats_df)} items")
        else:
            logger.error("Failed to validate market stats")
            return False
    except Exception as e:
        logger.error(f"Failed to get market stats columns: {e}")
        return False
    try:
        logger.info("Updating market stats in database")
        status = upsert_database(MarketStats, market_stats_df)
        if status:
            log_update("marketstats",remote=True)
            logger.info(f"Market stats updated:{get_table_length('marketstats')} items")
            return True, market_stats_df
        else:
            logger.error("Failed to update market stats")
            return False, None
    except Exception as e:
        logger.error(f"Failed to update market stats: {e}")
        return False, None

def process_doctrine_stats(remote: bool = True):
    logger.info("Calculating doctrines stats")
    logger.info("syncing database")
    db = DatabaseConfig("wcmkt")
    if remote:
        db.sync()
    logger.info("database synced")
    logger.info("validating database")
    if remote:
        validation_test = db.validate_sync()
    else:
        validation_test = True
    if validation_test:
        logger.info("database validated")
    else:
        logger.error("database validation failed")
        raise Exception("database validation failed in doctrines stats")

    doctrine_stats_df = calculate_doctrine_stats()
    doctrine_stats_df = convert_datetime_columns(doctrine_stats_df, ["timestamp"])


    status = upsert_database(Doctrines, doctrine_stats_df)


    if status:
        log_update("doctrines",remote=True)
        logger.info(f"Doctrines updated:{get_table_length('doctrines')} items")
        return True, doctrine_stats_df
    else:
        logger.error("Failed to update doctrines")
        return False, None


def process_gsheets(data: pd.DataFrame, sheet_name: str = 'market_data'):
    logger.info("Updating Google Sheets")
    gs_config = GoogleSheetConfig()
    status = gs_config.update_sheet(data, sheet_name=sheet_name)
    if status:
        logger.info("Google Sheets updated")
    else:
        logger.error("Failed to update Google Sheets")
        return False
    return True

def main(history: bool = False, remote: bool = True):
    """Main function to process market orders, history, market stats, and doctrines"""
    # Accept flags when invoked via console_script entrypoint
    if "--local" in sys.argv:
        remote = False

    if "--check_tables" in sys.argv:
        check_tables()
        return

    # Handle parse-items command
    if "parse-items" in sys.argv:
        input_file = None
        output_file = None

        for arg in sys.argv:
            if arg.startswith("--input="):
                input_file = arg.split("=", 1)[1]
            elif arg.startswith("--output="):
                output_file = arg.split("=", 1)[1]

        if not input_file or not output_file:
            print("Error: Both --input and --output parameters are required for parse-items command")
            print("Usage: mkts-backend parse-items --input=structure_data.txt --output=market_prices.csv")
            return

        success = parse_items(input_file, output_file)
        if success:
            print("Parse items command completed successfully")
        else:
            print("Parse items command failed")
        return

    # Handle add_watchlist command
    if "add_watchlist" in sys.argv:
        # Find the --type_id parameter
        type_ids_str = None
        for i, arg in enumerate(sys.argv):
            if arg.startswith("--type_id="):
                type_ids_str = arg.split("=", 1)[1]
                break

        if not type_ids_str:
            print("Error: --type_id parameter is required for add_watchlist command")
            print("Usage: mkts-backend add_watchlist --type_id=12345,67890,11111")
            print("       mkts-backend add_watchlist --type_id=12345,67890,11111 --local")
            return

        # Default to remote database, use --local flag for local database


        success = process_add_watchlist(type_ids_str, remote=remote)
        if success:
            print("Add watchlist command completed successfully")
        else:
            print("Add watchlist command failed")
        return

    if "--history" in sys.argv or "--include-history" in sys.argv:
        history = True
    start_time = time.perf_counter()
    logger.info(f"sys.argv: {sys.argv}")
    logger.info(f"history: {history}")
    logger.info("=" * 80)
    init_databases()
    logger.info("Databases initialized")


    esi = ESIConfig("primary")
    db = DatabaseConfig("wcmkt")
    logger.info(f"Database: {db.alias}")
    if remote:
        logger.info("Remote update mode. Validating database")
        validation_test = db.validate_sync()
    else:
        logger.info("Local update mode. Database not validated")
        validation_test = True

    if not validation_test:
        logger.warning("wcmkt database is not up to date. Updating...")
        if remote:
            db.sync()
            logger.info("database synced")
        else:
            logger.info("database not synced")
        
    print("=" * 80)
    print("Fetching market orders")
    print("=" * 80)
    status = process_market_orders(esi, order_type="all", test_mode=False, remote=remote)
    if status:
        logger.info("Market orders updated")
    else:
        logger.error("Failed to update market orders")
        exit()

    logger.info("=" * 80)

    watchlist = db.get_watchlist()
    if len(watchlist) > 0:
        logger.info(f"Watchlist found: {len(watchlist)} items")
    else:
        logger.error("No watchlist found. Unable to proceed further.")
  

    if history:
        logger.info("Processing history ")
        status = process_history()
        if status:
            logger.info("History updated")
        else:
            logger.error("Failed to update history")


        # TODO: Uncomment this when ready to use Jita history
        # jita_status = process_jita_history()
        # if jita_status:
        #     logger.info("Jita history updated")
        # else:
        #     logger.error("Failed to update Jita history")

    else:
        logger.info("History mode disabled. Skipping history processing")

    status, market_stats_df = process_market_stats(remote=remote)
    if status:
        logger.info("Market stats updated")
    else:
        logger.error("Failed to update market stats")
        exit()

    status, doctrine_stats_df = process_doctrine_stats(remote=remote)
    if status:
        logger.info("Doctrines updated")
    else:
        logger.error("Failed to update doctrines")
        exit()

    gsheets_status = {}

    if market_stats_df is not None:
        process_gsheets(market_stats_df, sheet_name='market_data')
        gsheets_status['market_data'] = "success"
    else:
        logger.error("Failed to update market stats in Google Sheets")
        gsheets_status['market_data'] = "failed"

    if doctrine_stats_df is not None:
        process_gsheets(doctrine_stats_df, sheet_name='doctrines_mkt')
        gsheets_status['doctrines_mkt'] = "success"
    else:
        logger.error("Failed to update doctrines in Google Sheets")
        gsheets_status['doctrines_mkt'] = "failed"

    logger.info(f"Google Sheets status: {gsheets_status}")

    logger.info("=" * 80)
    logger.info(f"Market job complete in {time.perf_counter()-start_time:.1f}s")
    for key, value in gsheets_status.items():
        logger.info(f"{key}: {value}")
    logger.info("=" * 80)


if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Starting mkts-backend")
    logger.info("=" * 80 + "\n")
    include_history = False

    if len(sys.argv) > 1:
        if "--history" in sys.argv:
            include_history = True
        elif "--check_tables" in sys.argv:
            check_tables()
            exit()
        elif "parse-items" in sys.argv:
            # Handle parse-items command in __main__ section
            input_file = None
            output_file = None

            for arg in sys.argv:
                if arg.startswith("--input="):
                    input_file = arg.split("=", 1)[1]
                elif arg.startswith("--output="):
                    output_file = arg.split("=", 1)[1]

            if not input_file or not output_file:
                print("Error: Both --input and --output parameters are required for parse-items command")
                print("Usage: mkts-backend parse-items --input=structure_data.txt --output=market_prices.csv")
                exit()

            success = parse_items(input_file, output_file)
            if success:
                print("Parse items command completed successfully")
            else:
                print("Parse items command failed")
            exit()
        elif "add_watchlist" in sys.argv:
            # Handle add_watchlist command in __main__ section too
            type_ids_str = None
            for i, arg in enumerate(sys.argv):
                if arg.startswith("--type_id="):
                    type_ids_str = arg.split("=", 1)[1]
                    break

            if not type_ids_str:
                print("Error: --type_id parameter is required for add_watchlist command")
                print("Usage: mkts-backend add_watchlist --type_id=12345,67890,11111")
                print("       mkts-backend add_watchlist --type_id=12345,67890,11111 --local")
                exit()

            # Default to remote database, use --local flag for local database
            remote = "--local" not in sys.argv
            success = process_add_watchlist(type_ids_str, remote=False)
            if success:
                print("Add watchlist command completed successfully")
            else:
                print("Add watchlist command failed")
            exit()
        elif "--help" in sys.argv:
            display_cli_help()
            exit()

        else:
            display_cli_help()
            exit()

    t0 = time.perf_counter()
    main(history=include_history)
    logger.info(f"Main function completed in {time.perf_counter()-t0:.1f}s")