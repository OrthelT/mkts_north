import pandas as pd
from sqlalchemy import select, insert, func, or_, delete
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from dotenv import load_dotenv
from datetime import datetime, timezone
import time

from mkts_backend.utils.utils import (
    add_timestamp,
    add_autoincrement,
    validate_columns,
    convert_datetime_columns,
    get_type_names_from_df,
)
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.models import Base, MarketHistory, MarketOrders, RegionOrders, UpdateLog, JitaHistory
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.db.db_queries import get_table_length, get_remote_status
from mkts_backend.esi.esi_requests import fetch_region_orders

load_dotenv()
logger = configure_logging(__name__)

db = DatabaseConfig("wcmkt")
sde_db = DatabaseConfig("sde")


def upsert_database(table: Base, df: pd.DataFrame) -> bool:
    WIPE_REPLACE_TABLES = ["marketstats", "doctrines"]
    tabname = table.__tablename__
    is_wipe_replace = tabname in WIPE_REPLACE_TABLES
    logger.info(f"Processing table: {tabname}, wipe_replace: {is_wipe_replace}")
    logger.info(f"Upserting {len(df)} rows into {table.__tablename__}")
    data = df.to_dict(orient="records")

    MAX_PARAMETER_BYTES = 256 * 1024
    BYTES_PER_PARAMETER = 8
    MAX_PARAMETERS = MAX_PARAMETER_BYTES // BYTES_PER_PARAMETER

    column_count = len(df.columns)
    chunk_size = min(2000, MAX_PARAMETERS // column_count)

    logger.info(
        f"Table {table.__tablename__} has {column_count} columns, using chunk size {chunk_size}"
    )

    db = DatabaseConfig("wcmkt")
    logger.info(f"updating: {db}")

    remote_engine = db.remote_engine
    session = Session(bind=remote_engine)

    t = table.__table__
    pk_cols = list(t.primary_key.columns)
    # Handle both single and composite primary keys
    if len(pk_cols) == 1:
        pk_col = pk_cols[0]
    elif len(pk_cols) > 1:
        pk_col = pk_cols  # Use all primary key columns for composite keys
    else:
        raise ValueError("Table must have at least one primary key column.")

    try:
        logger.info(f"Upserting {len(data)} rows into {table.__tablename__}")
        with session.begin():

            if is_wipe_replace:
                logger.info(
                    f"Wiping and replacing {len(data)} rows into {table.__tablename__}"
                )
                session.query(table).delete()
                logger.info(f"Wiped data from {table.__tablename__}")

                for idx in range(0, len(data), chunk_size):
                    chunk = data[idx : idx + chunk_size]
                    stmt = insert(t).values(chunk)
                    session.execute(stmt)
                    logger.info(
                        f"  • chunk {idx // chunk_size + 1}, {len(chunk)} rows"
                    )

                count = session.execute(select(func.count()).select_from(t)).scalar_one()
                if count != len(data):
                    raise RuntimeError(
                        f"Row count mismatch: expected {len(data)}, got {count}"
                    )
            else:
                # Delete records not present in incoming data (stale records)
                deleted_count = 0
                if isinstance(pk_col, list):
                    # Composite primary key - build list of tuples
                    incoming_pks = [tuple(row[col.name] for col in pk_col) for row in data]
                    # For composite keys, build a condition for each tuple
                    delete_conditions = []
                    for pk_tuple in incoming_pks:
                        tuple_conditions = [pk_cols[i] == pk_tuple[i] for i in range(len(pk_cols))]
                        delete_conditions.append(tuple_conditions)
                    # Delete where NOT IN incoming PKs (complex for composite, skip for now)
                    logger.warning(f"Stale record deletion not yet implemented for composite primary keys in {tabname}")
                else:
                    # Single primary key - delete records not in incoming data
                    incoming_pks = [row[pk_col.name] for row in data]
                    delete_stmt = delete(t).where(pk_col.notin_(incoming_pks))
                    delete_result = session.execute(delete_stmt)
                    deleted_count = delete_result.rowcount
                    if deleted_count > 0:
                        logger.info(f"Deleted {deleted_count} stale records from {tabname}")

                non_pk_cols = [c for c in t.columns if c not in pk_cols]
                # Exclude timestamp columns from change detection to avoid unnecessary updates
                data_cols = [c for c in non_pk_cols if c.name not in ['timestamp', 'last_update', 'created_at', 'updated_at']]

                total_updated = 0
                total_skipped = 0
                total_inserted = 0

                for idx in range(0, len(data), chunk_size):
                    chunk = data[idx : idx + chunk_size]
                    base = sqlite_insert(t).values(chunk)
                    excluded = base.excluded
                    set_mapping = {c.name: excluded[c.name] for c in non_pk_cols}

                    # Only check for changes in data columns (exclude timestamp fields)
                    if data_cols:
                        changed_pred = or_(*[c.is_distinct_from(excluded[c.name]) for c in data_cols])
                    else:
                        # If no data columns to check, always update (shouldn't happen in practice)
                        changed_pred = True

                    # Handle both single and composite primary keys for conflict resolution
                    if isinstance(pk_col, list):
                        # Composite primary key
                        stmt = base.on_conflict_do_update(
                            index_elements=pk_col, set_=set_mapping, where=changed_pred
                        )
                    else:
                        # Single primary key
                        stmt = base.on_conflict_do_update(
                            index_elements=[pk_col], set_=set_mapping, where=changed_pred
                        )

                    result = session.execute(stmt)
                    # Count affected rows (updated + inserted)
                    chunk_affected = result.rowcount
                    chunk_updated = min(chunk_affected, len(chunk))  # Approximate updates
                    chunk_skipped = len(chunk) - chunk_updated

                    total_updated += chunk_updated
                    total_skipped += chunk_skipped

                    print(f"\r upserting {table.__tablename__}. {round(100*(idx/len(data)),3)}%", end="", flush=True)

                # Calculate insertions: total incoming minus those that already existed
                count_after = session.execute(select(func.count()).select_from(t)).scalar_one()
                count_before = count_after - (deleted_count if not isinstance(pk_col, list) else 0)
                total_inserted = max(0, len(data) - (count_before - (deleted_count if not isinstance(pk_col, list) else 0)))

                if deleted_count > 0:
                    logger.info(f"Upsert summary for {table.__tablename__}: {deleted_count} rows deleted, {total_inserted} rows inserted, {total_updated} rows updated, {total_skipped} rows skipped (no data changes)")
                else:
                    logger.info(f"Upsert summary for {table.__tablename__}: {total_inserted} rows inserted, {total_updated} rows updated, {total_skipped} rows skipped (no data changes)")
            # Calculate distinct incoming records based on primary key type
            if isinstance(pk_col, list):
                # Composite primary key - create tuples of all pk column values
                distinct_incoming = len({tuple(row[col.name] for col in pk_col) for row in data})
                pk_desc = f"composite key ({', '.join(col.name for col in pk_col)})"
            else:
                # Single primary key
                distinct_incoming = len({row[pk_col.name] for row in data})
                pk_desc = f"{pk_col.name}"

            logger.info(f"distinct incoming: {distinct_incoming}")
            count = session.execute(select(func.count()).select_from(t)).scalar_one()
            logger.info(f"count: {count}")
            if count < distinct_incoming:
                logger.error(
                    f"Row count too low: expected at least {distinct_incoming} unique {pk_desc}s, got {count}"
                )
                raise RuntimeError(
                    f"Row count too low: expected at least {distinct_incoming} unique {pk_desc}s, got {count}"
                )

        logger.info(f"Upsert complete: {count} rows present in {table.__tablename__}")

    except SQLAlchemyError as e:
        logger.error("Failed upserting remote DB", exc_info=e)
        raise e
    finally:
        session.close()
        remote_engine.dispose()
    return True

def update_history(history_results: list[dict]):
    valid_history_columns = MarketHistory.__table__.columns.keys()

    flattened_history = []
    for result in history_results:
        # Handle new format: {"type_id": type_id, "data": [...]}
        if isinstance(result, dict) and "type_id" in result and "data" in result:
            type_id = result["type_id"]
            type_history = result["data"]
        else:
            # Fallback for old format - this shouldn't happen anymore
            logger.warning("Received unexpected history result format")
            continue

        if isinstance(type_history, list):
            for record in type_history:
                record['type_id'] = str(type_id)
                flattened_history.append(record)
        else:
            type_history['type_id'] = str(type_id)
            flattened_history.append(type_history)

    if not flattened_history:
        logger.error("No history data to process")
        return False

    history_df = pd.DataFrame.from_records(flattened_history)
    logger.info(f"Available columns: {list(history_df.columns)}")
    logger.info(f"Expected columns: {list(valid_history_columns)}")

    # Get type names efficiently with bulk lookup
    from mkts_backend.utils.utils import sde_db
    import sqlalchemy as sa
    from sqlalchemy import text

    unique_type_ids = history_df['type_id'].unique()

    engine = sa.create_engine(sde_db.url)
    with engine.connect() as conn:
        placeholders = ','.join([':type_id_' + str(i) for i in range(len(unique_type_ids))])
        params = {'type_id_' + str(i): int(unique_type_ids[i]) for i in range(len(unique_type_ids))}

        stmt = text(f"SELECT typeID, typeName FROM inv_info WHERE typeID IN ({placeholders})")
        res = conn.execute(stmt, params)
        type_name_map = dict(res.fetchall())
    engine.dispose()

    history_df['type_name'] = history_df['type_id'].map(lambda x: type_name_map.get(int(x), f'Unknown_{x}'))

    missing_columns = set(valid_history_columns) - set(history_df.columns)
    if missing_columns:
        logger.warning(f"Missing required columns: {missing_columns}")
        for col in missing_columns:
            if col in ('timestamp',):
                continue
            else:
                history_df[col] = 0

    history_df = add_timestamp(history_df)
    history_df = validate_columns(history_df, valid_history_columns)
    history_df = convert_datetime_columns(history_df, ['date'])
    history_df.infer_objects()
    history_df.fillna(0)

    try:
        upsert_database(MarketHistory, history_df)
    except Exception as e:
        logger.error(f"history data update failed: {e}")
        return False

    status = get_remote_status()['market_history']
    if status > 0:
        logger.info(f"History updated:{get_table_length('market_history')} items")
        print(f"History updated:{get_table_length('market_history')} items")
    else:
        logger.error("Failed to update market history")
        return False
    return True


def update_market_orders(orders: list[dict]) -> bool:
    orders_df = pd.DataFrame.from_records(orders)
    type_names = get_type_names_from_df(orders_df)
    orders_df = orders_df.merge(type_names, on="type_id", how="left")

    orders_df = convert_datetime_columns(orders_df, ['issued'])
    orders_df = add_timestamp(orders_df)
    orders_df = orders_df.infer_objects()
    orders_df = orders_df.fillna(0)
    orders_df = add_autoincrement(orders_df)

    valid_columns = MarketOrders.__table__.columns.keys()
    orders_df = validate_columns(orders_df, valid_columns)

    logger.info(f"Orders fetched:{len(orders_df)} items")
    status = upsert_database(MarketOrders, orders_df)
    if status:
        logger.info(f"Orders updated:{get_table_length('marketorders')} items")
        return True
    else:
        logger.error("Failed to update market orders")
        return False


def update_region_orders(region_id: int, order_type: str = 'sell') -> pd.DataFrame:
    orders = fetch_region_orders(region_id, order_type)
    engine = DatabaseConfig("wcmkt").engine
    session = Session(bind=engine)

    session.query(RegionOrders).delete()
    session.commit()
    session.expunge_all()
    session.close()
    time.sleep(1)
    session = Session(bind=engine)

    for order_data in orders:
        region_order = RegionOrders(
            order_id=order_data['order_id'],
            duration=order_data['duration'],
            is_buy_order=order_data['is_buy_order'],
            issued=datetime.fromisoformat(order_data['issued'].replace('Z', '+00:00')),
            location_id=order_data['location_id'],
            min_volume=order_data['min_volume'],
            price=order_data['price'],
            range=order_data['range'],
            system_id=order_data['system_id'],
            type_id=order_data['type_id'],
            volume_remain=order_data['volume_remain'],
            volume_total=order_data['volume_total']
        )
        session.add(region_order)

    session.commit()
    session.close()

    return pd.DataFrame(orders)

def update_jita_history(jita_records: list[JitaHistory]) -> bool:
    """Update JitaHistory table with Jita history data"""
    if not jita_records:
        logger.error("No Jita history data to process")
        return False

    # Convert JitaHistory objects to DataFrame
    records_data = []
    for record in jita_records:
        records_data.append({
            'date': record.date,
            'type_name': record.type_name,
            'type_id': record.type_id,
            'average': record.average,
            'volume': record.volume,
            'highest': record.highest,
            'lowest': record.lowest,
            'order_count': record.order_count,
            'timestamp': record.timestamp
        })

    jita_df = pd.DataFrame.from_records(records_data)

    valid_columns = JitaHistory.__table__.columns.keys()
    jita_df = validate_columns(jita_df, valid_columns)

    try:
        upsert_database(JitaHistory, jita_df)
        logger.info(f"Jita history updated: {len(jita_records)} records")
        return True
    except Exception as e:
        logger.error(f"Jita history update failed: {e}")
        return False

def log_update(table_name: str, remote: bool = False):
    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine if remote else db.engine

    session = Session(bind=engine)
    with session.begin():
        session.execute(delete(UpdateLog).where(UpdateLog.table_name == table_name))
        session.add(UpdateLog(table_name=table_name,timestamp=datetime.now(timezone.utc)))
        session.commit()
        session.close()

    engine.dispose()
    return True

if __name__ == "__main__":
    pass
