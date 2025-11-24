import datetime
from dataclasses import dataclass, field
from pickle import FALSE
from numpy._core.multiarray import scalar
from numpy.ma import count
import pandas as pd
from sqlalchemy import text, select
from sqlalchemy.orm import Session
from mkts_backend.db.models import Doctrines, LeadShips, DoctrineFit, Base
from mkts_backend.db.fit_models import WatchDoctrines
from mkts_backend.db.db_queries import get_watchlist_ids, get_fit_ids, get_fit_items
from mkts_backend.utils.get_type_info import TypeInfo
from mkts_backend.config.config import DatabaseConfig
from mkts_backend.config.logging_config import configure_logging
from mkts_backend.db.sde_models import SdeInfo
from mkts_backend.utils.add2doctrines_table import select_doctrines_table, add_fit_to_doctrine_table
from mkts_backend.utils.utils import get_type_name
from mkts_backend.utils.db_utils import add_missing_items_to_watchlist

doctrines_fields = ['id', 'fit_id', 'ship_id', 'ship_name', 'hulls', 'type_id', 'type_name', 'fit_qty', 'fits_on_mkt', 'total_stock', 'price', 'avg_vol', 'days', 'group_id', 'group_name', 'category_id', 'category_name', 'timestamp']
logger = configure_logging(__name__)

doctrine_fit_id = 494
ship_id = 33157
ship_name = 'Hurricane Fleet Issue'
ship_target = 100
doctrine_name = '2507  WC-EN Shield DPS HFI v1.0'
fit_name = '2507  WC-EN Shield DPS HFI v1.0'
ship_type_id = 33157

@dataclass
class DoctrineFitData:
    fit_id: int
    ship_id: int
    ship_name: str
    hulls: int
    type_id: int
    type_name: str
    fit_qty: int
    fits_on_mkt: float
    total_stock: int
    price: float
    avg_vol: float
    days: float
    group_id: int
    group_name: str
    category_id: int
    category_name: str
    timestamp: str = field(init=False)

    def __post_init__(self):
        self.timestamp = datetime.datetime.strftime(datetime.datetime.now(datetime.timezone.utc), '%Y-%m-%d %H:%M:%S')

@dataclass
class Doctrine:
    doctrine_id: int
    remote: bool = False
    def __post_init__(self):
        self.fits = get_fit_ids(self.doctrine_id)
    
    @property
    def all_item_ids(self)->list[int]:
        all_ids = []
        fits_dict = get_fit_dicts(self.doctrine_id, remote=self.remote)
        for fit_id, items in fits_dict.items():
            for item in items:
                all_ids.append(item["type_id"])
        return(list(set(all_ids)))

    def get_all_fit_ids(self)->list[int]:
        fits_dict = get_fit_dicts(self.doctrine_id, remote=self.remote)
        return(fits_dict.keys())

    def get_all_ships(self)->list[int]:
        all_ships = []
        for fit_id in self.get_all_fit_ids():
            ship_id = get_ship_for_fit(fit_id=fit_id, remote=self.remote)
            all_ships.append(ship_id)
        return(list(set(all_ships)))
    
    def add_fits(self):
        updated_items = []
        for fit_id in self.get_all_fit_ids():
            ship_id = get_ship_for_fit(fit_id=fit_id, remote=self.remote)
            ship_name = get_type_name(type_id=ship_id)
            print(f"Adding fit {fit_id} to doctrines table")
            print(fit_id, ship_id, ship_name)
            updated_items.append(add_fit_to_doctrine_table(fit_id=fit_id, ship_id=ship_id, ship_name=ship_name, remote=self.remote, dry_run=False))
        return updated_items

def add_ship_target(fit_id: int, target: int, remote: bool = False)->bool:
    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine if remote else db.engine
    data = []
    with engine.connect() as conn:
        stmt = text("SELECT * FROM doctrine_fits WHERE fit_id = :fit_id")
        result = conn.execute(stmt, {"fit_id": fit_id})
        data = result.fetchall()
        if len(data) > 0:
            for row in data:

                fit_name = row[2]
                ship_id = row[3]
                ship_name = row[6]
                created_at = datetime.datetime.strftime(datetime.datetime.now(datetime.timezone.utc), '%Y-%m-%d %H:%M:%S')
                print(f"fit_name: {fit_name}, ship_id: {ship_id}, ship_name: {ship_name}, created_at: {created_at}")

                stmt2 = text("""INSERT INTO ship_targets ('fit_id', 'fit_name', 'ship_id', 'ship_name', 'ship_target', 'created_at')
                VALUES (:fit_id, :fit_name, :ship_id, :ship_name, :ship_target, :created_at)""")
                insert_data = {
                    "fit_id": fit_id,
                    "fit_name": fit_name,
                    "ship_id": ship_id,
                    "ship_name": ship_name,
                    "ship_target": target,
                    "created_at": created_at
                }
                conn.execute(stmt2, insert_data)
                conn.commit()
                print(f"Ship target added for fit_name: {fit_name}, ship_id: {ship_id}, ship_name: {ship_name}")
            conn.close()
            engine.dispose()
    return True

def add_doctrine_map_from_fittings_doctrine_fittings(doctrine_id: int, remote: bool = False):
    db = DatabaseConfig("fittings")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM fittings_doctrine_fittings WHERE doctrine_id = :doctrine_id")
        df = pd.read_sql_query(stmt, conn, params={"doctrine_id": doctrine_id})
    conn.close()
    doctrine_map_db = DatabaseConfig("wcmkt")
    engine = doctrine_map_db.remote_engine if remote else doctrine_map_db.engine
    with engine.connect() as conn:
        for index, row in df.iterrows():
            stmt = text("INSERT INTO doctrine_map ('doctrine_id', 'fitting_id') VALUES (:doctrine_id, :fitting_id)")
            conn.execute(stmt, {"doctrine_id": doctrine_id, "fitting_id": row.fitting_id})
            logger.info(f"Added doctrine_map for doctrine_id: {doctrine_id}, fitting_id: {row.fitting_id}")
        conn.commit()
        print("Doctrine map added")
    conn.close()
    engine.dispose()

def get_ship_for_fit(fit_id: int, remote: bool = False)->int:
    db = DatabaseConfig("fittings")
    engine = db.engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM fittings_fitting WHERE id = :fit_id")
        result = conn.execute(stmt, {"fit_id": fit_id})
        ship_id = result.fetchone()[4]

    conn.close()
    engine.dispose()
    return ship_id

def add_hurricane_fleet_issue_to_doctrines():

    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM marketstats WHERE type_id = 33157")
        market_data = conn.execute(stmt).fetchone()
    conn.close()
    engine.dispose()

    if not market_data:
        logger.error("No market data found for Hurricane Fleet Issue (type_id 33157)")
        return False

    type_info = TypeInfo(33157)

    engine = db.remote_engine
    with engine.connect() as conn:
        stmt = text('SELECT MAX(id) as max_id FROM doctrines')
        result = conn.execute(stmt).fetchone()
        max_id = result.max_id if result.max_id else 0
        next_id = max_id + 1
        logger.info(f"Next available ID: {next_id}")
    conn.close()
    engine.dispose()

    fit_qty = 1
    hulls_on_market = market_data.total_volume_remain
    total_stock_on_market = market_data.total_volume_remain
    fits_on_mkt = total_stock_on_market / fit_qty

    stmt = text("""
        INSERT INTO doctrines (
            id, fit_id, ship_id, ship_name, hulls, type_id, type_name, fit_qty,
            fits_on_mkt, total_stock, price, avg_vol, days, group_id,
            group_name, category_id, category_name, timestamp
        ) VALUES (
            :id, :fit_id, :ship_id, :ship_name, :hulls, :type_id, :type_name, :fit_qty,
            :fits_on_mkt, :total_stock, :price, :avg_vol, :days, :group_id,
            :group_name, :category_id, :category_name, :timestamp
        )
    """)

    insert_data = {
        'id': next_id,
        'fit_id': 494,
        'ship_id': 33157,
        'ship_name': 'Hurricane Fleet Issue',
        'hulls': int(hulls_on_market),
        'type_id': 33157,
        'type_name': type_info.type_name,
        'fit_qty': fit_qty,
        'fits_on_mkt': float(fits_on_mkt),
        'total_stock': int(total_stock_on_market),
        'price': float(market_data.price),
        'avg_vol': float(market_data.avg_volume),
        'days': float(market_data.days_remaining),
        'group_id': int(type_info.group_id),
        'group_name': type_info.group_name,
        'category_id': int(type_info.category_id),
        'category_name': type_info.category_name,
        'timestamp': datetime.now(datetime.timezone.utc).isoformat()
    }

    engine = db.remote_engine
    with engine.connect() as conn:
        conn.execute(stmt, insert_data)
        conn.commit()
        logger.info("Successfully added Hurricane Fleet Issue (fit_id 494) to doctrines table")
        print("Hurricane Fleet Issue added to doctrines table successfully!")
    conn.close()
    engine.dispose()

    return True

def add_doctrine_fit(DoctrineFit: DoctrineFitData):
    db = DatabaseConfig("wcmkt")
    stmt = text("""INSERT INTO doctrines ('fit_id', 'fit_name', 'ship_id', 'ship_name', 'ship_target', 'created_at')
    VALUES (494, '2507  WC-EN Shield DPS HFI v1.0', 33157, 'Hurricane Fleet Issue', 100, '2025-07-05 00:00:00')""")
    engine = db.remote_engine
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()
        print("Fit added to doctrines table")
    conn.close()
    engine.dispose()

def add_lead_ship(lead_ship: LeadShips):
    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine
    session = Session(bind=engine)
    with session.begin():
        session.add(lead_ship)
        session.commit()
        print("Lead ship added")
    session.close()
    engine.dispose()

def process_hfi_fit_items(type_ids: list[int]) -> list[DoctrineFitData]:
    items = []
    for type_id in type_ids:
        item = DoctrineFitData(
            fit_id=494,
            ship_id=33157,
            ship_name='Hurricane Fleet Issue',
            type_id=type_id,
            type_name='Hurricane Fleet Issue',
            fit_qty=1,
            fits_on_mkt=100,
            total_stock=100,
            price=100,
            avg_vol=100,
            days=100,
            group_id=100,
            group_name='Hurricane Fleet Issue',
            category_id=100,
            category_name='Hurricane Fleet Issue'
        )
        items.append(item)
    return items

def get_fit_dicts(doctrine_id: int, remote: bool = False) -> dict[int, dict[int, int]]:
    fit_items = {}
    fits = {}
    db = DatabaseConfig("fittings")
    engine = db.remote_engine if remote else db.engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM fittings_doctrine_fittings WHERE doctrine_id = :doctrine_id")
        result = conn.execute(stmt, {"doctrine_id": doctrine_id})
        for row in result:
            fit_id = row[2]
            stmt = text("SELECT type_id FROM fittings_fittingitem WHERE fit_id = :fit_id")
            res2 = conn.execute(stmt, {"fit_id": fit_id})
            type_ids = [row[0] for row in res2]
            fit_items[fit_id] = type_ids
    conn.close()
    engine.dispose()
    for k, v in fit_items.items():
        items = []
        unique_ids = set(v)
        for id in unique_ids:
            count = v.count(id)
            items.append({"type_id": id, "count": count})
        fits[k] = items
    return fits

def add_doctrine_type_info_to_watchlist(doctrine_id: int, remote: False):
    watchlist_ids = get_watchlist_ids(remote=remote)
    fit_ids = get_fit_ids(doctrine_id)
    doctrine = Doctrine(doctrine_id=doctrine_id, remote=remote)

    missing_fit_items = []
    unique_items = []

    for fit_id in fit_ids:
        fit_items = get_fit_items(fit_id)
        for item in fit_items:
            if item not in watchlist_ids:
                missing_fit_items.append(item)
    ship_ids = doctrine.get_all_ships()
    for ship_id in ship_ids:
        if ship_id not in watchlist_ids:
            missing_fit_items.append(ship_id)
    missing_fit_items = set(missing_fit_items)
    missing_type_info = []
    logger.info(f"Adding {len(missing_fit_items)} missing items to watchlist")
    print(f"Adding {len(missing_fit_items)} missing items to watchlist")

    db = DatabaseConfig("wcmkt")
    logger.info(f"Adding {len(missing_fit_items)} missing items to watchlist to {db.alias, db.path} remote {remote} database")

    print("="*30)
    print("Missing items")
    print("="*30)

    for item in missing_fit_items:
        item_name = get_type_name(type_id=item)
        print(item_name, " ", item)
    

    continue_adding = input("Continue adding? (y/n)")
    if continue_adding == "n":
        return
    else:
        logger.info(f"Continuing to add {len(missing_fit_items)} missing items to watchlist")
        print(f"Continuing to add {len(missing_fit_items)} missing items to watchlist")
    

    for item in missing_fit_items:
        stmt4 = text("SELECT * FROM inv_info WHERE typeID = :item")
        db = DatabaseConfig("sde")
        engine = db.engine
        with engine.connect() as conn:
            result = conn.execute(stmt4, {"item": item})
            for row in result:
                type_info = TypeInfo(type_id=item)
                missing_type_info.append(type_info)

    for type_info in missing_type_info:
        stmt5 = text("INSERT INTO watchlist (type_id, type_name, group_name, category_name, category_id, group_id) VALUES (:type_id, :type_name, :group_name, :category_name, :category_id, :group_id)")
        db = DatabaseConfig("wcmkt")
        engine = db.remote_engine if remote else db.engine
    
        with engine.connect() as conn:
            conn.execute(stmt5, {"type_id": type_info.type_id, "type_name": type_info.type_name, "group_name": type_info.group_name, "category_name": type_info.category_name, "category_id": type_info.category_id, "group_id": type_info.group_id})
            conn.commit()
        conn.close()
        engine.dispose()
        logger.info(f"Added {type_info.type_name} to watchlist")
        print(f"Added {type_info.type_name} to watchlist")

def add_doctrine_fits_to_wcmkt(df: pd.DataFrame, remote: bool = False):

    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine if remote else db.engine
    print(db.alias + " " + " " + str(remote))
    session = Session(bind=engine)
    try:
        with session.begin():
            for index, row in df.iterrows():
                fit = DoctrineFit(doctrine_name=row["doctrine_name"], fit_name=row["fit_name"], ship_type_id=row["ship_type_id"], ship_name=row["ship_name"], fit_id=row["fit_id"], doctrine_id=row["doctrine_id"], target=row["target"])
                session.add(fit)
                print(f"Added {fit.fit_name} to doctrine_fits table")
    finally:
        session.close()
        engine.dispose()

def check_doctrine_fits_in_wcmkt(doctrine_id: int, remote: bool = False)->pd.DataFrame:
    db = DatabaseConfig("wcmkt")
    print(db.alias + " " + " " + str(remote))
    engine = db.remote_engine if remote else db.engine
    with engine.connect() as conn:
        stmt = text("SELECT * FROM doctrine_fits WHERE doctrine_id = :doctrine_id")
        df = pd.read_sql_query(stmt, conn, params={"doctrine_id": doctrine_id})
    return df

def reset_doctrines_table(remote: bool = False):
    wcmkt3 = DatabaseConfig("wcmkt")
    engine1 = wcmkt3.remote_engine if remote else wcmkt3.engine
    session = Session(bind=engine1)
    with session.begin():
        session.execute(text("DROP TABLE IF EXISTS doctrines"))
        session.commit()
    session.close()
    Base.metadata.create_all(engine1)
    print("Tables created")
    wcmkt2 = DatabaseConfig("wcmkt")
    engine2 = wcmkt2.remote_engine if remote else wcmkt2.engine
    stmt = "SELECT * FROM doctrines"
    with engine2.connect() as conn:
        df = pd.read_sql_query(stmt, conn)
    conn.close()
    engine2.dispose()
    with engine1.connect() as conn:
        df.to_sql("doctrines", conn, if_exists="replace", index=False)
        conn.commit()
    print(f"Added {len(df)} rows to doctrines table")
    conn.close()
    engine1.dispose()

def add_doctrine_fit_to_doctrines_table(df: pd.DataFrame, fit_id: int, ship_id: int, ship_name: str, remote: bool = False):
    db = DatabaseConfig("wcmkt")
    print(db.alias + " " + " " + str(remote))
    engine = db.remote_engine if remote else db.engine
    session = Session(bind=engine)

    with session.begin():
        for index, row in df.iterrows():
            try:
                type_name = get_type_name(row["type_id"])
            except Exception as e:
                logger.error(f"Error getting type name for {row['type_id']}: {e}")
                type_name = "Unknown"
                continue

            fit = Doctrines(fit_id=fit_id, ship_id=ship_id, ship_name=ship_name, type_id=row["type_id"], type_name=type_name, fit_qty=row["quantity"])
            session.add(fit)
            print(f"Added {fit.type_name} to doctrines table")
    session.commit()
    session.close()
    engine.dispose()
    print(f"Added {len(df)} rows to doctrines table")

def clean_doctrines_table(remote: bool = False):
    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine if remote else db.engine
    session = Session(bind=engine)
    with session.begin():
        session.execute(text("DROP TABLE IF EXISTS doctrines"))
        session.commit()
    session.close()
    Base.metadata.create_all(engine)
    engine.dispose()
    print("Tables created")

def add_doctrines_to_table(df: pd.DataFrame, remote: bool = False):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    clean_doctrines_table(remote)
    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine if remote else db.engine
    session = Session(bind=engine)
    with session.begin():
        for index, row in df.iterrows():
            fit = Doctrines(**row)
            session.add(fit)
            print(f"Added {fit.type_name} to doctrines table")
    session.commit()
    session.close()
    engine.dispose()
    print(f"Added {len(df)} rows to doctrines table")

def check_doctrines_table(remote: bool = False, fit_id: int = None):
    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine if remote else db.engine
    session = Session(bind=engine)
    row_count = 0
    type_ids = []
    with session.begin():
        if fit_id:
            result = session.scalars(select(Doctrines).where(Doctrines.fit_id == fit_id))
        else:
            result = session.scalars(select(Doctrines))
        if fit_id:
            result = session.execute(select(Doctrines).where(Doctrines.fit_id == fit_id))
        for row in result:
            type_ids.append(row[0].type_id)
    session.close()
    engine.dispose()
    return type_ids

def replace_doctrines_table(df: pd.DataFrame, remote: bool = False):
    df = df.rename(columns={"quantity": "fit_qty"})
    add_doctrines_to_table(df, remote=True)
    check_doctrines_table(remote=True)

def get_watch_doctrines(remote: bool = False):
    db = DatabaseConfig("fittings")
    engine = db.engine
    session = Session(bind=engine)
    result = {}
    with session.begin():
        doctrin_data = session.scalars(select(WatchDoctrines))
        for row in doctrin_data:
            result[row.id] = row.name
            print(row.id, row.name)
    session.close()
    engine.dispose()
    return result

def add_doctrine_info_to_doctrines_table(doctrine_id: int, remote: bool = False):
    db = DatabaseConfig("fittings")
    engine = db.engine
    df = pd.read_sql_query(text("SELECT * FROM watch_doctrines"), engine)
    engine.dispose()
    df2 = df.copy()
    df2.rename(columns={"id": "doctrine_id"}, inplace=True)
    df2.drop(columns=["icon_url", "description", "created", "last_updated"], inplace=True)
    engine.dispose()
    db = DatabaseConfig("wcmkt")
    engine = db.remote_engine if remote else db.engine
    with engine.connect() as conn:
        df2.to_sql("doctrine_info", conn, if_exists="replace", index=False)
        conn.commit()
    conn.close()
    engine.dispose()
    logger.info(f"Added {len(df2)} rows to doctrines table")

def get_doctrine_fits(doctrine_id: int, remote: bool = False) -> pd.DataFrame:

    db = DatabaseConfig("fittings")
    engine = db.remote_engine if remote else db.engine
    doctrine_name = None
    with engine.connect() as conn:
        stmt = text("SELECT fitting_id FROM fittings_doctrine_fittings WHERE doctrine_id = :doctrine_id")
        result = conn.execute(stmt, {"doctrine_id": doctrine_id})
        fit_info_list = result.fetchall()
        fit_ids = [row[0] for row in fit_info_list]
        fit_info = []
        for fit_id in fit_ids:
            stmt2 = text("SELECT name, ship_type_id, id FROM fittings_fitting WHERE id = :fit_id")
            result2 = conn.execute(stmt2, {"fit_id": fit_id})
            data = result2.fetchall()
            name = data[0][0]
            ship_type_id = data[0][1]
            fit_info.append({"fit_name": name, "ship_type_id": ship_type_id, "doctrine_id": doctrine_id, "fit_id": fit_id})
        stmt2 = text("SELECT name, id FROM fittings_doctrine WHERE id = :doctrine_id")
        result2 = conn.execute(stmt2, {"doctrine_id": doctrine_id})
        data = result2.fetchall()
        print(data)
        doctrine_name = data[0][0]
        doctrine_id = data[0][1]
            
    conn.close()
    engine.dispose()
    df = pd.DataFrame(fit_info)
    df['doctrine_name'] = doctrine_name
    df['doctrine_id'] = doctrine_id
    db = DatabaseConfig("sde")
    ship_type_ids = df["ship_type_id"].unique().tolist()
    engine = db.engine
    with engine.connect() as conn:
        for ship_type_id in ship_type_ids:
            stmt3 = text("SELECT typeid, typename as ship_name FROM inv_info WHERE typeID = :ship_type_id")
            result3 = conn.execute(stmt3, {"ship_type_id": ship_type_id})
            data = result3.fetchall()
            if len(data) > 0:
                for row in data:
                    type_id = row[0]
                    type_name = row[1]
                    df.loc[df["ship_type_id"] == ship_type_id, "ship_name"] = type_name
            else:
                logger.error(f"No data found for ship type id: {ship_type_id}")
    conn.close()
    engine.dispose()
    df2 = df.copy()
    df2 = df2[["doctrine_name", "fit_name", "ship_type_id", "doctrine_id", "fit_id", "ship_name"]]
    df3 = get_ship_target(df2)
    return df3

def get_ship_target(df: pd.DataFrame) -> pd.DataFrame:
    fit_ids = df["fit_id"].unique().tolist()
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        for fit_id in fit_ids:
            stmt = text("SELECT ship_target FROM ship_targets WHERE fit_id = :fit_id")
            result = conn.execute(stmt, {"fit_id": fit_id})
            data = result.fetchall()
            if len(data) > 0:
                for row in data:
                    target = row[0]
                    df.loc[df["fit_id"] == fit_id, "target"] = target
            else:
                logger.warning(f"No data found for fit id: {fit_id} setting target to 20")
                df.loc[df["fit_id"] == fit_id, "target"] = 20
    conn.close()
    engine.dispose()
    return df

def rebuild_doctrine_fits_table():
    db = DatabaseConfig("wcmkt")
    engine = db.engine
    with engine.connect() as conn:
        df2 = pd.read_sql_table("doctrine_fits", conn)
    conn.close()
    engine.dispose()
    engine = db.remote_engine
    with engine.connect() as conn:
        stmt = text("DROP TABLE IF EXISTS doctrine_fits")
        conn.execute(stmt)
        Base.metadata.create_all(engine)
        stmt = text("INSERT INTO doctrine_fits (doctrine_name, fit_name, ship_type_id, doctrine_id, fit_id, ship_name, target) VALUES (:doctrine_name, :fit_name, :ship_type_id, :doctrine_id, :fit_id, :ship_name, :target)")
        for index, row in df2.iterrows():
            conn.execute(stmt, {"doctrine_name": row["doctrine_name"], "fit_name": row["fit_name"], "ship_type_id": row["ship_type_id"], "doctrine_id": row["doctrine_id"], "fit_id": row["fit_id"], "ship_name": row["ship_name"], "target": row["target"]})
        conn.commit()
    
    conn.close()
    engine.dispose()
    print("Doctrine fits table rebuilt")


if __name__ == "__main__":
    pass

