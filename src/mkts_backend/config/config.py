import os
from sqlalchemy import create_engine, text
import pandas as pd
import pathlib
#os.environ.setdefault("RUST_LOG", "debug")
import libsql
from dotenv import load_dotenv
from mkts_backend.config.logging_config import configure_logging
from datetime import datetime
load_dotenv()

logger = configure_logging(__name__)

class DatabaseConfig:
    wcdbmap = "wcnorth" #select wcmkt2 (production), wcnorth (war staging), wcmkt3 (development)

    _db_paths = {
        "wcnorth": "wcmktnorth2.db",
        "sde": "sde_info.db",
        "fittings": "wcfitting.db",
        "wcmkt": "wcmkt2.db",
    }

    _db_turso_urls = {
        "wcnorth_turso": os.getenv("wcmktnorth_url"),
        "sde_turso": os.getenv("TURSO_SDE_URL"),
        "fittings_turso": os.getenv("TURSO_FITTING_URL"),
        "wcmkt_turso": os.getenv("wcmkt2_url"),
    }

    _db_turso_auth_tokens = {
        "wcnorth_turso": os.getenv("wcmktnorth_token"),
        "sde_turso": os.getenv("TURSO_SDE_TOKEN"),
        "fittings_turso": os.getenv("TURSO_FITTING_TOKEN"),
        "wcmkt_turso": os.getenv("wcmkt2_token"),
    }

    def __init__(self, alias: str, dialect: str = "sqlite+libsql"):
        if alias == "wcmkt":
            alias = self.wcdbmap
        elif alias == "wcmkt3" or alias == "wcmkt2":
            logger.warning(
                f"Database alias '{alias}' is deprecated. Configure wcdbmap in config.py to select wcmkt2 or wcmkt3 instead."
            )

        if alias not in self._db_paths:
            raise ValueError(
                f"Unknown database alias '{alias}'. Available: {list(self._db_paths.keys())}"
            )

        self.alias = alias
        self.path = self._db_paths[alias]
        self.url = f"{dialect}:///{self.path}"
        self.turso_url = self._db_turso_urls[f"{self.alias}_turso"]
        self.token = self._db_turso_auth_tokens[f"{self.alias}_turso"]
        self._engine = None
        self._remote_engine = None
        self._libsql_connect = None
        self._libsql_sync_connect = None
        self._sqlite_local_connect = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(self.url)
        return self._engine

    @property
    def remote_engine(self):
        if self._remote_engine is None:
            turso_url = self._db_turso_urls[f"{self.alias}_turso"]
            auth_token = self._db_turso_auth_tokens[f"{self.alias}_turso"]
            self._remote_engine = create_engine(
                f"sqlite+{turso_url}?secure=true",
                connect_args={
                    "auth_token": auth_token,
                },
            )
        return self._remote_engine

    @property
    def libsql_local_connect(self):
        if self._libsql_connect is None:
            self._libsql_connect = libsql.connect(self.path)
        return self._libsql_connect

    @property
    def libsql_sync_connect(self):
        logger.info(f"Connecting to libsql sync: path={self.path}, url={self.turso_url}, token={self.token[:10]}...")
        self._libsql_sync_connect = libsql.connect(
                f"{self.path}", sync_url=self.turso_url, auth_token=self.token
            )
        return self._libsql_sync_connect

    @property
    def sqlite_local_connect(self):
        if self._sqlite_local_connect is None:
            self._sqlite_local_connect = libsql.connect(self.path)
        return self._sqlite_local_connect

    def sync(self):
        conn = self.libsql_sync_connect
        logger.info(f"Syncing database: alias={self.alias}, path={self.path}")
        with conn:
            conn.sync()
        conn.close()

    def validate_sync(self) -> bool:
        with self.remote_engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(last_update) FROM marketstats")).fetchone()
            remote_last_update = result[0]
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(last_update) FROM marketstats")).fetchone()
            local_last_update = result[0]

        # Convert string timestamps to datetime objects for proper comparison
        if isinstance(remote_last_update, str):
            remote_dt = datetime.fromisoformat(remote_last_update)
        else:
            remote_dt = remote_last_update

        if isinstance(local_last_update, str):
            local_dt = datetime.fromisoformat(local_last_update)
        else:
            local_dt = local_last_update

        logger.info(f"remote_last_update: {remote_dt}, {type(remote_dt)}")
        logger.info(f"local_last_update: {local_dt}")

        # Sync is valid if local is up-to-date (equal or newer than remote)
        # Only fail if remote is MORE RECENT than local
        if local_dt >= remote_dt:
            return True
        else:
            logger.warning(f"Sync validation failed: remote ({remote_dt}) is more recent than local ({local_dt})")
            return False


    def get_table_list(self, local_only: bool = True) -> list[tuple]:
        if local_only:
            engine = self.engine
            with engine.connect() as conn:
                stmt = text("PRAGMA table_list")
                result = conn.execute(stmt)
                tables = result.fetchall()
                table_list = [table.name for table in tables if "sqlite" not in table.name]
                return table_list
        else:
            engine = self.remote_engine
            with engine.connect() as conn:
                stmt = text("PRAGMA table_list")
                result = conn.execute(stmt)
                tables = result.fetchall()
                table_list = [table.name for table in tables if "sqlite" not in table.name]
                return table_list

    def get_table_columns(self, table_name: str, local_only: bool = True, full_info: bool = False) -> list[dict]:
        if local_only:
            engine = self.engine
        else:
            engine = self.remote_engine

        with engine.connect() as conn:
            stmt = text(f"PRAGMA table_info({table_name})")
            result = conn.execute(stmt)
            columns = result.fetchall()
            if full_info:
                column_info = []
                for col in columns:
                    column_info.append(
                        {
                            "cid": col.cid,
                            "name": col.name,
                            "type": col.type,
                            "notnull": col.notnull,
                            "dflt_value": col.dflt_value,
                            "pk": col.pk,
                        }
                    )
            else:
                column_info = [col.name for col in columns]

            return column_info

    def get_table_length(self, table: str):
        with self.remote_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
            return result[0]

    def get_status(self):
        status_dict = {}
        # Get tables from remote database to avoid querying tables that don't exist
        remote_tables = self.get_table_list(local_only=False)
        for table in remote_tables:
            try:
                with self.remote_engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                    status_dict[table] = result[0]
                conn.close()
            except Exception as e:
                logger.warning(f"Failed to get status for table {table}: {e}")
                status_dict[table] = 0
        return status_dict

    def get_watchlist(self):
        engine = self.engine
        with engine.connect() as conn:
            df = pd.read_sql_table("watchlist", conn)
        conn.close()
        return df

    def verify_db_exists(self):
        path = pathlib.Path(self.path)
        if not path.exists():
            logger.error(f"Database file does not exist: {self.path}")
            self.sync()
        else:
            logger.info(f"Database file exists: {self.path}")

        return True
    def get_db_turso_url(self):
        return self.turso_url

if __name__ == "__main__":
    db = DatabaseConfig("wcmkt")


    print(db.validate_sync())

