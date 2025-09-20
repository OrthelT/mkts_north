from config import DatabaseConfig
import os
from logging_config import setup_logging
from sync_state import update_wcmkt_state


logger = setup_logging(__name__)
def verify_db_path(path):
    if not os.path.exists(path):
        logger.warning(f"DB path does not exist: {path}")
        return False
    return True

def init_db():
    """ This function checks to see if the databases are available locally. If not, it will sync the databases from the remote server using the configuration in given in the config.py file, using credentials stored in the .streamlit/secrets.toml (for local development) or st.secrets (for production). This code was designed to be used with sqlite embedded-replica databases hosted on Turso Cloud.
    """

    logger.info("-"*100)
    logger.info("initializing databases")
    logger.info("-"*100)

    mkt_db = DatabaseConfig("wcmkt")
    sde_db = DatabaseConfig("sde")
    build_cost_db = DatabaseConfig("build_cost")
    db_paths = {
        mkt_db.alias: mkt_db.path,
        sde_db.alias: sde_db.path,
        build_cost_db.alias: build_cost_db.path,
    }
    status = {}
    for key, value in db_paths.items():
        alias = key
        db_path = value
        db = DatabaseConfig(alias)
        try:
            if verify_db_path(db_path):
                logger.info(f"DB path exists: {db_path}✔️")
                status = {key: "success initialized🟢" if verify_db_path(db_path) else "failed🔴"}
            else:
                logger.warning(f"DB path does not exist: {db_path}⚠️")
                logger.info("syncing db")
                logger.info(f"syncing db: {db_path}🛜")
                db.sync()
                status = {key: "initialized and synced🟢" if verify_db_path(db_path) else "failed🔴"}
        except Exception as e:
                logger.error(f"Error syncing db: {e}")
                status = {key: "failed🔴"}
        logger.info(f"db initialization status: {key}: {status[key]}")
    logger.info("-"*100)
    logger.info("updating wcmkt state")
    logger.info("-"*100)
    update_wcmkt_state()
    logger.info("wcmkt state updated✅")
    logger.info("-"*100)
    return True

if __name__ == "__main__":
    pass