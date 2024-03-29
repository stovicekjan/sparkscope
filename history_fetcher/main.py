from db.base import Session, engine, Base
from history_fetcher.data_fetcher import DataFetcher
import time
import logging.config
import argparse
import os
from logger.logger import SparkscopeLogger

# Set up logger
logging.config.fileConfig(os.path.join(os.path.dirname(__file__), 'logger.conf'))
logger = logging.getLogger(__name__)

# Initialize the database
Base.metadata.create_all(engine)
session = Session()

# read arguments
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--test-mode", action="store_true", help="fetch some defined number of apps from the history "
                                                                 "server, regardless of what has been processed before")
arg_parser.add_argument("--truncate", action="store_true", help="truncate the database before fetching new data")
args = arg_parser.parse_args()

if args.truncate:
    session.execute('''TRUNCATE TABLE application CASCADE''')
    logger.info("Truncated the database")
    session.commit()

start = time.time()

data_fetcher = DataFetcher(session, test_mode=args.test_mode)

try:
    app_ids = data_fetcher.fetch_all_data()
    session.commit()
    end = time.time()
    logger.info(f"""
    ====================================================================================================================
    Finished: Saved {len(app_ids)} applications metadata into database.
    List of the application_id's: {app_ids}
    Elapsed time: {(end-start):.3f} seconds
    ====================================================================================================================
    """)
except Exception as ex:
    logger.exception(f"Caught an exception: {ex}")
    session.rollback()
finally:
    session.close()






