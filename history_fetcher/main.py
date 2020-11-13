from db.base import Session, engine, Base
from history_fetcher.data_fetcher import DataFetcher
import time
import logging.config
import argparse
import os
from logger.logger import SparkscopeLogger

"""
Set up logger
"""
logging.config.fileConfig(os.path.join(os.path.dirname(__file__), 'logger.conf'))
# logger = logging.getLogger(__name__)
logger = SparkscopeLogger(__name__)

"""
Initialize the database
"""
Base.metadata.create_all(engine)
session = Session()

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
data_fetcher.fetch_all_data()

session.commit()
session.close()

end = time.time()

logger.info(f"Time: {end-start} sec")
