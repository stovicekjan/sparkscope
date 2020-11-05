from db.base import Session, engine, Base
from history_fetcher.data_fetcher import DataFetcher
import time
import logging.config


"""
Set up logger
"""
logging.config.fileConfig('logger.conf')
logger = logging.getLogger(__name__)

"""
Initialize the database
"""
Base.metadata.create_all(engine)
session = Session()

# TODO remove this
session.execute('''TRUNCATE TABLE executor CASCADE''')
logger.info("truncated table executor")
session.execute('''TRUNCATE TABLE application CASCADE''')
logger.info("truncated table application")
session.commit()

start = time.time()

data_fetcher = DataFetcher(session)
data_fetcher.fetch_all_data()

session.commit()
session.close()

end = time.time()

logger.info(f"Time: {end-start} sec")
