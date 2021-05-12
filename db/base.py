import configparser
import os

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

"""
A module for the database initializaion
"""

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'db_config.ini'))

user = config['database_connection']['user']
password = config['database_connection']['password']
hostname = config['database_connection']['hostname']
database_name = config['database_connection']['database_name']

engine = create_engine(f'postgresql://{user}:{password}@{hostname}/{database_name}',
                       pool_size=25,
                       max_overflow=20)

Session = sessionmaker(bind=engine)

# establish the ORM
Base = declarative_base()
