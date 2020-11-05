import configparser

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

config = configparser.ConfigParser()
config.read('config.ini')

user = config['database_connection']['user']
password = config['database_connection']['password']
hostname = config['database_connection']['hostname']
database_name = config['database_connection']['database_name']

engine = create_engine(f'postgresql://{user}:{password}@{hostname}/{database_name}')

Session = sessionmaker(bind=engine)

Base = declarative_base()
