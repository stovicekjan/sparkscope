# coding=utf-8

from sqlalchemy import Column, String, DateTime, BigInteger, Boolean, JSON
from db.base import Base
from history_fetcher.utils import get_prop, get_system_property


class Application(Base):
    """
    A class used to represent the Application entity in the database.

    Each Spark application should be represented by one record in the Application table.
    """
    __tablename__ = 'application'

    app_id = Column(String, primary_key=True)
    name = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    duration = Column(BigInteger)
    spark_user = Column(String)
    completed = Column(Boolean)
    runtime = Column(JSON)
    spark_properties = Column(JSON)
    spark_command = Column(String)
    mode = Column(String)

    def __init__(self, attributes):
        """
        Create an Application object.
        :param attributes: dictionary {name: value} containing the attributes
        """
        self.app_id = attributes['app_id']
        self.name = attributes['name']
        self.start_time = attributes['start_time']
        self.end_time = attributes['end_time']
        self.duration = attributes['duration']
        self.spark_user = attributes['spark_user']
        self.completed = attributes['completed']
        self.runtime = attributes['runtime']
        self.spark_properties = attributes['spark_properties']
        self.spark_command = attributes['spark_command']
        self.mode = attributes['mode']

    @staticmethod
    def get_fetch_dict(app, app_env_data):
        return {
            'app_id': app['id'],
            'name': app['name'],
            'start_time': app['attempts'][0]['startTime'],
            'end_time': app['attempts'][0]['endTime'],
            'duration': app['attempts'][0]['duration'],
            'spark_user': app['attempts'][0]['sparkUser'],
            'completed': app['attempts'][0]['completed'],
            'runtime': get_prop(app_env_data, 'runtime'),
            'spark_properties': get_prop(app_env_data, 'sparkProperties'),
            'spark_command': get_system_property(app_env_data, app['id'], 'sun.java.command'),
            'mode': app['mode']
        }
