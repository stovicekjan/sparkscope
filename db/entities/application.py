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
        self.app_id = get_prop(attributes, 'app_id')
        self.name = get_prop(attributes, 'name')
        self.start_time = get_prop(attributes, 'start_time')
        self.end_time = get_prop(attributes, 'end_time')
        self.duration = get_prop(attributes, 'duration')
        self.spark_user = get_prop(attributes, 'spark_user')
        self.completed = get_prop(attributes, 'completed')
        self.runtime = get_prop(attributes, 'runtime')
        self.spark_properties = get_prop(attributes, 'spark_properties')
        self.spark_command = get_prop(attributes, 'spark_command')
        self.mode = get_prop(attributes, 'mode')

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
