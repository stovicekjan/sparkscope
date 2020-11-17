# coding=utf-8

from sqlalchemy import Column, String, DateTime, BigInteger, Boolean, JSON
from db.base import Base


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

