# coding=utf-8

from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey
from sqlalchemy.orm import relationship

from db.base import Base

class Executor(Base):
    __tablename__ = 'executor'

    executor_key = Column(String, primary_key=True)
    app = relationship("Application")
    app_id = Column(String, ForeignKey('application.app_id'))
    id = Column(String)
    host_port = Column(String)
    is_active = Column(Boolean)
    rdd_blocks = Column(Integer)
    memory_used = Column(Integer)
    disk_used = Column(Integer)
    total_cores = Column(Integer)
    max_tasks = Column(Integer)
    active_tasks = Column(Integer)
    failed_tasks = Column(Integer)
    total_duration = Column(BigInteger)
    total_gc_time = Column(BigInteger)
    total_input_bytes = Column(BigInteger)
    total_shuffle_read = Column(BigInteger)
    total_shuffle_write = Column(BigInteger)
    is_blacklisted = Column(Boolean)
    max_memory = Column(BigInteger)
    add_time = Column(DateTime)
    executor_stdout_log = Column(String)
    executor_stderr_log = Column(String)
    used_on_heap_storage_memory = Column(String)
    used_off_heap_storage_memory = Column(String)
    total_on_heap_storage_memory = Column(String)
    total_off_heap_storage_memory = Column(String)
    blacklisted_in_stages = Column(String)  # TODO implement this as a relationship to stage?


    def __init__(self, attributes):
        self.executor_key = attributes["executor_key"]
        self.app_id = attributes["app_id"]
        self.id = attributes["id"]
        self.host_port = attributes["host_port"]
        self.is_active = attributes["is_active"]
        self.rdd_blocks = attributes["rdd_blocks"]
        self.memory_used = attributes["memory_used"]
        self.disk_used = attributes["disk_used"]
        self.total_cores = attributes["total_cores"]
        self.max_tasks = attributes["max_tasks"]
        self.active_tasks = attributes["active_tasks"]
        self.failed_tasks = attributes["failed_tasks"]
        self.total_duration = attributes["total_duration"]
        self.total_gc_time = attributes["total_gc_time"]
        self.total_input_bytes = attributes["total_input_bytes"]
        self.total_shuffle_read = attributes["total_shuffle_read"]
        self.total_shuffle_write = attributes["total_shuffle_write"]
        self.is_blacklisted = attributes["is_blacklisted"]
        self.max_memory = attributes["max_memory"]
        self.add_time = attributes["add_time"]
        self.executor_stdout_log = attributes["executor_stdout_log"]
        self.executor_stderr_log = attributes["executor_stderr_log"]
        self.used_on_heap_storage_memory = attributes["used_on_heap_storage_memory"]
        self.used_off_heap_storage_memory = attributes["used_off_heap_storage_memory"]
        self.total_on_heap_storage_memory = attributes["total_on_heap_storage_memory"]
        self.total_off_heap_storage_memory = attributes["total_off_heap_storage_memory"]
        self.blacklisted_in_stages = attributes["blacklisted_in_stages"]


