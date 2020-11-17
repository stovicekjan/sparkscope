# coding=utf-8

from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey, Float
from sqlalchemy.orm import relationship

from db.base import Base


class Task(Base):
    """
    A class used to represent the Task entity in the database.

    Each task within each stage within each Spark application should be represented by one record in the Task table.
    """
    __tablename__ = 'task'

    task_key = Column(String, primary_key=True)
    stage_key = Column(String, ForeignKey('stage.stage_key'))
    stage = relationship("Stage")
    task_id = Column(Integer)
    index = Column(Integer)
    attempt = Column(Integer)
    launch_time = Column(DateTime)
    duration = Column(BigInteger)
    executor_key = Column(String, ForeignKey('executor.executor_key'))
    executor = relationship("Executor")
    host = Column(String)
    status = Column(String)
    task_locality = Column(String)
    speculative = Column(Boolean)
    accumulator_updates = Column(ARRAY(String))
    executor_deserialize_time = Column(BigInteger)
    executor_deserialize_cpu_time = Column(BigInteger)
    executor_run_time = Column(BigInteger)
    executor_cpu_time = Column(BigInteger)
    result_size = Column(BigInteger)
    jvm_gc_time = Column(BigInteger)
    result_serialization_time = Column(BigInteger)
    memory_bytes_spilled = Column(BigInteger)
    disk_bytes_spilled = Column(BigInteger)
    peak_execution_memory = Column(BigInteger)
    bytes_read = Column(BigInteger)
    records_read = Column(BigInteger)
    bytes_written = Column(BigInteger)
    records_written = Column(BigInteger)
    shuffle_remote_blocks_fetched = Column(BigInteger)
    shuffle_local_blocks_fetched = Column(BigInteger)
    shuffle_fetch_wait_time = Column(BigInteger)
    shuffle_remote_bytes_read = Column(BigInteger)
    shuffle_remote_bytes_read_to_disk = Column(BigInteger)
    shuffle_local_bytes_read = Column(BigInteger)
    shuffle_records_read = Column(BigInteger)
    shuffle_bytes_written = Column(BigInteger)
    shuffle_write_time = Column(BigInteger)
    shuffle_records_written = Column(BigInteger)

    def __init__(self, attributes):
        """
        Create an Executor object.
        :param attributes: dictionary {name: value} containing the attributes
        """
        self.task_key = attributes['task_key']
        self.stage_key = attributes['stage_key']
        self.task_id = attributes['task_id']
        self.index = attributes['index']
        self.attempt = attributes['attempt']
        self.launch_time = attributes['launch_time']
        self.duration = attributes['duration']
        self.executor_key = attributes['executor_key']
        self.host = attributes['host']
        self.status = attributes['status']
        self.task_locality = attributes['task_locality']
        self.speculative = attributes['speculative']
        self.accumulator_updates = attributes['accumulator_updates']
        self.executor_deserialize_time = attributes['executor_deserialize_time']
        self.executor_deserialize_cpu_time = attributes['executor_deserialize_cpu_time']
        self.executor_run_time = attributes['executor_run_time']
        self.executor_cpu_time = attributes['executor_cpu_time']
        self.result_size = attributes['result_size']
        self.jvm_gc_time = attributes['jvm_gc_time']
        self.result_serialization_time = attributes['result_serialization_time']
        self.memory_bytes_spilled = attributes['memory_bytes_spilled']
        self.disk_bytes_spilled = attributes['disk_bytes_spilled']
        self.peak_execution_memory = attributes['peak_execution_memory']
        self.bytes_read = attributes['bytes_read']
        self.records_read = attributes['records_read']
        self.bytes_written = attributes['bytes_written']
        self.records_written = attributes['records_written']
        self.shuffle_remote_blocks_fetched = attributes['shuffle_remote_blocks_fetched']
        self.shuffle_local_blocks_fetched = attributes['shuffle_local_blocks_fetched']
        self.shuffle_fetch_wait_time = attributes['shuffle_fetch_wait_time']
        self.shuffle_remote_bytes_read = attributes['shuffle_remote_bytes_read']
        self.shuffle_remote_bytes_read_to_disk = attributes['shuffle_remote_bytes_read_to_disk']
        self.shuffle_local_bytes_read = attributes['shuffle_local_bytes_read']
        self.shuffle_records_read = attributes['shuffle_records_read']
        self.shuffle_bytes_written = attributes['shuffle_bytes_written']
        self.shuffle_write_time = attributes['shuffle_write_time']
        self.shuffle_records_written = attributes['shuffle_records_written']

