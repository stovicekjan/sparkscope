# coding=utf-8

from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey, Float
from sqlalchemy.orm import relationship

from db.base import Base


class StageStatistics(Base):
    """
    A class used to represent the stage_statistics entity in the database.

    Each stage in each Spark application should be represented by one record in the stage_statistics table.
    All the attributes (except for stage_key) should contain an array of five values, one for each quantile:
    0.001, 0.25, 0.5, 0.75, 0.999
    """
    __tablename__ = 'stage_statistics'

    stage_key = Column(String, ForeignKey('stage.stage_key'), primary_key=True)
    stage = relationship("Stage", back_populates="stage_statistics")
    quantiles = Column(ARRAY(Float))
    executor_deserialize_time = Column(ARRAY(Float))
    executor_deserialize_cpu_time = Column(ARRAY(Float))
    executor_run_time = Column(ARRAY(Float))
    executor_cpu_time = Column(ARRAY(Float))
    result_size = Column(ARRAY(Float))
    jvm_gc_time = Column(ARRAY(Float))
    result_serialization_time = Column(ARRAY(Float))
    getting_result_time = Column(ARRAY(Float))
    scheduler_delay = Column(ARRAY(Float))
    peak_execution_memory = Column(ARRAY(Float))
    memory_bytes_spilled = Column(ARRAY(Float))
    disk_bytes_spilled = Column(ARRAY(Float))
    bytes_read = Column(ARRAY(Float))
    records_read = Column(ARRAY(Float))
    bytes_written = Column(ARRAY(Float))
    records_written = Column(ARRAY(Float))
    shuffle_read_bytes = Column(ARRAY(Float))
    shuffle_read_records = Column(ARRAY(Float))
    shuffle_remote_blocks_fetched = Column(ARRAY(Float))
    shuffle_local_blocks_fetched = Column(ARRAY(Float))
    shuffle_fetch_wait_time = Column(ARRAY(Float))
    shuffle_remote_bytes_read = Column(ARRAY(Float))
    shuffle_remote_bytes_read_to_disk = Column(ARRAY(Float))
    shuffle_total_blocks_fetched = Column(ARRAY(Float))
    shuffle_write_bytes = Column(ARRAY(Float))
    shuffle_write_records = Column(ARRAY(Float))
    shuffle_write_time = Column(ARRAY(Float))

    def __init__(self, attributes):
        """
        Create a StageStatistics object.
        :param attributes: dictionary {name: value} containing the attributes
        """
        self.stage_key = attributes["stage_key"]
        self.quantiles = attributes["quantiles"]
        self.executor_deserialize_time = attributes["executor_deserialize_time"]
        self.executor_deserialize_cpu_time = attributes["executor_deserialize_cpu_time"]
        self.executor_run_time = attributes["executor_run_time"]
        self.executor_cpu_time = attributes["executor_cpu_time"]
        self.result_size = attributes["result_size"]
        self.jvm_gc_time = attributes["jvm_gc_time"]
        self.result_serialization_time = attributes["result_serialization_time"]
        self.getting_result_time = attributes["getting_result_time"]
        self.scheduler_delay = attributes["scheduler_delay"]
        self.peak_execution_memory = attributes["peak_execution_memory"]
        self.memory_bytes_spilled = attributes["memory_bytes_spilled"]
        self.disk_bytes_spilled = attributes["disk_bytes_spilled"]
        self.bytes_read = attributes["bytes_read"]
        self.records_read = attributes["records_read"]
        self.bytes_written = attributes["bytes_written"]
        self.records_written = attributes["records_written"]
        self.shuffle_read_bytes = attributes["shuffle_read_bytes"]
        self.shuffle_read_records = attributes["shuffle_read_records"]
        self.shuffle_remote_blocks_fetched = attributes["shuffle_remote_blocks_fetched"]
        self.shuffle_local_blocks_fetched = attributes["shuffle_local_blocks_fetched"]
        self.shuffle_fetch_wait_time = attributes["shuffle_fetch_wait_time"]
        self.shuffle_remote_bytes_read = attributes["shuffle_remote_bytes_read"]
        self.shuffle_remote_bytes_read_to_disk = attributes["shuffle_remote_bytes_read_to_disk"]
        self.shuffle_total_blocks_fetched = attributes["shuffle_total_blocks_fetched"]
        self.shuffle_write_bytes = attributes["shuffle_write_bytes"]
        self.shuffle_write_records = attributes["shuffle_write_records"]
        self.shuffle_write_time = attributes["shuffle_write_time"]

