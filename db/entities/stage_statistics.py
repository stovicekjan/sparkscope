# coding=utf-8

from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey, Float
from sqlalchemy.orm import relationship

from db.base import Base
from history_fetcher.utils import get_prop

class StageStatisticsEntity(Base):
    """
    A class used to represent the stage_statistics entity in the database.

    Each stage in each Spark application should be represented by one record in the stage_statistics table.
    All the attributes (except for stage_key) should contain an array of five values, one for each quantile:
    0.001, 0.25, 0.5, 0.75, 0.999
    """
    __tablename__ = 'stage_statistics'

    stage_key = Column(String, ForeignKey('stage.stage_key'), primary_key=True)
    stage = relationship("StageEntity", back_populates="stage_statistics")
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
        self.stage_key = get_prop(attributes, "stage_key")
        self.quantiles = get_prop(attributes, "quantiles")
        self.executor_deserialize_time = get_prop(attributes, "executor_deserialize_time")
        self.executor_deserialize_cpu_time = get_prop(attributes, "executor_deserialize_cpu_time")
        self.executor_run_time = get_prop(attributes, "executor_run_time")
        self.executor_cpu_time = get_prop(attributes, "executor_cpu_time")
        self.result_size = get_prop(attributes, "result_size")
        self.jvm_gc_time = get_prop(attributes, "jvm_gc_time")
        self.result_serialization_time = get_prop(attributes, "result_serialization_time")
        self.getting_result_time = get_prop(attributes, "getting_result_time")
        self.scheduler_delay = get_prop(attributes, "scheduler_delay")
        self.peak_execution_memory = get_prop(attributes, "peak_execution_memory")
        self.memory_bytes_spilled = get_prop(attributes, "memory_bytes_spilled")
        self.disk_bytes_spilled = get_prop(attributes, "disk_bytes_spilled")
        self.bytes_read = get_prop(attributes, "bytes_read")
        self.records_read = get_prop(attributes, "records_read")
        self.bytes_written = get_prop(attributes, "bytes_written")
        self.records_written = get_prop(attributes, "records_written")
        self.shuffle_read_bytes = get_prop(attributes, "shuffle_read_bytes")
        self.shuffle_read_records = get_prop(attributes, "shuffle_read_records")
        self.shuffle_remote_blocks_fetched = get_prop(attributes, "shuffle_remote_blocks_fetched")
        self.shuffle_local_blocks_fetched = get_prop(attributes, "shuffle_local_blocks_fetched")
        self.shuffle_fetch_wait_time = get_prop(attributes, "shuffle_fetch_wait_time")
        self.shuffle_remote_bytes_read = get_prop(attributes, "shuffle_remote_bytes_read")
        self.shuffle_remote_bytes_read_to_disk = get_prop(attributes, "shuffle_remote_bytes_read_to_disk")
        self.shuffle_total_blocks_fetched = get_prop(attributes, "shuffle_total_blocks_fetched")
        self.shuffle_write_bytes = get_prop(attributes, "shuffle_write_bytes")
        self.shuffle_write_records = get_prop(attributes, "shuffle_write_records")
        self.shuffle_write_time = get_prop(attributes, "shuffle_write_time")

    @staticmethod
    def get_fetch_dict(stage_key, stage_statistics):
        return {
            'stage_key': stage_key,
            'quantiles': stage_statistics["quantiles"],
            'executor_deserialize_time': stage_statistics["executorDeserializeTime"],
            'executor_deserialize_cpu_time': stage_statistics["executorDeserializeCpuTime"],
            'executor_run_time': stage_statistics["executorRunTime"],
            'executor_cpu_time': stage_statistics["executorCpuTime"],
            'result_size': stage_statistics["resultSize"],
            'jvm_gc_time': stage_statistics["jvmGcTime"],
            'result_serialization_time': stage_statistics["resultSerializationTime"],
            'getting_result_time': stage_statistics["gettingResultTime"],
            'scheduler_delay': stage_statistics["schedulerDelay"],
            'peak_execution_memory': stage_statistics["peakExecutionMemory"],
            'memory_bytes_spilled': stage_statistics["memoryBytesSpilled"],
            'disk_bytes_spilled': stage_statistics["diskBytesSpilled"],
            'bytes_read': stage_statistics["inputMetrics"]["bytesRead"],
            'records_read': stage_statistics["inputMetrics"]["recordsRead"],
            'bytes_written': stage_statistics["outputMetrics"]["bytesWritten"],
            'records_written': stage_statistics["outputMetrics"]["recordsWritten"],
            'shuffle_read_bytes': stage_statistics["shuffleReadMetrics"]["readBytes"],
            'shuffle_read_records': stage_statistics["shuffleReadMetrics"]["readRecords"],
            'shuffle_remote_blocks_fetched': stage_statistics["shuffleReadMetrics"]["remoteBlocksFetched"],
            'shuffle_local_blocks_fetched': stage_statistics["shuffleReadMetrics"]["localBlocksFetched"],
            'shuffle_fetch_wait_time': stage_statistics["shuffleReadMetrics"]["fetchWaitTime"],
            'shuffle_remote_bytes_read': stage_statistics["shuffleReadMetrics"]["remoteBytesRead"],
            'shuffle_remote_bytes_read_to_disk': stage_statistics["shuffleReadMetrics"]["remoteBytesReadToDisk"],
            'shuffle_total_blocks_fetched': stage_statistics["shuffleReadMetrics"]["totalBlocksFetched"],
            'shuffle_write_bytes': stage_statistics["shuffleWriteMetrics"]["writeBytes"],
            'shuffle_write_records': stage_statistics["shuffleWriteMetrics"]["writeRecords"],
            'shuffle_write_time': stage_statistics["shuffleWriteMetrics"]["writeTime"]
        }
