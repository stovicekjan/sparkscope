# coding=utf-8

from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey, Float
from sqlalchemy.orm import relationship

from db.base import Base
from history_fetcher.utils import get_prop


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
    executor_deserialize_time = Column(BigInteger, comment="Elapsed time spent to deserialize this task. The value is expressed in milliseconds.")
    executor_deserialize_cpu_time = Column(BigInteger, comment="CPU time taken on the executor to deserialize this task. The value is expressed in nanoseconds.")
    executor_run_time = Column(BigInteger, comment="Elapsed time the executor spent running this task. This includes time fetching shuffle data. The value is expressed in milliseconds.")
    executor_cpu_time = Column(BigInteger, comment="CPU time the executor spent running this task. This includes time fetching shuffle data. The value is expressed in nanoseconds.")
    result_size = Column(BigInteger, comment="The number of bytes this task transmitted back to the driver as the TaskResult.")
    jvm_gc_time = Column(BigInteger, comment="Elapsed time the JVM spent in garbage collection while executing this task. The value is expressed in milliseconds.")
    result_serialization_time = Column(BigInteger, comment="Elapsed time spent serializing the task result. The value is expressed in milliseconds.")
    memory_bytes_spilled = Column(BigInteger, comment="The number of in-memory bytes spilled by this task.")
    disk_bytes_spilled = Column(BigInteger, comment="The number of on-disk bytes spilled by this task.")
    peak_execution_memory = Column(BigInteger, comment="Peak memory used by internal data structures created during shuffles, aggregations and joins. The value of this accumulator should be approximately the sum of the peak sizes across all such data structures created in this task. For SQL jobs, this only tracks all unsafe operators and ExternalSort.")
    bytes_read = Column(BigInteger, comment="Total number of bytes read. Related to reading data from org.apache.spark.rdd.HadoopRDD or from persisted data")
    records_read = Column(BigInteger, comment="Total number of records read. Related to reading data from org.apache.spark.rdd.HadoopRDD or from persisted data")
    bytes_written = Column(BigInteger, comment="Total number of bytes written. Related to writing data externally (e.g. to a distributed filesystem), defined only in tasks with output.")
    records_written = Column(BigInteger, comment="Total number of records written. Related to writing data externally (e.g. to a distributed filesystem), defined only in tasks with output.")
    shuffle_remote_blocks_fetched = Column(BigInteger, comment="Number of remote blocks fetched in shuffle operations")
    shuffle_local_blocks_fetched = Column(BigInteger, comment="Number of local (as opposed to read from a remote executor) blocks fetched in shuffle operations")
    shuffle_fetch_wait_time = Column(BigInteger, comment="Time the task spent waiting for remote shuffle blocks. This only includes the time blocking on shuffle input data. For instance if block B is being fetched while the task is still not finished processing block A, it is not considered to be blocking on block B. The value is expressed in milliseconds.")
    shuffle_remote_bytes_read = Column(BigInteger, comment="Number of remote bytes read in shuffle operations")
    shuffle_remote_bytes_read_to_disk = Column(BigInteger, comment="Number of remote bytes read to disk in shuffle operations. Large blocks are fetched to disk in shuffle read operations, as opposed to being read into memory, which is the default behavior.")
    shuffle_local_bytes_read = Column(BigInteger, comment="Number of bytes read in shuffle operations from local disk (as opposed to read from a remote executor)")
    shuffle_records_read = Column(BigInteger, comment="Number of records read in shuffle operations")
    shuffle_bytes_written = Column(BigInteger, comment="Number of bytes written in shuffle operations")
    shuffle_write_time = Column(BigInteger, comment="Time spent blocking on writes to disk or buffer cache. The value is expressed in nanoseconds.")
    shuffle_records_written = Column(BigInteger, comment="Number of records written in shuffle operations")

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

    @staticmethod
    def get_fetch_dict(stage_key, task, app_id):
        return {
            'task_key': f"{stage_key}_{task['taskId']}",
            'stage_key': stage_key,
            'task_id': task['taskId'],
            'index': task['index'],
            'attempt': task['attempt'],
            'launch_time': task['launchTime'],
            'duration': task['duration'],
            'executor_key': f"{app_id}_{task['executorId']}",
            'host': task['host'],
            'status': task['status'],
            'task_locality': task['taskLocality'],
            'speculative': task['speculative'],
            'accumulator_updates': task['accumulatorUpdates'],
            'executor_deserialize_time': get_prop(task, 'taskMetrics', 'executorDeserializeTime'),
            'executor_deserialize_cpu_time': get_prop(task, 'taskMetrics', 'executorDeserializeCpuTime'),
            'executor_run_time': get_prop(task, 'taskMetrics', 'executorRunTime'),
            'executor_cpu_time': get_prop(task, 'taskMetrics', 'executorCpuTime'),
            'result_size': get_prop(task, 'taskMetrics', 'resultSize'),
            'jvm_gc_time': get_prop(task, 'taskMetrics', 'jvmGcTime'),
            'result_serialization_time': get_prop(task, 'taskMetrics', 'resultSerializationTime'),
            'memory_bytes_spilled': get_prop(task, 'taskMetrics', 'memoryBytesSpilled'),
            'disk_bytes_spilled': get_prop(task, 'taskMetrics', 'diskBytesSpilled'),
            'peak_execution_memory': get_prop(task, 'taskMetrics', 'peakExecutionMemory'),
            'bytes_read': get_prop(task, 'taskMetrics', 'inputMetrics', 'bytesRead'),
            'records_read': get_prop(task, 'taskMetrics', 'inputMetrics', 'recordsRead'),
            'bytes_written': get_prop(task, 'taskMetrics', 'outputMetrics', 'bytesWritten'),
            'records_written': get_prop(task, 'taskMetrics', 'outputMetrics', 'recordsWritten'),
            'shuffle_remote_blocks_fetched': get_prop(task, 'taskMetrics','shuffleReadMetrics','remoteBlocksFetched'),
            'shuffle_local_blocks_fetched': get_prop(task, 'taskMetrics', 'shuffleReadMetrics', 'localBlocksFetched'),
            'shuffle_fetch_wait_time': get_prop(task, 'taskMetrics', 'shuffleReadMetrics', 'fetchWaitTime'),
            'shuffle_remote_bytes_read': get_prop(task, 'taskMetrics', 'shuffleReadMetrics', 'remoteBytesRead'),
            'shuffle_remote_bytes_read_to_disk': get_prop(task, 'taskMetrics', 'shuffleReadMetrics', 'remoteBytesReadToDisk'),
            'shuffle_local_bytes_read': get_prop(task, 'taskMetrics', 'shuffleReadMetrics', 'localBytesRead'),
            'shuffle_records_read': get_prop(task, 'taskMetrics', 'shuffleReadMetrics', 'recordsRead'),
            'shuffle_bytes_written': get_prop(task, 'taskMetrics', 'shuffleWriteMetrics', 'bytesWritten'),
            'shuffle_write_time': get_prop(task, 'taskMetrics', 'shuffleWriteMetrics', 'writeTime'),
            'shuffle_records_written': get_prop(task, 'taskMetrics', 'shuffleWriteMetrics', 'recordsWritten')
        }

