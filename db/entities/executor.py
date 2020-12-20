# coding=utf-8

from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey
from sqlalchemy.orm import relationship

from db.base import Base
from history_fetcher.utils import get_prop


class Executor(Base):
    """
    A class used to represent the Executor entity in the database.

    Each executor in each Spark application should be represented by one record in the Executor table.
    """
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
    remove_time = Column(DateTime)
    remove_reason = Column(String)
    executor_stdout_log = Column(String)
    executor_stderr_log = Column(String)
    used_on_heap_storage_memory = Column(String)
    used_off_heap_storage_memory = Column(String)
    total_on_heap_storage_memory = Column(String)
    total_off_heap_storage_memory = Column(String)
    blacklisted_in_stages = Column(String)  # TODO implement this as a relationship to stage?

    def __init__(self, attributes):
        """
        Create an Executor object.
        :param attributes: dictionary {name: value} containing the attributes
        """
        self.executor_key = get_prop(attributes, "executor_key")
        self.app_id = get_prop(attributes, "app_id")
        self.id = get_prop(attributes, "id")
        self.host_port = get_prop(attributes, "host_port")
        self.is_active = get_prop(attributes, "is_active")
        self.rdd_blocks = get_prop(attributes, "rdd_blocks")
        self.memory_used = get_prop(attributes, "memory_used")
        self.disk_used = get_prop(attributes, "disk_used")
        self.total_cores = get_prop(attributes, "total_cores")
        self.max_tasks = get_prop(attributes, "max_tasks")
        self.active_tasks = get_prop(attributes, "active_tasks")
        self.failed_tasks = get_prop(attributes, "failed_tasks")
        self.total_duration = get_prop(attributes, "total_duration")
        self.total_gc_time = get_prop(attributes, "total_gc_time")
        self.total_input_bytes = get_prop(attributes, "total_input_bytes")
        self.total_shuffle_read = get_prop(attributes, "total_shuffle_read")
        self.total_shuffle_write = get_prop(attributes, "total_shuffle_write")
        self.is_blacklisted = get_prop(attributes, "is_blacklisted")
        self.max_memory = get_prop(attributes, "max_memory")
        self.add_time = get_prop(attributes, "add_time")
        self.remove_time = get_prop(attributes, "remove_time")
        self.remove_reason = get_prop(attributes, "remove_reason")
        self.executor_stdout_log = get_prop(attributes, "executor_stdout_log")
        self.executor_stderr_log = get_prop(attributes, "executor_stderr_log")
        self.used_on_heap_storage_memory = get_prop(attributes, "used_on_heap_storage_memory")
        self.used_off_heap_storage_memory = get_prop(attributes, "used_off_heap_storage_memory")
        self.total_on_heap_storage_memory = get_prop(attributes, "total_on_heap_storage_memory")
        self.total_off_heap_storage_memory = get_prop(attributes, "total_off_heap_storage_memory")
        self.blacklisted_in_stages = get_prop(attributes, "blacklisted_in_stages")

    @staticmethod
    def get_fetch_dict(app_id, executor):
        return {
            'executor_key': f"{app_id}_{executor['id']}",
            'app_id': app_id,
            'id': executor['id'],
            'host_port': executor['hostPort'],
            'is_active': executor['isActive'],
            'rdd_blocks': executor['rddBlocks'],
            'memory_used': executor['memoryUsed'],
            'disk_used': executor['diskUsed'],
            'total_cores': executor['totalCores'],
            'max_tasks': executor['maxTasks'],
            'active_tasks': executor['activeTasks'],
            'failed_tasks': executor['failedTasks'],
            'total_duration': executor['totalDuration'],
            'total_gc_time': executor['totalGCTime'],
            'total_input_bytes': executor['totalInputBytes'],
            'total_shuffle_read': executor['totalShuffleRead'],
            'total_shuffle_write': executor['totalShuffleWrite'],
            'is_blacklisted': executor['isBlacklisted'],
            'max_memory': executor['maxMemory'],
            'add_time': executor['addTime'],
            'remove_time': get_prop(executor, 'removeTime'),
            'remove_reason': get_prop(executor, 'removeReason'),
            'executor_stdout_log': get_prop(executor, 'executorLogs', 'stdout'),
            'executor_stderr_log': get_prop(executor, 'executorLogs', 'stderr'),
            'used_on_heap_storage_memory': get_prop(executor, 'memoryMetrics', 'usedOnHeapStorageMemory'),
            'used_off_heap_storage_memory': get_prop(executor, 'memoryMetrics', 'usedOffHeapStorageMemory'),
            'total_on_heap_storage_memory': get_prop(executor, 'memoryMetrics', 'totalOnHeapStorageMemory'),
            'total_off_heap_storage_memory': get_prop(executor, 'memoryMetrics', 'totalOffHeapStorageMemory'),
            'blacklisted_in_stages': executor['blacklistedInStages']
        }
