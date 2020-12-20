# coding=utf-8

from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey
from sqlalchemy.orm import relationship

from db.base import Base
from history_fetcher.utils import get_prop


class StageExecutor(Base):
    """
    A class used to represent stage_executor entity in the database (a usage of Executors within a Stage).

    Each Executor being used in a single Stage should be represented by one record in the StageExecutor table.
    """
    __tablename__ = 'stage_executor'

    stage_executor_key = Column(String, primary_key=True)
    stage = relationship("Stage")
    executor = relationship("Executor")
    stage_key = Column(String, ForeignKey("stage.stage_key"))
    executor_key = Column(String, ForeignKey("executor.executor_key"))
    executor_id = Column(String)
    task_time = Column(BigInteger)
    failed_tasks = Column(Integer)
    succeeded_tasks = Column(Integer)
    killed_tasks = Column(Integer)
    input_bytes = Column(BigInteger)
    input_records = Column(BigInteger)
    output_bytes = Column(BigInteger)
    output_records = Column(BigInteger)
    shuffle_read = Column(BigInteger)
    shuffle_read_records = Column(BigInteger)
    shuffle_write = Column(BigInteger)
    shuffle_write_records = Column(BigInteger)
    memory_bytes_spilled = Column(BigInteger)
    disk_bytes_spilled = Column(BigInteger)
    is_blacklisted_for_stage = Column(Boolean)

    def __init__(self, attributes):
        """
        Create a StageExecutor object.
        :param attributes: dictionary {name: value} containing the attributes
        """
        self.stage_executor_key = get_prop(attributes, "stage_executor_key")
        self.stage_key = get_prop(attributes, "stage_key")
        self.executor_key = get_prop(attributes, "executor_key")
        self.executor_id = get_prop(attributes, "executor_id")
        self.task_time = get_prop(attributes, "task_time")
        self.failed_tasks = get_prop(attributes, "failed_tasks")
        self.succeeded_tasks = get_prop(attributes, "succeeded_tasks")
        self.killed_tasks = get_prop(attributes, "killed_tasks")
        self.input_bytes = get_prop(attributes, "input_bytes")
        self.input_records = get_prop(attributes, "input_records")
        self.output_bytes = get_prop(attributes, "output_bytes")
        self.output_records = get_prop(attributes, "output_records")
        self.shuffle_read = get_prop(attributes, "shuffle_read")
        self.shuffle_read_records = get_prop(attributes, "shuffle_read_records")
        self.shuffle_write = get_prop(attributes, "shuffle_write")
        self.shuffle_write_records = get_prop(attributes, "shuffle_write_records")
        self.memory_bytes_spilled = get_prop(attributes, "memory_bytes_spilled")
        self.disk_bytes_spilled = get_prop(attributes, "disk_bytes_spilled")
        self.is_blacklisted_for_stage = get_prop(attributes, "is_blacklisted_for_stage")

    @staticmethod
    def get_fetch_dict(stage_key, executor_id, app_id, stage_executor_dict):
        return {
            'stage_executor_key': f"{stage_key}_{executor_id}",
            'stage_key': stage_key,
            'executor_key': f"{app_id}_{executor_id}",
            'executor_id': executor_id,
            'task_time': stage_executor_dict['taskTime'],
            'failed_tasks': stage_executor_dict['failedTasks'],
            'succeeded_tasks': stage_executor_dict['succeededTasks'],
            'killed_tasks': stage_executor_dict['killedTasks'],
            'input_bytes': stage_executor_dict['inputBytes'],
            'input_records': stage_executor_dict['inputRecords'],
            'output_bytes': stage_executor_dict['outputBytes'],
            'output_records': stage_executor_dict['outputRecords'],
            'shuffle_read': stage_executor_dict['shuffleRead'],
            'shuffle_read_records': stage_executor_dict['shuffleReadRecords'],
            'shuffle_write': stage_executor_dict['shuffleWrite'],
            'shuffle_write_records': stage_executor_dict['shuffleWriteRecords'],
            'memory_bytes_spilled': stage_executor_dict['memoryBytesSpilled'],
            'disk_bytes_spilled': stage_executor_dict['diskBytesSpilled'],
            'is_blacklisted_for_stage': stage_executor_dict['isBlacklistedForStage']
        }