# coding=utf-8

from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey
from sqlalchemy.orm import relationship

from db.base import Base
from history_fetcher.utils import get_prop


class Stage(Base):
    """
    A class used to represent the Stage entity in the database.

    Each stage within each Spark application should be represented by one record in the Stage table.
    """
    __tablename__ = 'stage'

    stage_key = Column(String, primary_key=True)
    app = relationship("Application")
    app_id = Column(String, ForeignKey('application.app_id'))
    stage_statistics = relationship("StageStatistics", uselist=False, back_populates="stage")
    status = Column(String)
    stage_id = Column(Integer)
    attempt_id = Column(Integer)
    job = relationship("Job")
    job_key = Column(String, ForeignKey('job.job_key'))
    num_tasks = Column(Integer)
    num_active_tasks = Column(Integer)
    num_complete_tasks = Column(Integer)
    num_failed_tasks = Column(Integer)
    num_killed_tasks = Column(Integer)
    num_completed_indices = Column(Integer)
    executor_run_time = Column(BigInteger)
    executor_cpu_time = Column(BigInteger)
    submission_time = Column(DateTime)
    first_task_launched_time = Column(DateTime)
    completion_time = Column(DateTime)
    input_bytes = Column(BigInteger)
    input_records = Column(BigInteger)
    output_bytes = Column(BigInteger)
    output_records = Column(BigInteger)
    shuffle_read_bytes = Column(BigInteger)
    shuffle_read_records = Column(BigInteger)
    shuffle_write_bytes = Column(BigInteger)
    shuffle_write_records = Column(BigInteger)
    memory_bytes_spilled = Column(BigInteger)
    disk_bytes_spilled = Column(BigInteger)
    name = Column(String)
    details = Column(String)
    scheduling_pool = Column(String)
    rdd_ids = Column(ARRAY(Integer))
    accumulator_updates = Column(ARRAY(String))
    killed_tasks_summary = Column(JSON)

    def __init__(self, attributes):
        """
        Create a Stage object.
        :param attributes: dictionary {name: value} containing the attributes
        """
        self.stage_key = get_prop(attributes, "stage_key")
        self.app_id = get_prop(attributes, "app_id")
        self.status = get_prop(attributes, "status")
        self.stage_id = get_prop(attributes, "stage_id")
        self.attempt_id = get_prop(attributes, "attempt_id")
        self.job_key = get_prop(attributes, "job_key")
        self.num_tasks = get_prop(attributes, "num_tasks")
        self.num_active_tasks = get_prop(attributes, "num_active_tasks")
        self.num_complete_tasks = get_prop(attributes, "num_complete_tasks")
        self.num_failed_tasks = get_prop(attributes, "num_failed_tasks")
        self.num_killed_tasks = get_prop(attributes, "num_killed_tasks")
        self.num_completed_indices = get_prop(attributes, "num_completed_indices")
        self.executor_run_time = get_prop(attributes, "executor_run_time")
        self.executor_cpu_time = get_prop(attributes, "executor_cpu_time")
        self.submission_time = get_prop(attributes, "submission_time")
        self.first_task_launched_time = get_prop(attributes, "first_task_launched_time")
        self.completion_time = get_prop(attributes, "completion_time")
        self.input_bytes = get_prop(attributes, "input_bytes")
        self.input_records = get_prop(attributes, "input_records")
        self.output_bytes = get_prop(attributes, "output_bytes")
        self.output_records = get_prop(attributes, "output_records")
        self.shuffle_read_bytes = get_prop(attributes, "shuffle_read_bytes")
        self.shuffle_read_records = get_prop(attributes, "shuffle_read_records")
        self.shuffle_write_bytes = get_prop(attributes, "shuffle_write_bytes")
        self.shuffle_write_records = get_prop(attributes, "shuffle_write_records")
        self.memory_bytes_spilled = get_prop(attributes, "memory_bytes_spilled")
        self.disk_bytes_spilled = get_prop(attributes, "disk_bytes_spilled")
        self.name = get_prop(attributes, "name")
        self.details = get_prop(attributes, "details")
        self.scheduling_pool = get_prop(attributes, "scheduling_pool")
        self.rdd_ids = get_prop(attributes, "rdd_ids")
        self.accumulator_updates = get_prop(attributes, "accumulator_updates")
        self.killed_tasks_summary = get_prop(attributes, "killed_tasks_summary")

    @staticmethod
    def get_fetch_dict(app_id, stage, stage_job_mapping):
        return {
            'stage_key': f"{app_id}_{stage['stageId']}",
            'app_id': app_id,
            'status': stage['status'],
            'stage_id': stage['stageId'],
            'attempt_id': stage['attemptId'],
            'job_key': stage_job_mapping[f"{app_id}_{stage['stageId']}"],
            'num_tasks': stage['numTasks'],
            'num_active_tasks': stage['numActiveTasks'],
            'num_complete_tasks': stage['numCompleteTasks'],
            'num_failed_tasks': stage['numFailedTasks'],
            'num_killed_tasks': stage['numKilledTasks'],
            'num_completed_indices': stage['numCompletedIndices'],
            'executor_run_time': stage['executorRunTime'],
            'executor_cpu_time': stage['executorCpuTime'],
            'submission_time': get_prop(stage, 'submissionTime'),
            'first_task_launched_time': get_prop(stage, 'firstTaskLaunchedTime'),
            'completion_time': get_prop(stage, 'completionTime'),
            'input_bytes': stage['inputBytes'],
            'input_records': stage['inputRecords'],
            'output_bytes': stage['outputBytes'],
            'output_records': stage['outputRecords'],
            'shuffle_read_bytes': stage['shuffleReadBytes'],
            'shuffle_read_records': stage['shuffleReadRecords'],
            'shuffle_write_bytes': stage['shuffleWriteBytes'],
            'shuffle_write_records': stage['shuffleWriteRecords'],
            'memory_bytes_spilled': stage['memoryBytesSpilled'],
            'disk_bytes_spilled': stage['diskBytesSpilled'],
            'name': stage['name'],
            'details': stage['details'],
            'scheduling_pool': stage['schedulingPool'],
            'rdd_ids': stage['rddIds'],
            'accumulator_updates': stage['accumulatorUpdates'],
            'killed_tasks_summary': stage['killedTasksSummary']
        }
