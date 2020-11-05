# coding=utf-8

from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey
from sqlalchemy.orm import relationship

from db.base import Base


class Stage(Base):
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
        self.stage_key = attributes["stage_key"]
        self.app_id = attributes["app_id"]
        self.status = attributes["status"]
        self.stage_id = attributes["stage_id"]
        self.attempt_id = attributes["attempt_id"]
        self.job_key = attributes["job_key"]
        self.num_tasks = attributes["num_tasks"]
        self.num_active_tasks = attributes["num_active_tasks"]
        self.num_complete_tasks = attributes["num_complete_tasks"]
        self.num_failed_tasks = attributes["num_failed_tasks"]
        self.num_killed_tasks = attributes["num_killed_tasks"]
        self.num_completed_indices = attributes["num_completed_indices"]
        self.executor_run_time = attributes["executor_run_time"]
        self.executor_cpu_time = attributes["executor_cpu_time"]
        self.submission_time = attributes["submission_time"]
        self.first_task_launched_time = attributes["first_task_launched_time"]
        self.completion_time = attributes["completion_time"]
        self.input_bytes = attributes["input_bytes"]
        self.input_records = attributes["input_records"]
        self.output_bytes = attributes["output_bytes"]
        self.output_records = attributes["output_records"]
        self.shuffle_read_bytes = attributes["shuffle_read_bytes"]
        self.shuffle_read_records = attributes["shuffle_read_records"]
        self.shuffle_write_bytes = attributes["shuffle_write_bytes"]
        self.shuffle_write_records = attributes["shuffle_write_records"]
        self.memory_bytes_spilled = attributes["memory_bytes_spilled"]
        self.disk_bytes_spilled = attributes["disk_bytes_spilled"]
        self.name = attributes["name"]
        self.details = attributes["details"]
        self.scheduling_pool = attributes["scheduling_pool"]
        self.rdd_ids = attributes["rdd_ids"]
        self.accumulator_updates = attributes["accumulator_updates"]
        self.killed_tasks_summary = attributes["killed_tasks_summary"]

