# coding=utf-8

from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey
from sqlalchemy.orm import relationship

from db.base import Base
from history_fetcher.utils import get_prop


class JobEntity(Base):
    """
    A class used to represent the Job entity in the database.

    Each job within each Spark application should be represented by one record in the Job table.
    """
    __tablename__ = 'job'

    job_key = Column(String, primary_key=True)
    app = relationship("ApplicationEntity")
    app_id = Column(String, ForeignKey('application.app_id'))
    job_id = Column(String)
    submission_time = Column(DateTime)
    completion_time = Column(DateTime)
    status = Column(String)
    num_tasks = Column(Integer)
    num_active_tasks = Column(Integer)
    num_completed_tasks = Column(Integer)
    num_skipped_tasks = Column(Integer)
    num_failed_tasks = Column(Integer)
    num_killed_tasks = Column(Integer)
    num_completed_indices = Column(Integer)
    num_active_stages = Column(Integer)
    num_completed_stages = Column(Integer)
    num_skipped_stages = Column(Integer)
    num_failed_stages = Column(Integer)
    killed_tasks_summary = Column(JSON)

    def __init__(self, attributes):
        """
        Create a Job object.
        :param attributes: dictionary {name: value} containing the attributes
        """
        self.job_key = get_prop(attributes, "job_key")
        self.app_id = get_prop(attributes, "app_id")
        self.job_id = get_prop(attributes, "job_id")
        self.submission_time = get_prop(attributes, "submission_time")
        self.completion_time = get_prop(attributes, "completion_time")
        self.status = get_prop(attributes, "status")
        self.num_tasks = get_prop(attributes, "num_tasks")
        self.num_active_tasks = get_prop(attributes, "num_active_tasks")
        self.num_completed_tasks = get_prop(attributes, "num_completed_tasks")
        self.num_skipped_tasks = get_prop(attributes, "num_skipped_tasks")
        self.num_failed_tasks = get_prop(attributes, "num_failed_tasks")
        self.num_killed_tasks = get_prop(attributes, "num_killed_tasks")
        self.num_completed_indices = get_prop(attributes, "num_completed_indices")
        self.num_active_stages = get_prop(attributes, "num_active_stages")
        self.num_completed_stages = get_prop(attributes, "num_completed_stages")
        self.num_skipped_stages = get_prop(attributes, "num_skipped_stages")
        self.num_failed_stages = get_prop(attributes, "num_failed_stages")
        self.killed_tasks_summary = get_prop(attributes, "killed_tasks_summary")

    @staticmethod
    def get_fetch_dict(app_id, job):
        return {
            'job_key': f"{app_id}_{job['jobId']}",
            'app_id': app_id,
            'job_id': job['jobId'],
            'submission_time': job['submissionTime'],
            'completion_time': job['completionTime'],
            'status': job['status'],
            'num_tasks': job['numTasks'],
            'num_active_tasks': job['numActiveTasks'],
            'num_completed_tasks': job['numCompletedTasks'],
            'num_skipped_tasks': job['numSkippedTasks'],
            'num_failed_tasks': job['numFailedTasks'],
            'num_killed_tasks': job['numKilledTasks'],
            'num_completed_indices': job['numCompletedIndices'],
            'num_active_stages': job['numActiveStages'],
            'num_completed_stages': job['numCompletedStages'],
            'num_skipped_stages': job['numSkippedStages'],
            'num_failed_stages': job['numFailedStages'],
            'killed_tasks_summary': job['killedTasksSummary']
        }
