# coding=utf-8

from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey
from sqlalchemy.orm import relationship

from db.base import Base


class Job(Base):
    """
    A class used to represent the Job entity in the database.

    Each job within each Spark application should be represented by one record in the Job table.
    """
    __tablename__ = 'job'

    job_key = Column(String, primary_key=True)
    app = relationship("Application")
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
        self.job_key = attributes["job_key"]
        self.app_id = attributes["app_id"]
        self.job_id = attributes["job_id"]
        self.submission_time = attributes["submission_time"]
        self.completion_time = attributes["completion_time"]
        self.status = attributes["status"]
        self.num_tasks = attributes["num_tasks"]
        self.num_active_tasks = attributes["num_active_tasks"]
        self.num_completed_tasks = attributes["num_completed_tasks"]
        self.num_skipped_tasks = attributes["num_skipped_tasks"]
        self.num_failed_tasks = attributes["num_failed_tasks"]
        self.num_killed_tasks = attributes["num_killed_tasks"]
        self.num_completed_indices = attributes["num_completed_indices"]
        self.num_active_stages = attributes["num_active_stages"]
        self.num_completed_stages = attributes["num_completed_stages"]
        self.num_skipped_stages = attributes["num_skipped_stages"]
        self.num_failed_stages = attributes["num_failed_stages"]
        self.killed_tasks_summary = attributes["killed_tasks_summary"]

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
