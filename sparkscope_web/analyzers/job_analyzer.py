from db.entities.job import Job
from sparkscope_web.analyzers.analyzer import Analyzer
from sparkscope_web.metrics.metric import StageFailureMetric, EmptyMetric, JobFailureMetric
from sparkscope_web.metrics.severity import Severity


class JobAnalyzer(Analyzer):
    def __init__(self, app):
        super().__init__()
        self.app = app
        self.jobs = self.db.query(Job.job_id, Job.status)\
                        .filter(Job.app_id == self.app.app_id)\
                        .all()

    def analyze_failed_jobs(self):
        """
        Analyze the Stages of the defined apps and return the metric details
        :param app: Application object
        :return: Metric details
        """
        all_jobs_count = len(self.jobs)
        if all_jobs_count == 0:
            return EmptyMetric(severity=Severity.NONE)
        failed_jobs = [j for j in self.jobs if j.status in ["FAILED", "KILLED"]]
        failed_jobs_count = len(failed_jobs)

        if failed_jobs_count > 0:
            severity = Severity.HIGH
            overall_info = f"{failed_jobs_count}/{all_jobs_count} jobs failed"
            details = {}
            for fj in failed_jobs:
                details[fj.job_id] = (f"Job {fj.job_id} {fj.status}", "")
            return JobFailureMetric(severity, overall_info, details)
        else:
            return EmptyMetric(severity=Severity.NONE)




