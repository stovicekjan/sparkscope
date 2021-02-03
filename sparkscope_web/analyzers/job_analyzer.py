from db.entities.job import Job
from sparkscope_web.analyzers.analyzer import Analyzer
from sparkscope_web.metrics.metric import StageFailureMetric, EmptyMetric, JobFailureMetric
from sparkscope_web.metrics.severity import Severity


class JobAnalyzer(Analyzer):
    def __init__(self):
        super().__init__()

    def analyze(self, app):
        """
        Analyze the Stages of the defined apps and return the metric details
        :param app: Application object
        :return: Metric details
        """

        jobs = self.db.query(Job).filter(Job.app_id == app.app_id)
        all_jobs_count = jobs.count()
        if all_jobs_count == 0:
            return EmptyMetric(severity=Severity.NONE)
        failed_jobs = jobs.filter(Job.status.in_(["FAILED", "KILLED"]))
        failed_jobs_count = failed_jobs.count()

        if failed_jobs_count > 0:
            severity = Severity.HIGH
            overall_info = f"{failed_jobs_count}/{all_jobs_count} jobs failed"
            details = []
            for fj in failed_jobs.all():
                details.append(f"Job {fj.job_id} {fj.status}")
            return JobFailureMetric(severity, overall_info, details)
        else:
            return EmptyMetric(severity=Severity.NONE)




