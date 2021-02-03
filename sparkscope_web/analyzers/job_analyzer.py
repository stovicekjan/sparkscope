from sparkscope_web.analyzers.analyzer import Analyzer
from db.entities.stage import Stage
from sparkscope_web.metrics.metric import StageFailureMetric, EmptyMetric
from sparkscope_web.metrics.severity import Severity


class StageAnalyzer(Analyzer):
    def __init__(self):
        super().__init__()

    def analyze(self, app):
        """
        Analyze the Stages of the defined apps and return the metric details
        :param app: Application object
        :return: Metric details
        """

        stages = self.db.query(Stage).filter(Stage.app_id == app.app_id)
        all_stages_count = stages.count()
        if all_stages_count == 0:
            return EmptyMetric(severity=Severity.NONE)
        failed_stages = stages.filter(Stage.status.in_(["FAILED", "KILLED"]))
        failed_stages_count = failed_stages.count()

        if failed_stages_count > 0:
            severity = Severity.HIGH
            overall_info = f"{failed_stages_count}/{all_stages_count} stages failed"
            print(overall_info)
            details = []
            for fs in failed_stages.all():
                details.append(f"Stage {fs.stage_id} {fs.status}: {fs.failure_reason}")
            return StageFailureMetric(severity, overall_info, details)
        else:
            return EmptyMetric(severity=Severity.NONE)




