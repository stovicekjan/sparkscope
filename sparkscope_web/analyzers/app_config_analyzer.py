from sqlalchemy import desc
from sqlalchemy.sql import func

from sparkscope_web.analyzers.analyzer import Analyzer
from sparkscope_web.constants import SERIALIZER_KEY
from sparkscope_web.metrics.metric import StageFailureMetric, EmptyMetric, StageSkewMetric, StageDiskSpillMetric, \
    DriverGcTimeMetric, ExecutorGcTimeMetric, SerializerConfigMetric
from sparkscope_web.metrics.metrics_constants import DEFAULT_SERIALIZER, PREFERRED_SERIALIZER
from sparkscope_web.metrics.severity import Severity


class AppConfigAnalyzer(Analyzer):
    def __init__(self, app):
        super().__init__()
        self.app = app

    def analyze_serializer_config(self):
        used_serializer = self.app.get_spark_property(SERIALIZER_KEY) or DEFAULT_SERIALIZER

        if used_serializer != PREFERRED_SERIALIZER:
            severity = Severity.HIGH
            overall_info = f"{used_serializer} was used, but {PREFERRED_SERIALIZER} could have better performance."
            details = {}
            return SerializerConfigMetric(severity, overall_info, details)
        else:
            return EmptyMetric(Severity.NONE)
