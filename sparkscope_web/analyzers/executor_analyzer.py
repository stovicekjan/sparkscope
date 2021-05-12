from sqlalchemy import desc
from sqlalchemy.sql import func

from db.entities.executor import ExecutorEntity
from db.entities.stage_statistics import StageStatisticsEntity
from db.entities.task import TaskEntity
from sparkscope_web.analyzers.analyzer import Analyzer
from db.entities.stage import StageEntity
from sparkscope_web.metrics.helpers import size_in_bytes, fmt_time
from sparkscope_web.metrics.metric import StageFailureMetric, EmptyMetric, StageSkewMetric, StageDiskSpillMetric, \
    DriverGcTimeMetric, ExecutorGcTimeMetric
from sparkscope_web.metrics.metric_details import MetricDetailsList
from sparkscope_web.metrics.metrics_constants import SPARK_RESERVED_MEMORY, DEFAULT_EXECUTOR_MEMORY, \
    EXECUTOR_MEMORY_WASTAGE_THRESHOLDS, DRIVER_TOO_LOW_GC_THRESHOLDS, \
    DRIVER_TOO_HIGH_GC_THRESHOLDS, EXECUTOR_TOO_LOW_GC_THRESHOLDS, EXECUTOR_TOO_HIGH_GC_THRESHOLDS
from sparkscope_web.constants import EXECUTOR_MEMORY_KEY
from sparkscope_web.metrics.severity import Severity


class ExecutorAnalyzer(Analyzer):
    """
    Class for analyzing executors.
    """
    def __init__(self, app):
        """
        Create the ExecutorAnalyzer object
        :param app: Application object
        """
        super().__init__()
        self.app = app
        self.executors = self.db.query(ExecutorEntity) \
                                .filter(ExecutorEntity.app_id == app.app_id,
                                        ExecutorEntity.id != "driver",
                                        ExecutorEntity.id is not None)\
                                .all()
        self.driver = self.db.query(ExecutorEntity) \
                             .filter(ExecutorEntity.app_id == app.app_id,
                                     ExecutorEntity.id == "driver") \
                             .first()

    def analyze_executor_memory_wastage(self):
        """
        !!! At the moment, this analysis does not work correctly and should not be used !!!
        Analyze if executors have reasonable amount of memory allocated or if they have more than needed.
        :return: Metric object
        """
        severity = Severity.NONE
        details = {}

        allocated_executor_memory = self.app.get_spark_property(EXECUTOR_MEMORY_KEY)
        allocated_executor_memory_bytes = size_in_bytes(allocated_executor_memory, default=DEFAULT_EXECUTOR_MEMORY)

        # TODO Currently, this analysis/metric cannot work with Spark History Server (on SHS, peak execution
        # TODO memory is always 0).
        # TODO Maybe there could be a way how to retrieve the peak memory usage data from event logs.
        max_memory_usage_per_executor = 0

        ratio = allocated_executor_memory_bytes / (max_memory_usage_per_executor + EXECUTOR_MEMORY_KEY)
        severity = EXECUTOR_MEMORY_WASTAGE_THRESHOLDS.severity_of(ratio)

        return EmptyMetric(severity=Severity.NONE)

    def analyze_driver_gc_time(self):
        """
        Analyze garbage collection time of the driver
        :return: DriverGcTimeMetric object if an issue is found. Otherwise, EmptyMetric is returned.
        """
        if self.driver is None or self.driver.total_duration == 0:
            return EmptyMetric(severity=Severity.NONE)

        ratio = self.driver.total_gc_time / self.driver.total_duration

        severity_low = DRIVER_TOO_LOW_GC_THRESHOLDS.severity_of(ratio)
        severity_high = DRIVER_TOO_HIGH_GC_THRESHOLDS.severity_of(ratio)

        if severity_low > severity_high:
            overall_info = f"Driver spent too low time with Garbage Collection: " \
                           f"{fmt_time(self.driver.total_gc_time/1000)} out of " \
                           f"{fmt_time(self.driver.total_duration/1000)} ({(ratio*100):.2f} %)"
            details = MetricDetailsList(ascending=True)
            severity = severity_low
        elif severity_low < severity_high:
            overall_info = f"Driver spent too much time with Garbage Collection: " \
                           f"{fmt_time(self.driver.total_gc_time/1000)} out of " \
                           f"{fmt_time(self.driver.total_duration/1000)} ({(ratio*100):.2f} %)"
            details = MetricDetailsList(ascending=True)
            severity = severity_high
        else:
            return EmptyMetric(severity=Severity.NONE)

        return DriverGcTimeMetric(severity, overall_info, details)

    def analyze_executors_gc_time(self):
        """
        Analyze garbage collection time of the executors.
        :return: ExecutorGcTimeMetric object if an issue is found for at leas one executor. Otherwise, EmptyMetric is
        returned.
        """
        if self.executors is None or len(self.executors) == 0:
            return EmptyMetric(severity=Severity.NONE)

        total_gc_time = sum(e.total_gc_time for e in self.executors)
        total_duration = sum(e.total_duration for e in self.executors)

        if total_duration == 0:
            return EmptyMetric(severity=Severity.NONE)

        ratio = total_gc_time / total_duration

        severity_low = EXECUTOR_TOO_LOW_GC_THRESHOLDS.severity_of(ratio)
        severity_high = EXECUTOR_TOO_HIGH_GC_THRESHOLDS.severity_of(ratio)

        if severity_low > severity_high:
            overall_info = f"Executors spent too low time with Garbage Collection: {fmt_time(total_gc_time/1000)} " \
                           f"/ {fmt_time(total_duration/1000)} ({(ratio*100):.2f} %)"
            details = MetricDetailsList(ascending=True)
            severity = severity_low
        elif severity_low < severity_high:
            overall_info = f"Executors spent too much time with Garbage Collection: {fmt_time(total_gc_time/1000)} " \
                           f"/ {fmt_time(total_duration/1000)} ({(ratio*100):.2f} %)"
            details = MetricDetailsList(ascending=True)
            severity = severity_high
        else:
            return EmptyMetric(severity=Severity.NONE)

        return ExecutorGcTimeMetric(severity, overall_info, details)
