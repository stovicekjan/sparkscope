import math

from sparkscope_web import constants
from sparkscope_web.analyzers.analyzer import Analyzer
from sparkscope_web.constants import SERIALIZER_KEY, SHUFFLE_SERVICE_ENABLED_KEY, SHUFFLE_TRACKING_ENABLED_KEY, \
    DYNAMIC_ALLOCATION_KEY, DYNAMIC_ALLOCATION_MIN_EXECUTORS_KEY, DYNAMIC_ALLOCATION_MAX_EXECUTORS_KEY, \
    SPARK_YARN_QUEUE_KEY, SPARK_YARN_QUEUE_DEFAULT_VALUE, EXECUTOR_MEMORY_KEY, EXECUTOR_MEMORY_OVERHEAD_KEY, \
    DRIVER_MEMORY_KEY, DRIVER_MEMORY_OVERHEAD_KEY, EXECUTOR_MEMORY_DEFAULT_VALUE, DRIVER_MEMORY_DEFAULT_VALUE, \
    EXECUTOR_CORES_KEY, EXECUTOR_CORES_DEFAULT_VALUE, DRIVER_CORES_KEY, DRIVER_CORES_DEFAULT_VALUE
from sparkscope_web.metrics.helpers import cast_to_bool, cast_to_int, size_in_bytes, fmt_bytes
from sparkscope_web.metrics.metric import EmptyMetric, SerializerConfigMetric, DynamicAllocationConfigMetric, \
    DynamicAllocationMinMaxExecutorsMetric, YarnQueueMetric, MemoryConfigMetric, CoreNumberMetric
from sparkscope_web.metrics.metric_details import MetricDetailsList, MetricDetails
from sparkscope_web.metrics.metrics_constants import DEFAULT_SERIALIZER, PREFERRED_SERIALIZER, \
    IS_DYNAMIC_ALLOCATION_PREFERRED, DYNAMIC_ALLOCATION_MAX_EXECUTORS_THRESHOLDS, \
    DYNAMIC_ALLOCATION_MIN_EXECUTORS_THRESHOLDS, DYNAMIC_ALLOCATION_MAX_EXECUTORS_LOW, \
    DYNAMIC_ALLOCATION_MIN_EXECUTORS_LOW, IS_DEFAULT_QUEUE_ALLOWED, EXECUTOR_MEMORY_CONFIG_THRESHOLDS, \
    EXECUTOR_MEMORY_OVERHEAD_CONFIG_THRESHOLDS, DRIVER_MEMORY_CONFIG_THRESHOLDS, \
    DRIVER_MEMORY_CONFIG_OVERHEAD_THRESHOLDS, EXECUTOR_MEMORY_CONFIG_LOW, EXECUTOR_MEMORY_OVERHEAD_CONFIG_LOW, \
    DRIVER_MEMORY_CONFIG_LOW, DRIVER_MEMORY_OVERHEAD_CONFIG_LOW, EXECUTOR_CORES_THRESHOLDS, DRIVER_CORES_THRESHOLDS
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
            details = MetricDetailsList()
            return SerializerConfigMetric(severity, overall_info, details)
        else:
            return EmptyMetric(Severity.NONE)

    def analyze_dynamic_allocation(self):
        """
        Analyzes dynamic allocation configuration. When dynamic allocation is preferred (this can be configured, True is
        default), but not used, an issue with HIGH severity is reported.
        If the dynamic allocation is used (the respective configuration is enabled), shuffle tracking or shuffle service
        should be enabled. If this is not the case, an issue with HIGH severity is reported. The purpose for enabling
        the shuffle tracking or shuffle service is to keep the respective shuffle files even after the executor is
        decommisioned.
        :return: DynamicAllocationMetric if issues are detected or EmptyMetric if no issues are detected.
        """
        try:
            is_dynamic_allocation_enabled = cast_to_bool(self.app.get_spark_property(DYNAMIC_ALLOCATION_KEY))
        except ValueError as e:
            # TODO add logging
            is_dynamic_allocation_enabled = False

        try:
            is_shuffle_tracking_enabled = cast_to_bool(self.app.get_spark_property(SHUFFLE_TRACKING_ENABLED_KEY))
        except ValueError as e:
            # TODO add logging
            is_shuffle_tracking_enabled = False

        try:
            is_shuffle_service_enabled = cast_to_bool(self.app.get_spark_property(SHUFFLE_SERVICE_ENABLED_KEY))
        except ValueError as e:
            # TODO add logging
            is_shuffle_service_enabled = False

        if IS_DYNAMIC_ALLOCATION_PREFERRED and not is_dynamic_allocation_enabled:
            overall_info = f"Dynamic allocation was disabled for this application."
            return DynamicAllocationConfigMetric(severity=Severity.HIGH, overall_info=overall_info,
                                                 details=MetricDetailsList())

        if is_dynamic_allocation_enabled and not is_shuffle_service_enabled and not is_shuffle_tracking_enabled:
            overall_info = f"If Dynamic allocation is enabled, an external shuffle service or " \
                           f"shuffle tracking should be enabled."
            details = MetricDetailsList()
            details.add(MetricDetails(entity_id=None, detail_string="Current settings: ", sort_attr=0,
                                      subdetails=[f"{SHUFFLE_TRACKING_ENABLED_KEY} = {is_shuffle_tracking_enabled}",
                                                  f"{SHUFFLE_SERVICE_ENABLED_KEY} = {is_shuffle_service_enabled}"]))
            return DynamicAllocationConfigMetric(severity=Severity.HIGH, overall_info=overall_info, details=details)
        else:
            return EmptyMetric(Severity.NONE)

    def analyze_min_max_executors(self):
        """
        If dynamic allocation is enabled, this method analyzes a configuration of minimum and maximum executors
        required.
        If the minimum value is larger than recommended, it should report an issue (as the resources might be wasted).
        If the maximum value is larger than recommended, it should report an issue. On multi-tenant clusters, where
        separate YARN queues should be used, these issues are not reported if the non-default queue is used (this
        should prevent eating up all the resources, one the queues are set up correctly).
        :return: DynamicAllocationMinMaxExecutorsMetric if issues are detected or EmptyMetric if no issues are detected.
        """
        try:
            is_dynamic_allocation_enabled = cast_to_bool(self.app.get_spark_property(DYNAMIC_ALLOCATION_KEY))
        except ValueError as e:
            # TODO add logging
            is_dynamic_allocation_enabled = False

        if not is_dynamic_allocation_enabled:
            return EmptyMetric(Severity.NONE)

        min_executors = cast_to_int(self.app.get_spark_property(DYNAMIC_ALLOCATION_MIN_EXECUTORS_KEY)) or 0
        max_executors = cast_to_int(self.app.get_spark_property(DYNAMIC_ALLOCATION_MAX_EXECUTORS_KEY)) or math.inf
        queue_name = self.app.get_spark_property(SPARK_YARN_QUEUE_KEY) or SPARK_YARN_QUEUE_DEFAULT_VALUE

        severity_min = DYNAMIC_ALLOCATION_MIN_EXECUTORS_THRESHOLDS.severity_of(min_executors)

        severity_max = DYNAMIC_ALLOCATION_MAX_EXECUTORS_THRESHOLDS.severity_of(max_executors) \
            if queue_name == SPARK_YARN_QUEUE_DEFAULT_VALUE else Severity.NONE

        severity = severity_max if (severity_max > severity_min) else severity_min

        if severity > Severity.NONE:
            overall_info = "Non-optimal configuration of minimum or maximum number of executors"
            details = MetricDetailsList()
            if severity_min > Severity.NONE:
                details.add(MetricDetails(entity_id=None,
                                          detail_string=f"{DYNAMIC_ALLOCATION_MIN_EXECUTORS_KEY} is currently set to "
                                                        f"{min_executors}, but ideally, it should be <= "
                                                        f"{DYNAMIC_ALLOCATION_MIN_EXECUTORS_LOW}",
                                          sort_attr=0,
                                          subdetails=[]))
            if severity_max > Severity.NONE:
                details.add(MetricDetails(entity_id=None,
                                          detail_string=f"{DYNAMIC_ALLOCATION_MAX_EXECUTORS_KEY} is currently set to "
                                                        f"{max_executors}, but ideally, it should be <= "
                                                        f"{DYNAMIC_ALLOCATION_MAX_EXECUTORS_LOW}",
                                          sort_attr=0,
                                          subdetails=["Also consider using non-default YARN queue to prevent the application to eat up all the resources"]))
            return DynamicAllocationMinMaxExecutorsMetric(overall_info=overall_info, severity=severity, details=details)
        else:
            return EmptyMetric(Severity.NONE)

    def analyze_yarn_queue(self):
        """
        If usage of the default YARN queue is not allowed in the config file, this method investigates if the
        particular application was submitted to the default queue, and if so, it will report an issue.
        :return: YarnQueueMetric if issue is detected or EmptyMetric if no issues are detected.
        """
        queue_name = self.app.get_spark_property(SPARK_YARN_QUEUE_KEY) or SPARK_YARN_QUEUE_DEFAULT_VALUE
        if not IS_DEFAULT_QUEUE_ALLOWED and queue_name == SPARK_YARN_QUEUE_DEFAULT_VALUE:
            overall_info = f"This application was submitted to YARN Queue '{SPARK_YARN_QUEUE_DEFAULT_VALUE}'. " \
                           f"Another queue should be used."
            return YarnQueueMetric(overall_info=overall_info, severity=Severity.HIGH, details=MetricDetailsList())
        else:
            return EmptyMetric(severity=Severity.NONE)

    def analyze_memory_configuration(self):
        """
        This method analyzes if the application requested more memory (driver, executor, memory+overhead) than
        recommended. If so, it will report an issue.
        :return: MemoryConfigMetric if issue is detected or EmptyMetric if no issues are detected.
        """
        executor_memory = size_in_bytes(self.app.get_spark_property(EXECUTOR_MEMORY_KEY), EXECUTOR_MEMORY_DEFAULT_VALUE)
        executor_memory_overhead = size_in_bytes(self.app.get_spark_property(EXECUTOR_MEMORY_OVERHEAD_KEY),
                                                 EXECUTOR_MEMORY_DEFAULT_VALUE) \
            or constants.get_default_memory_overhead(executor_memory)
        driver_memory = size_in_bytes(self.app.get_spark_property(DRIVER_MEMORY_KEY), DRIVER_MEMORY_DEFAULT_VALUE)
        driver_memory_overhead = size_in_bytes(self.app.get_spark_property(DRIVER_MEMORY_OVERHEAD_KEY),
                                               DRIVER_MEMORY_DEFAULT_VALUE) \
            or constants.get_default_memory_overhead(driver_memory)

        severity_executor_memory = EXECUTOR_MEMORY_CONFIG_THRESHOLDS.severity_of(executor_memory)
        severity_executor_memory_overhead = EXECUTOR_MEMORY_OVERHEAD_CONFIG_THRESHOLDS.severity_of(executor_memory_overhead)
        severity_driver_memory = DRIVER_MEMORY_CONFIG_THRESHOLDS.severity_of(driver_memory)
        severity_driver_memory_overhead = DRIVER_MEMORY_CONFIG_OVERHEAD_THRESHOLDS.severity_of(driver_memory_overhead)

        severity = max(severity_executor_memory, severity_executor_memory_overhead, severity_driver_memory,
                       severity_driver_memory_overhead)

        if severity > Severity.NONE:
            overall_info = f"Memory allocation limits were exceeded."
            details = MetricDetailsList()
            if severity_executor_memory > Severity.NONE:
                details.add(MetricDetails(detail_string=f"{EXECUTOR_MEMORY_KEY} = {fmt_bytes(executor_memory)}, "
                                                        f"but ideally, it should be <= {fmt_bytes(EXECUTOR_MEMORY_CONFIG_LOW)}",
                                          ))
            if severity_executor_memory_overhead > Severity.NONE:
                details.add(MetricDetails(detail_string=f"{EXECUTOR_MEMORY_OVERHEAD_KEY} = {fmt_bytes(executor_memory_overhead)}, "
                                                        f"but ideally, it should be <= {fmt_bytes(EXECUTOR_MEMORY_OVERHEAD_CONFIG_LOW)}",
                                          ))
            if severity_driver_memory > Severity.NONE:
                details.add(MetricDetails(detail_string=f"{DRIVER_MEMORY_KEY} = {fmt_bytes(driver_memory)}, "
                                                        f"but ideally, it should be <= {fmt_bytes(DRIVER_MEMORY_CONFIG_LOW)}",
                                          ))
            if severity_driver_memory_overhead > Severity.NONE:
                details.add(MetricDetails(detail_string=f"{DRIVER_MEMORY_OVERHEAD_KEY} = {fmt_bytes(driver_memory_overhead)}, "
                                                        f"but ideally, it should be <= {fmt_bytes(DRIVER_MEMORY_OVERHEAD_CONFIG_LOW)}",
                                          ))
            return MemoryConfigMetric(overall_info=overall_info, severity=severity, details=details)
        else:
            return EmptyMetric(severity=Severity.NONE)

    def analyze_core_number(self):
        """
        This method analyzes whether the number of cores per driver or executors was configured within defined limits.
        If any of the values gets out of limits, an issue is reported.
        :return: CoreNumberMetric if issue is detected or EmptyMetric if no issues are detected.
        """
        executor_cores = cast_to_int(self.app.get_spark_property(EXECUTOR_CORES_KEY)) or EXECUTOR_CORES_DEFAULT_VALUE
        driver_cores = cast_to_int(self.app.get_spark_property(DRIVER_CORES_KEY)) or DRIVER_CORES_DEFAULT_VALUE

        severity_executor_cores = EXECUTOR_CORES_THRESHOLDS.severity_of(executor_cores)
        severity_driver_cores = DRIVER_CORES_THRESHOLDS.severity_of(driver_cores)

        severity = max(severity_driver_cores, severity_executor_cores)

        if severity > Severity.NONE:
            overall_info = f"Number of cores per driver or executor might be not optimal"
            details = MetricDetailsList()
            if severity_executor_cores > Severity.NONE:
                details.add(MetricDetails(detail_string=f"{EXECUTOR_CORES_KEY} = {executor_cores}, but ideally, it should be between {EXECUTOR_CORES_THRESHOLDS.lower_threshold_low_severity} and {EXECUTOR_CORES_THRESHOLDS.upper_threshold_low_severity}."))
            if severity_driver_cores > Severity.NONE:
                details.add(MetricDetails(
                    detail_string=f"{DRIVER_CORES_KEY} = {driver_cores}, but ideally, it should be between {DRIVER_CORES_THRESHOLDS.lower_threshold_low_severity} and {DRIVER_CORES_THRESHOLDS.upper_threshold_low_severity}."))
            return CoreNumberMetric(overall_info=overall_info, severity=severity, details=details)
        else:
            return EmptyMetric(severity=Severity.NONE)
