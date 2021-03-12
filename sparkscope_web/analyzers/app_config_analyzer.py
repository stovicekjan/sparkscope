from sparkscope_web.analyzers.analyzer import Analyzer
from sparkscope_web.constants import SERIALIZER_KEY, SHUFFLE_SERVICE_ENABLED_KEY, SHUFFLE_TRACKING_ENABLED_KEY, \
    DYNAMIC_ALLOCATION_KEY
from sparkscope_web.metrics.helpers import cast_to_bool
from sparkscope_web.metrics.metric import EmptyMetric, SerializerConfigMetric, DynamicAllocationConfigMetric
from sparkscope_web.metrics.metric_details import MetricDetailsList, MetricDetails
from sparkscope_web.metrics.metrics_constants import DEFAULT_SERIALIZER, PREFERRED_SERIALIZER, \
    IS_DYNAMIC_ALLOCATION_PREFERRED
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
            is_shuffle_tracking_enabled = cast_to_bool(self.app.get_spark_property(SHUFFLE_TRACKING_ENABLED_KEY))
            is_shuffle_service_enabled = cast_to_bool(self.app.get_spark_property(SHUFFLE_SERVICE_ENABLED_KEY))
        except ValueError as e:
            # TODO add logging
            is_dynamic_allocation_enabled = False
            is_shuffle_tracking_enabled = False
            is_shuffle_service_enabled = False

        if IS_DYNAMIC_ALLOCATION_PREFERRED and not is_dynamic_allocation_enabled:
            overall_info = f"Dynamic allocation was disabled for this application."
            return DynamicAllocationConfigMetric(severity=Severity.HIGH, overall_info=overall_info, details=MetricDetailsList())

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
