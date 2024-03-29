from sparkscope_web.metrics.metrics_constants import STAGE_FAILURE_READABLE_LIST_LENGTH, \
    STAGE_SKEW_READABLE_LIST_LENGTH, STAGE_DISK_SPILL_READABLE_LIST_LENGTH, JOB_FAILURE_READABLE_LIST_LENGTH, \
    DRIVER_GC_READABLE_LIST_LENGTH, EXECUTOR_GC_READABLE_LIST_LENGTH
from sparkscope_web.metrics.severity import Severity


class AbstractMetric:
    """
    Abstract class for holding the metrics.
    """
    def __init__(self, severity):
        self.severity = severity


class Metric(AbstractMetric):
    """
    Generic class for non-empty Metrics
    """
    def __init__(self, severity, overall_info, details, length):
        super().__init__(severity)
        self.overall_info = overall_info
        self.details = details
        self.details.set_readable_list_length(length)


class EmptyMetric(AbstractMetric):
    """
    Dummy object for Metrics with Severity NONE (without issues)
    """
    def __init__(self, severity=Severity.NONE):
        super().__init__(severity)


class StageFailureMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create StageFailureMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (how many stages failed)
        :param details: detailed info (which stages failed and why)
        """
        super().__init__(severity, overall_info, details, STAGE_FAILURE_READABLE_LIST_LENGTH)
        self.title = "Failed Stages"


class StageSkewMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create StageSkewMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result
        :param details: detailed info (which stages are skewed and how)
        """
        super().__init__(severity, overall_info, details, STAGE_SKEW_READABLE_LIST_LENGTH)
        self.title = "Stage Skew"


class StageDiskSpillMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create StageDiskSpillMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result
        :param details: detailed info (which stages are skewed and how)
        """
        super().__init__(severity, overall_info, details, STAGE_DISK_SPILL_READABLE_LIST_LENGTH)
        self.title = "Disk Spill"


class JobFailureMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create StageFailureMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (how many stages failed)
        :param details: detailed info (which stages failed and why)
        """
        super().__init__(severity, overall_info, details, JOB_FAILURE_READABLE_LIST_LENGTH)
        self.title = "Failed Jobs"


class DriverGcTimeMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create DriverGcTimeMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (Driver spends too much/low time with GC + how much)
        :param details: detailed info (empty dict so far)
        """
        super().__init__(severity, overall_info, details, DRIVER_GC_READABLE_LIST_LENGTH)
        self.title = "Driver GC Time"


class ExecutorGcTimeMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create ExecutorGcTimeMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (Executors spend too much/low time with GC + how much)
        :param details: detailed info (empty dict so far)
        """
        super().__init__(severity, overall_info, details, EXECUTOR_GC_READABLE_LIST_LENGTH)
        self.title = "Executors GC Time"


class SerializerConfigMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create SerializerConfigMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (which serializer was used and which one should have
        been used)
        :param details: detailed info (empty dict so far)
        """
        super().__init__(severity, overall_info, details, length=-1)
        self.title = "Serializer"


class DynamicAllocationConfigMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create DynamicAllocationConfigMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (Dynamic allocation is either disabled or
        misconfigured)
        :param details: detailed info (empty dict so far)
        """
        super().__init__(severity, overall_info, details, length=-1)
        self.title = "Dynamic Allocation"


class DynamicAllocationMinMaxExecutorsMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create DynamicAllocationMinMaxExecutorsMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (min or max executors number is misconfigured)
        :param details: detailed info (Which parameters are misconfigured)
        """
        super().__init__(severity, overall_info, details, length=-1)
        self.title = "Min/Max Executors"


class YarnQueueMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create YarnQueueMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (Default YARN queue is used, although it should not be)
        :param details: detailed info (empty)
        """
        super().__init__(severity, overall_info, details, length=-1)
        self.title = "YARN Queue"


class MemoryConfigMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create MemoryConfigMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (The requested memory exceeded the recommended limit)
        :param details: detailed info (specific attributes which exceeded the limits)
        """
        super().__init__(severity, overall_info, details, length=-1)
        self.title = "Memory Config"


class CoreNumberMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create CoreNumberMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (The number of executor or driver cores is not set
        optimally)
        :param details: detailed info (which properties are not optimal)
        """
        super().__init__(severity, overall_info, details, length=-1)
        self.title = "Number of Cores"
