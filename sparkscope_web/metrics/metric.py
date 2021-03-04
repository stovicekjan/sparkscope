from sparkscope_web.metrics.severity import Severity


class AbstractMetric:
    def __init__(self):
        pass


class Metric(AbstractMetric):
    """
    Generic class for non-empty Metrics
    """
    def __init__(self, severity, overall_info, details):
        super().__init__()
        self.severity = severity
        self.overall_info = overall_info
        self.details = details


class EmptyMetric(AbstractMetric):
    """
    Dummy object for Metrics with Severity NONE (without issues)
    """
    def __init__(self, severity):
        super().__init__()
        self.severity = severity


class StageFailureMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create StageFailureMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (how many stages failed)
        :param details: detailed info (which stages failed and why)
        """
        super().__init__(severity, overall_info, details)
        self.title = "Failed Stages"


class StageSkewMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create StageSkewMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result
        :param details: detailed info (which stages are skewed and how)
        """
        super().__init__(severity, overall_info, details)
        self.title = "Stage Skew"


class StageDiskSpillMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create StageDiskSpillMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result
        :param details: detailed info (which stages are skewed and how)
        """
        super().__init__(severity, overall_info, details)
        self.title = "Disk Spill"


class JobFailureMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create StageFailureMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (how many stages failed)
        :param details: detailed info (which stages failed and why)
        """
        super().__init__(severity, overall_info, details)
        self.title = "Failed Jobs"


class DriverGcTimeMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create DriverGcTimeMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (Driver spends too much/low time with GC + how much)
        :param details: detailed info (empty dict so far)
        """
        super().__init__(severity, overall_info, details)
        self.title = "Driver GC Time"


class ExecutorGcTimeMetric(Metric):
    def __init__(self, severity, overall_info, details):
        """
        Create ExecutorGcTimeMetric object
        :param severity: Severity enum object
        :param overall_info: high level info about metric result (Executors spend too much/low time with GC + how much)
        :param details: detailed info (empty dict so far)
        """
        super().__init__(severity, overall_info, details)
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
        super().__init__(severity, overall_info, details)
        self.title = "Serializer"
