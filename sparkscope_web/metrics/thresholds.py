import configparser

import os

from sparkscope_web.metrics.severity import Severity


class Thresholds:
    def __init__(self, threshold_low, threshold_high, ascending):
        """
        Class for storage of thresholds for Metric Severities.
        :param threshold_low: threshold for Severity.LOW
        :param threshold_high: threshold for Severity.HIGH
        :param ascending: True if "the greater, the worse" applies. Otherwise, false
        """
        self.threshold_low = threshold_low
        self.threshold_high = threshold_high
        self.ascending = ascending

    def severity_of(self, value):
        """
        Get Severity for a given value, based on the thresholds
        :param value: value of a metric
        :return: Severity
        """
        if self.ascending:
            if value > self.threshold_high:
                return Severity.HIGH
            elif value > self.threshold_low:
                return Severity.LOW
            else:
                return Severity.NONE
        else:
            if value < self.threshold_high:
                return Severity.HIGH
            elif value < self.threshold_low:
                return Severity.LOW
            else:
                return Severity.NONE


class IntervalThresholds:
    def __init__(self, lower_threshold_high_severity, lower_threshold_low_severity, upper_threshold_low_severity,
                 upper_threshold_high_severity):
        """
        Class for storage of interval thresholds for Metric Severities. Ideally, the value of the criterion should be
        between lower_threshold_low_severity and higher_threshold_low_severity.
        :param lower_threshold_high_severity: lower bound threshold, Severity.HIGH
        :param lower_threshold_low_severity: lower bound threshold, Severity.LOW
        :param upper_threshold_low_severity: upper bound threshold, Severity.LOW
        :param upper_threshold_high_severity: upper bound threshold, Severity.HIGH
        """
        self.lower_threshold_high_severity = lower_threshold_high_severity
        self.lower_threshold_low_severity = lower_threshold_low_severity
        self.upper_threshold_low_severity = upper_threshold_low_severity
        self.upper_threshold_high_severity = upper_threshold_high_severity

    def severity_of(self, value):
        """
        Get Severity for a given value, based on the thresholds
        :param value: value of a metric
        :return: Severity
        """
        if self.lower_threshold_low_severity <= value <= self.upper_threshold_low_severity:
            return Severity.NONE
        elif self.lower_threshold_high_severity <= value <= self.upper_threshold_high_severity:
            return Severity.LOW
        else:
            return Severity.HIGH


