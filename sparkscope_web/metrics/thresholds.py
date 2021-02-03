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


# TODO check if LOW threshold is more strict than HIGH threshold


