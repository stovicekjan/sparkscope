import configparser

import os

from sparkscope_web.metrics.thresholds import Thresholds

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'thresholds.conf'))


# stage skew configuration
STAGE_SKEW_RATIO_LOW_DEFAULT = 2.0
STAGE_SKEW_RATIO_HIGH_DEFAULT = 1000000.0

STAGE_SKEW_RATIO_LOW = config.getfloat("stage_skew", "ratio_low", fallback=STAGE_SKEW_RATIO_LOW_DEFAULT)
STAGE_SKEW_RATIO_HIGH = config.getfloat("stage_skew", "ratio_high", fallback=STAGE_SKEW_RATIO_HIGH_DEFAULT)

STAGE_SKEW_THRESHOLDS = Thresholds(STAGE_SKEW_RATIO_LOW, STAGE_SKEW_RATIO_HIGH, ascending=True)

STAGE_SKEW_MIN_RUNTIME_MILLIS_DEFAULT = 30000
STAGE_SKEW_MIN_RUNTIME_MILLIS = config.getfloat("stage_skew", "min_runtime",
                                                fallback=STAGE_SKEW_MIN_RUNTIME_MILLIS_DEFAULT)


# stage disk spill configuration
STAGE_DISK_SPILL_LOW_DEFAULT = 0.0
STAGE_DISK_SPILL_HIGH_DEFAULT = 0.0

STAGE_DISK_SPILL_LOW = config.getfloat("stage_disk_spill", "ratio_low", fallback=STAGE_DISK_SPILL_LOW_DEFAULT)
STAGE_DISK_SPILL_HIGH = config.getfloat("stage_disk_spill", "ratio_high", fallback=STAGE_DISK_SPILL_HIGH_DEFAULT)

STAGE_DISK_SPILL_THRESHOLDS = Thresholds(STAGE_DISK_SPILL_LOW, STAGE_DISK_SPILL_HIGH, ascending=True)