import configparser
import os

from sparkscope_web.metrics.thresholds import Thresholds

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'thresholds.conf'))


# ========== stage skew metric configuration ==========
# Ratio (maximum task duration)/(median task duration) within a single stage
STAGE_SKEW_RATIO_LOW_DEFAULT = 2.0
STAGE_SKEW_RATIO_HIGH_DEFAULT = 10.0
STAGE_SKEW_RATIO_LOW = config.getfloat("stage_skew", "ratio_low", fallback=STAGE_SKEW_RATIO_LOW_DEFAULT)
STAGE_SKEW_RATIO_HIGH = config.getfloat("stage_skew", "ratio_high", fallback=STAGE_SKEW_RATIO_HIGH_DEFAULT)

STAGE_SKEW_THRESHOLDS = Thresholds(STAGE_SKEW_RATIO_LOW, STAGE_SKEW_RATIO_HIGH, ascending=True)

# A minimum duration of a stage which should be investigated for a skew (perhaps you don't care about
# skews if the stage finishes in a few seconds)
STAGE_SKEW_MIN_RUNTIME_MILLIS_DEFAULT = 30000  # milliseconds
STAGE_SKEW_MIN_RUNTIME_MILLIS = config.getfloat("stage_skew", "min_runtime",
                                                fallback=STAGE_SKEW_MIN_RUNTIME_MILLIS_DEFAULT)


# ========== stage disk spill metric configuration ==========
# Ratio (spilled memory)/(total memory used for input, output or shuffle) within a single stage
STAGE_DISK_SPILL_LOW_DEFAULT = 0.0
STAGE_DISK_SPILL_HIGH_DEFAULT = 0.0
STAGE_DISK_SPILL_LOW = config.getfloat("stage_disk_spill", "ratio_low", fallback=STAGE_DISK_SPILL_LOW_DEFAULT)
STAGE_DISK_SPILL_HIGH = config.getfloat("stage_disk_spill", "ratio_high", fallback=STAGE_DISK_SPILL_HIGH_DEFAULT)

STAGE_DISK_SPILL_THRESHOLDS = Thresholds(STAGE_DISK_SPILL_LOW, STAGE_DISK_SPILL_HIGH, ascending=True)


# ========== executor memory metric configuration ==========
# Ratio (allocated executor memory)/(executor peak memory + Spark reserved memory)
# where the reserved memory is hardcoded to 300 MB in Spark
EXECUTOR_MEMORY_RATIO_LOW_DEFAULT = 2.0
EXECUTOR_MEMORY_RATIO_HIGH_DEFAULT = 10.0
EXECUTOR_MEMORY_RATIO_LOW = config.getfloat("executor_memory_wastage", "ratio_low",
                                            fallback=EXECUTOR_MEMORY_RATIO_LOW_DEFAULT)
EXECUTOR_MEMORY_RATIO_HIGH = config.getfloat("executor_memory_wastage", "ratio_high",
                                             fallback=EXECUTOR_MEMORY_RATIO_HIGH_DEFAULT)
EXECUTOR_MEMORY_WASTAGE_THRESHOLDS = Thresholds(EXECUTOR_MEMORY_RATIO_LOW, EXECUTOR_MEMORY_RATIO_HIGH, ascending=True)

SPARK_RESERVED_MEMORY = 300 << 20  # 300 MB expressed in bytes
DEFAULT_EXECUTOR_MEMORY = 1 << 30  # 1 GB expressed in bytes
EXECUTOR_MEMORY_CONFIG_KEY = "spark.executor.memory"


# ========== driver GC metric configuration ==========
DRIVER_TOO_LOW_GC_RATIO_LOW_DEFAULT = 0.04
DRIVER_TOO_LOW_GC_RATIO_HIGH_DEFAULT = 0.01
DRIVER_TOO_HIGH_GC_RATIO_LOW_DEFAULT = 0.1
DRIVER_TOO_HIGH_GC_RATIO_HIGH_DEFAULT = 0.2

DRIVER_TOO_LOW_GC_RATIO_LOW = config.getfloat("driver_gc", "low_ratio_low_severity",
                                              fallback=DRIVER_TOO_LOW_GC_RATIO_LOW_DEFAULT)
DRIVER_TOO_LOW_GC_RATIO_HIGH = config.getfloat("driver_gc", "low_ratio_high_severity",
                                               fallback=DRIVER_TOO_LOW_GC_RATIO_HIGH_DEFAULT)
DRIVER_TOO_HIGH_GC_RATIO_LOW = config.getfloat("driver_gc", "high_ratio_low_severity",
                                               fallback=DRIVER_TOO_HIGH_GC_RATIO_LOW_DEFAULT)
DRIVER_TOO_HIGH_GC_RATIO_HIGH = config.getfloat("driver_gc", "high_ratio_high_severity",
                                                fallback=DRIVER_TOO_HIGH_GC_RATIO_HIGH_DEFAULT)

DRIVER_TOO_LOW_GC_THRESHOLDS = Thresholds(DRIVER_TOO_LOW_GC_RATIO_LOW, DRIVER_TOO_LOW_GC_RATIO_HIGH, ascending=False)
DRIVER_TOO_HIGH_GC_THRESHOLDS = Thresholds(DRIVER_TOO_HIGH_GC_RATIO_LOW, DRIVER_TOO_HIGH_GC_RATIO_HIGH, ascending=True)


# ========== executor GC metric configuration ==========
EXECUTOR_TOO_LOW_GC_RATIO_LOW_DEFAULT = 0.04
EXECUTOR_TOO_LOW_GC_RATIO_HIGH_DEFAULT = 0.01
EXECUTOR_TOO_HIGH_GC_RATIO_LOW_DEFAULT = 0.1
EXECUTOR_TOO_HIGH_GC_RATIO_HIGH_DEFAULT = 0.2

EXECUTOR_TOO_LOW_GC_RATIO_LOW = config.getfloat("executor_gc", "low_ratio_low_severity",
                                              fallback=EXECUTOR_TOO_LOW_GC_RATIO_LOW_DEFAULT)
EXECUTOR_TOO_LOW_GC_RATIO_HIGH = config.getfloat("executor_gc", "low_ratio_high_severity",
                                               fallback=EXECUTOR_TOO_LOW_GC_RATIO_HIGH_DEFAULT)
EXECUTOR_TOO_HIGH_GC_RATIO_LOW = config.getfloat("executor_gc", "high_ratio_low_severity",
                                               fallback=EXECUTOR_TOO_HIGH_GC_RATIO_LOW_DEFAULT)
EXECUTOR_TOO_HIGH_GC_RATIO_HIGH = config.getfloat("executor_gc", "high_ratio_high_severity",
                                                fallback=EXECUTOR_TOO_HIGH_GC_RATIO_HIGH_DEFAULT)

EXECUTOR_TOO_LOW_GC_THRESHOLDS = Thresholds(EXECUTOR_TOO_LOW_GC_RATIO_LOW, EXECUTOR_TOO_LOW_GC_RATIO_HIGH,
                                            ascending=False)
EXECUTOR_TOO_HIGH_GC_THRESHOLDS = Thresholds(EXECUTOR_TOO_HIGH_GC_RATIO_LOW, EXECUTOR_TOO_HIGH_GC_RATIO_HIGH,
                                             ascending=True)