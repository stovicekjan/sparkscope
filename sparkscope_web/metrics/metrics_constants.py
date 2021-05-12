import configparser
import os

from sparkscope_web.metrics.helpers import size_in_bytes
from sparkscope_web.metrics.thresholds import Thresholds, IntervalThresholds

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'user_config.conf'))

"""
The configuration parameter names in this file follow several patterns:
*_DEFAULT -> default value which is used as a fallback if no value is provided in the conf file.
*_LOW, *_LOW_DEFAULT -> threshold for low severity
*_HIGH, *_HIGH_DEFAULT -> threshold for high severity

"""


# ========== stage skew metric configuration ==========
# Ratio (maximum task duration)/(median task duration) within a single stage
STAGE_SKEW_RATIO_LOW_DEFAULT = 2.0
STAGE_SKEW_RATIO_HIGH_DEFAULT = 10.0
STAGE_SKEW_RATIO_LOW = config.getfloat("thresholds_stage_skew", "ratio_low", fallback=STAGE_SKEW_RATIO_LOW_DEFAULT)
STAGE_SKEW_RATIO_HIGH = config.getfloat("thresholds_stage_skew", "ratio_high", fallback=STAGE_SKEW_RATIO_HIGH_DEFAULT)

STAGE_SKEW_THRESHOLDS = Thresholds(STAGE_SKEW_RATIO_LOW, STAGE_SKEW_RATIO_HIGH, ascending=True)

# A minimum duration of a stage which should be investigated for a skew (perhaps you don't care about
# skews if the stage finishes in a few seconds)
STAGE_SKEW_MIN_RUNTIME_MILLIS_DEFAULT = 30000  # milliseconds
STAGE_SKEW_MIN_RUNTIME_MILLIS = config.getfloat("thresholds_stage_skew", "min_runtime",
                                                fallback=STAGE_SKEW_MIN_RUNTIME_MILLIS_DEFAULT)


# ========== stage disk spill metric configuration ==========
# Ratio (spilled memory)/(total memory used for input, output or shuffle) within a single stage
STAGE_DISK_SPILL_LOW_DEFAULT = 0.0
STAGE_DISK_SPILL_HIGH_DEFAULT = 0.0
STAGE_DISK_SPILL_LOW = config.getfloat("thresholds_stage_disk_spill", "ratio_low", fallback=STAGE_DISK_SPILL_LOW_DEFAULT)
STAGE_DISK_SPILL_HIGH = config.getfloat("thresholds_stage_disk_spill", "ratio_high", fallback=STAGE_DISK_SPILL_HIGH_DEFAULT)

STAGE_DISK_SPILL_THRESHOLDS = Thresholds(STAGE_DISK_SPILL_LOW, STAGE_DISK_SPILL_HIGH, ascending=True)


# ========== executor memory metric configuration ==========
# Ratio (allocated executor memory)/(executor peak memory + Spark reserved memory)
# where the reserved memory is hardcoded to 300 MB in Spark
EXECUTOR_MEMORY_RATIO_LOW_DEFAULT = 2.0
EXECUTOR_MEMORY_RATIO_HIGH_DEFAULT = 10.0
EXECUTOR_MEMORY_RATIO_LOW = config.getfloat("thresholds_executor_memory_wastage", "ratio_low",
                                            fallback=EXECUTOR_MEMORY_RATIO_LOW_DEFAULT)
EXECUTOR_MEMORY_RATIO_HIGH = config.getfloat("thresholds_executor_memory_wastage", "ratio_high",
                                             fallback=EXECUTOR_MEMORY_RATIO_HIGH_DEFAULT)
EXECUTOR_MEMORY_WASTAGE_THRESHOLDS = Thresholds(EXECUTOR_MEMORY_RATIO_LOW, EXECUTOR_MEMORY_RATIO_HIGH, ascending=True)

SPARK_RESERVED_MEMORY = 300 << 20  # 300 MB expressed in bytes
DEFAULT_EXECUTOR_MEMORY = 1 << 30  # 1 GB expressed in bytes


# ========== driver GC metric configuration ==========
# GC time / total driver time ratio
DRIVER_TOO_LOW_GC_RATIO_LOW_DEFAULT = 0.04
DRIVER_TOO_LOW_GC_RATIO_HIGH_DEFAULT = 0.01
DRIVER_TOO_HIGH_GC_RATIO_LOW_DEFAULT = 0.1
DRIVER_TOO_HIGH_GC_RATIO_HIGH_DEFAULT = 0.2

DRIVER_TOO_LOW_GC_RATIO_LOW = config.getfloat("thresholds_driver_gc", "low_ratio_low_severity",
                                              fallback=DRIVER_TOO_LOW_GC_RATIO_LOW_DEFAULT)
DRIVER_TOO_LOW_GC_RATIO_HIGH = config.getfloat("thresholds_driver_gc", "low_ratio_high_severity",
                                               fallback=DRIVER_TOO_LOW_GC_RATIO_HIGH_DEFAULT)
DRIVER_TOO_HIGH_GC_RATIO_LOW = config.getfloat("thresholds_driver_gc", "high_ratio_low_severity",
                                               fallback=DRIVER_TOO_HIGH_GC_RATIO_LOW_DEFAULT)
DRIVER_TOO_HIGH_GC_RATIO_HIGH = config.getfloat("thresholds_driver_gc", "high_ratio_high_severity",
                                                fallback=DRIVER_TOO_HIGH_GC_RATIO_HIGH_DEFAULT)

DRIVER_TOO_LOW_GC_THRESHOLDS = Thresholds(DRIVER_TOO_LOW_GC_RATIO_LOW, DRIVER_TOO_LOW_GC_RATIO_HIGH, ascending=False)
DRIVER_TOO_HIGH_GC_THRESHOLDS = Thresholds(DRIVER_TOO_HIGH_GC_RATIO_LOW, DRIVER_TOO_HIGH_GC_RATIO_HIGH, ascending=True)


# ========== executor GC metric configuration ==========
# GC time / total executor time ratio
EXECUTOR_TOO_LOW_GC_RATIO_LOW_DEFAULT = 0.04
EXECUTOR_TOO_LOW_GC_RATIO_HIGH_DEFAULT = 0.01
EXECUTOR_TOO_HIGH_GC_RATIO_LOW_DEFAULT = 0.1
EXECUTOR_TOO_HIGH_GC_RATIO_HIGH_DEFAULT = 0.2

EXECUTOR_TOO_LOW_GC_RATIO_LOW = config.getfloat("thresholds_executor_gc", "low_ratio_low_severity",
                                                fallback=EXECUTOR_TOO_LOW_GC_RATIO_LOW_DEFAULT)
EXECUTOR_TOO_LOW_GC_RATIO_HIGH = config.getfloat("thresholds_executor_gc", "low_ratio_high_severity",
                                                 fallback=EXECUTOR_TOO_LOW_GC_RATIO_HIGH_DEFAULT)
EXECUTOR_TOO_HIGH_GC_RATIO_LOW = config.getfloat("thresholds_executor_gc", "high_ratio_low_severity",
                                                 fallback=EXECUTOR_TOO_HIGH_GC_RATIO_LOW_DEFAULT)
EXECUTOR_TOO_HIGH_GC_RATIO_HIGH = config.getfloat("thresholds_executor_gc", "high_ratio_high_severity",
                                                  fallback=EXECUTOR_TOO_HIGH_GC_RATIO_HIGH_DEFAULT)

EXECUTOR_TOO_LOW_GC_THRESHOLDS = Thresholds(EXECUTOR_TOO_LOW_GC_RATIO_LOW, EXECUTOR_TOO_LOW_GC_RATIO_HIGH,
                                            ascending=False)
EXECUTOR_TOO_HIGH_GC_THRESHOLDS = Thresholds(EXECUTOR_TOO_HIGH_GC_RATIO_LOW, EXECUTOR_TOO_HIGH_GC_RATIO_HIGH,
                                             ascending=True)

# ========== serializer metric ==========
DEFAULT_SERIALIZER = "Java Serializer"
PREFERRED_SERIALIZER_DEFAULT = "org.apache.spark.serializer.KryoSerializer"
PREFERRED_SERIALIZER = config.get("config_serializer", "preferred_serializer", fallback=PREFERRED_SERIALIZER_DEFAULT)

# ========== dynamic allocation metric ==========
IS_DYNAMIC_ALLOCATION_PREFERRED = config.getboolean("configs_dynamic_allocation", "is_dynamic_allocation_preferred",
                                                    fallback=True)

# ========== min/max executors metric ==========
# minimum/maximum desired number of executors if dynamic allocation is used
DYNAMIC_ALLOCATION_MIN_EXECUTORS_LOW_DEFAULT = 1
DYNAMIC_ALLOCATION_MIN_EXECUTORS_HIGH_DEFAULT = 1
DYNAMIC_ALLOCATION_MAX_EXECUTORS_LOW_DEFAULT = 1000
DYNAMIC_ALLOCATION_MAX_EXECUTORS_HIGH_DEFAULT = 1000
DYNAMIC_ALLOCATION_MIN_EXECUTORS_LOW = config.getint("configs_dynamic_allocation_min_max_executors",
                                                     "min_executors_low_severity",
                                                     fallback=DYNAMIC_ALLOCATION_MIN_EXECUTORS_LOW_DEFAULT)
DYNAMIC_ALLOCATION_MIN_EXECUTORS_HIGH = config.getint("configs_dynamic_allocation_min_max_executors",
                                                      "min_executors_high_severity",
                                                      fallback=DYNAMIC_ALLOCATION_MIN_EXECUTORS_HIGH_DEFAULT)
DYNAMIC_ALLOCATION_MAX_EXECUTORS_LOW = config.getint("configs_dynamic_allocation_min_max_executors",
                                                     "max_executors_low_severity",
                                                     fallback=DYNAMIC_ALLOCATION_MAX_EXECUTORS_LOW_DEFAULT)
DYNAMIC_ALLOCATION_MAX_EXECUTORS_HIGH = config.getint("configs_dynamic_allocation_min_max_executors",
                                                      "max_executors_high_severity",
                                                      fallback=DYNAMIC_ALLOCATION_MAX_EXECUTORS_HIGH_DEFAULT)
DYNAMIC_ALLOCATION_MIN_EXECUTORS_THRESHOLDS = Thresholds(DYNAMIC_ALLOCATION_MIN_EXECUTORS_LOW,
                                                         DYNAMIC_ALLOCATION_MIN_EXECUTORS_HIGH, ascending=True)
DYNAMIC_ALLOCATION_MAX_EXECUTORS_THRESHOLDS = Thresholds(DYNAMIC_ALLOCATION_MAX_EXECUTORS_LOW,
                                                         DYNAMIC_ALLOCATION_MAX_EXECUTORS_HIGH, ascending=True)

# ========== YARN queue metric ==========
IS_DEFAULT_QUEUE_ALLOWED = config.getboolean("configs_yarn_queue", "is_default_queue_allowed", fallback=True)

# ========== memory config metric ==========
# maximum allowed driver/executor memory
EXECUTOR_MEMORY_CONFIG_LOW_DEFAULT = 10 << 30  # 10 GB
EXECUTOR_MEMORY_CONFIG_HIGH_DEFAULT = 16 << 30  # 16 GB
EXECUTOR_MEMORY_OVERHEAD_CONFIG_LOW_DEFAULT = 4 << 30  # 4 GB
EXECUTOR_MEMORY_OVERHEAD_CONFIG_HIGH_DEFAULT = 6 << 30  # 6 GB
DRIVER_MEMORY_CONFIG_LOW_DEFAULT = 8 << 30  # 8 GB
DRIVER_MEMORY_CONFIG_HIGH_DEFAULT = 12 << 30  # 12 GB
DRIVER_MEMORY_OVERHEAD_CONFIG_LOW_DEFAULT = 1 << 30  # 1 GB
DRIVER_MEMORY_OVERHEAD_CONFIG_HIGH_DEFAULT = 2 << 30  # 2 GB

EXECUTOR_MEMORY_CONFIG_LOW = size_in_bytes(config.get("configs_memory_settings", "max_executor_memory_low_severity",
                                                      fallback=str(EXECUTOR_MEMORY_CONFIG_LOW_DEFAULT)),
                                           default=EXECUTOR_MEMORY_CONFIG_LOW_DEFAULT)
EXECUTOR_MEMORY_CONFIG_HIGH = size_in_bytes(config.get("configs_memory_settings", "max_executor_memory_high_severity",
                                                       fallback=str(EXECUTOR_MEMORY_CONFIG_HIGH_DEFAULT)),
                                            default=EXECUTOR_MEMORY_CONFIG_HIGH_DEFAULT)
EXECUTOR_MEMORY_OVERHEAD_CONFIG_LOW = size_in_bytes(config.get("configs_memory_settings",
                                                               "max_executor_memory_overhead_low_severity",
                                                               fallback=str(EXECUTOR_MEMORY_OVERHEAD_CONFIG_LOW_DEFAULT)),
                                                    default=EXECUTOR_MEMORY_OVERHEAD_CONFIG_LOW_DEFAULT)
EXECUTOR_MEMORY_OVERHEAD_CONFIG_HIGH = size_in_bytes(config.get("configs_memory_settings",
                                                                "max_executor_memory_overhead_high_severity",
                                                                fallback=str(EXECUTOR_MEMORY_OVERHEAD_CONFIG_HIGH_DEFAULT)),
                                                     default=EXECUTOR_MEMORY_OVERHEAD_CONFIG_HIGH_DEFAULT)
DRIVER_MEMORY_CONFIG_LOW = size_in_bytes(config.get("configs_memory_settings", "max_driver_memory_low_severity",
                                                    fallback=str(DRIVER_MEMORY_CONFIG_LOW_DEFAULT)),
                                         default=DRIVER_MEMORY_CONFIG_LOW_DEFAULT)
DRIVER_MEMORY_CONFIG_HIGH = size_in_bytes(config.get("configs_memory_settings", "max_driver_memory_high_severity",
                                                     fallback=str(DRIVER_MEMORY_CONFIG_HIGH_DEFAULT)),
                                          default=DRIVER_MEMORY_CONFIG_HIGH_DEFAULT)
DRIVER_MEMORY_OVERHEAD_CONFIG_LOW = size_in_bytes(config.get("configs_memory_settings",
                                                             "max_driver_memory_overhead_low_severity",
                                                             fallback=str(DRIVER_MEMORY_OVERHEAD_CONFIG_LOW_DEFAULT)),
                                                  default=DRIVER_MEMORY_OVERHEAD_CONFIG_LOW_DEFAULT)
DRIVER_MEMORY_OVERHEAD_CONFIG_HIGH = size_in_bytes(config.get("configs_memory_settings",
                                                              "max_driver_memory_overhead_high_severity",
                                                              fallback=str(DRIVER_MEMORY_OVERHEAD_CONFIG_HIGH_DEFAULT)),
                                                   default=DRIVER_MEMORY_OVERHEAD_CONFIG_HIGH_DEFAULT)
EXECUTOR_MEMORY_CONFIG_THRESHOLDS = Thresholds(EXECUTOR_MEMORY_CONFIG_LOW, EXECUTOR_MEMORY_CONFIG_HIGH, ascending=True)
EXECUTOR_MEMORY_OVERHEAD_CONFIG_THRESHOLDS = Thresholds(EXECUTOR_MEMORY_OVERHEAD_CONFIG_LOW,
                                                        EXECUTOR_MEMORY_OVERHEAD_CONFIG_HIGH, ascending=True)
DRIVER_MEMORY_CONFIG_THRESHOLDS = Thresholds(DRIVER_MEMORY_CONFIG_LOW, DRIVER_MEMORY_CONFIG_HIGH, ascending=True)
DRIVER_MEMORY_CONFIG_OVERHEAD_THRESHOLDS = Thresholds(DRIVER_MEMORY_OVERHEAD_CONFIG_LOW,
                                                      DRIVER_MEMORY_OVERHEAD_CONFIG_HIGH, ascending=True)

# ========== core number metric ==========
# minimum/maximum allowed number of cores per driver/executor
EXECUTOR_CORES_LOW_LIMIT_LOW_SEVERITY_DEFAULT = 3
EXECUTOR_CORES_LOW_LIMIT_HIGH_SEVERITY_DEFAULT = 2
EXECUTOR_CORES_HIGH_LIMIT_LOW_SEVERITY_DEFAULT = 5
EXECUTOR_CORES_HIGH_LIMIT_HIGH_SEVERITY_DEFAULT = 6
DRIVER_CORES_LOW_LIMIT_LOW_SEVERITY_DEFAULT = 3
DRIVER_CORES_LOW_LIMIT_HIGH_SEVERITY_DEFAULT = 2
DRIVER_CORES_HIGH_LIMIT_LOW_SEVERITY_DEFAULT = 5
DRIVER_CORES_HIGH_LIMIT_HIGH_SEVERITY_DEFAULT = 6

EXECUTOR_CORES_LOW_LIMIT_LOW_SEVERITY = config.getint("configs_cores", "executor_low_limit_low_severity",
                                                      fallback=EXECUTOR_CORES_LOW_LIMIT_LOW_SEVERITY_DEFAULT)
EXECUTOR_CORES_LOW_LIMIT_HIGH_SEVERITY = config.getint("configs_cores", "executor_low_limit_high_severity",
                                                       fallback=EXECUTOR_CORES_LOW_LIMIT_HIGH_SEVERITY_DEFAULT)
EXECUTOR_CORES_HIGH_LIMIT_LOW_SEVERITY = config.getint("configs_cores", "executor_high_limit_low_severity",
                                                       fallback=EXECUTOR_CORES_HIGH_LIMIT_LOW_SEVERITY_DEFAULT)
EXECUTOR_CORES_HIGH_LIMIT_HIGH_SEVERITY = config.getint("configs_cores", "executor_high_limit_high_severity",
                                                        fallback=EXECUTOR_CORES_HIGH_LIMIT_HIGH_SEVERITY_DEFAULT)
EXECUTOR_CORES_THRESHOLDS = IntervalThresholds(EXECUTOR_CORES_LOW_LIMIT_HIGH_SEVERITY,
                                               EXECUTOR_CORES_LOW_LIMIT_LOW_SEVERITY,
                                               EXECUTOR_CORES_HIGH_LIMIT_LOW_SEVERITY,
                                               EXECUTOR_CORES_HIGH_LIMIT_HIGH_SEVERITY)
DRIVER_CORES_LOW_LIMIT_LOW_SEVERITY = config.getint("configs_cores", "driver_low_limit_low_severity",
                                                    fallback=DRIVER_CORES_LOW_LIMIT_LOW_SEVERITY_DEFAULT)
DRIVER_CORES_LOW_LIMIT_HIGH_SEVERITY = config.getint("configs_cores", "driver_low_limit_high_severity",
                                                     fallback=DRIVER_CORES_LOW_LIMIT_HIGH_SEVERITY_DEFAULT)
DRIVER_CORES_HIGH_LIMIT_LOW_SEVERITY = config.getint("configs_cores", "driver_high_limit_low_severity",
                                                     fallback=DRIVER_CORES_HIGH_LIMIT_LOW_SEVERITY_DEFAULT)
DRIVER_CORES_HIGH_LIMIT_HIGH_SEVERITY = config.getint("configs_cores", "driver_high_limit_high_severity",
                                                      fallback=DRIVER_CORES_HIGH_LIMIT_HIGH_SEVERITY_DEFAULT)
DRIVER_CORES_THRESHOLDS = IntervalThresholds(DRIVER_CORES_LOW_LIMIT_HIGH_SEVERITY, DRIVER_CORES_LOW_LIMIT_LOW_SEVERITY,
                                             DRIVER_CORES_HIGH_LIMIT_LOW_SEVERITY, DRIVER_CORES_HIGH_LIMIT_HIGH_SEVERITY)

# ========== lengths of the readable metric lists ==========
STAGE_FAILURE_READABLE_LIST_LENGTH = config.getint("readable_list_length", "stage_failure_readable_list_length",
                                                   fallback=-1)
STAGE_SKEW_READABLE_LIST_LENGTH = config.getint("readable_list_length", "stage_skew_readable_list_length", fallback=-1)
STAGE_DISK_SPILL_READABLE_LIST_LENGTH = config.getint("readable_list_length", "stage_disk_spill_readable_list_length",
                                                      fallback=-1)
JOB_FAILURE_READABLE_LIST_LENGTH = config.getint("readable_list_length", "job_failure_readable_list_length",
                                                 fallback=-1)
DRIVER_GC_READABLE_LIST_LENGTH = config.getint("readable_list_length", "driver_gc_readable_list_length", fallback=-1)
EXECUTOR_GC_READABLE_LIST_LENGTH = config.getint("readable_list_length", "executor_gc_readable_list_length",
                                                 fallback=-1)
SERIALIZER_CONFIG_READABLE_LIST_LENGTH = config.getint("readable_list_length", "serializer_config_readable_list_length",
                                                       fallback=-1)
DYNAMIC_ALLOCATION_CONFIG_READABLE_LIST_LENGTH = config.getint("readable_list_length",
                                                               "dynamic_allocation_config_readable_list_length",
                                                               fallback=-1)

