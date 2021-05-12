# Spark configuration keys
DRIVER_MEMORY_KEY = "spark.driver.memory"
DRIVER_MEMORY_OVERHEAD_KEY = "spark.driver.memoryOverhead"
EXECUTOR_MEMORY_KEY = "spark.executor.memory"
EXECUTOR_MEMORY_OVERHEAD_KEY = "spark.executor.memoryOverhead"
DRIVER_MEMORY_DEFAULT_VALUE = 1 << 30  # 1 GB
EXECUTOR_MEMORY_DEFAULT_VALUE = 1 << 30  # 1 GB


def get_default_memory_overhead(memory):
    """
    The default memory overhead (related to both driver and executor) depends on how much memory is used: 0.10*memory,
    with minimum of 384 MB.
    :param memory: driver or executor memory
    :return: default memory overhead
    """
    MINIMUM_OVERHEAD = 384 << 20  # 384 MB
    return 0.1*memory if 0.1*memory > MINIMUM_OVERHEAD else MINIMUM_OVERHEAD


DRIVER_MAX_RESULT_SIZE_KEY = "spark.driver.maxResultSize"
EXECUTOR_INSTANCES_KEY = "spark.executor.instances"

SERIALIZER_KEY = "spark.serializer"

DYNAMIC_ALLOCATION_KEY = "spark.dynamicAllocation.enabled"
SHUFFLE_TRACKING_ENABLED_KEY = "spark.dynamicAllocation.shuffleTracking.enabled"
SHUFFLE_SERVICE_ENABLED_KEY = "spark.shuffle.service.enabled"

DYNAMIC_ALLOCATION_MIN_EXECUTORS_KEY = "spark.dynamicAllocation.minExecutors"
DYNAMIC_ALLOCATION_MAX_EXECUTORS_KEY = "spark.dynamicAllocation.maxExecutors"

SPARK_YARN_QUEUE_KEY = "spark.yarn.queue"
SPARK_YARN_QUEUE_DEFAULT_VALUE = "default"

EXECUTOR_CORES_KEY = "spark.executor.cores"
DRIVER_CORES_KEY = "spark.driver.cores"
EXECUTOR_CORES_DEFAULT_VALUE = 1
DRIVER_CORES_DEFAULT_VALUE = 1
