# coding=utf-8

from sqlalchemy import Column, String, DateTime, BigInteger, Boolean, JSON, orm, func
from db.base import Base, Session
from db.entities.executor import ExecutorEntity
from db.entities.job import JobEntity
from db.entities.stage import StageEntity
from db.entities.task import TaskEntity
from history_fetcher.utils import get_prop, get_system_property
from sparkscope_web.analyzers.app_config_analyzer import AppConfigAnalyzer
from sparkscope_web.analyzers.executor_analyzer import ExecutorAnalyzer
from sparkscope_web.analyzers.job_analyzer import JobAnalyzer
from sparkscope_web.analyzers.stage_analyzer import StageAnalyzer
from sparkscope_web.metrics.severity import Severity
from sparkscope_web.metrics.helpers import fmt_time, fmt_bytes, cast_or_none
from sparkscope_web.constants import DRIVER_MEMORY_KEY, DRIVER_MEMORY_OVERHEAD_KEY, EXECUTOR_MEMORY_KEY, \
    EXECUTOR_MEMORY_OVERHEAD_KEY, DRIVER_MAX_RESULT_SIZE_KEY, EXECUTOR_CORES_KEY, EXECUTOR_INSTANCES_KEY, \
    DYNAMIC_ALLOCATION_KEY


class ApplicationEntity(Base):
    """
    A class used to represent the Application entity in the database.

    Each Spark application should be represented by one record in the Application table.
    """
    __tablename__ = 'application'

    app_id = Column(String, primary_key=True)
    name = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    duration = Column(BigInteger)
    spark_user = Column(String)
    completed = Column(Boolean)
    runtime = Column(JSON)
    spark_properties = Column(JSON)
    spark_command = Column(String)
    mode = Column(String)

    def __init__(self, attributes):
        """
        Create an Application object.
        :param attributes: dictionary {name: value} containing the attributes
        """

        self.app_id = get_prop(attributes, 'app_id')
        self.name = get_prop(attributes, 'name')
        self.start_time = get_prop(attributes, 'start_time')
        self.end_time = get_prop(attributes, 'end_time')
        self.duration = get_prop(attributes, 'duration')
        self.spark_user = get_prop(attributes, 'spark_user')
        self.completed = get_prop(attributes, 'completed')
        self.runtime = get_prop(attributes, 'runtime')
        self.spark_properties = get_prop(attributes, 'spark_properties')
        self.spark_command = get_prop(attributes, 'spark_command')
        self.mode = get_prop(attributes, 'mode')

        self.is_processed = False

        self.stage_failure_metric = None
        self.stage_skew_metric = None
        self.stage_disk_spill_metric = None
        self.job_metrics = None
        self.driver_gc_time_metric = None
        self.executor_gc_time_metric = None
        self.executor_gc_time_metric = None
        self.serializer_metric = None
        self.dynamic_allocation_metric = None

        self.duration_formatted = fmt_time(self.duration/1000)

        # this dict should serve as an overview of metrics with severity >= LOW
        self.metrics_overview = {}  # key: metric, value: severity

        self.overall_severity = Severity.NONE

    @staticmethod
    def get_fetch_dict(app, app_env_data):
        return {
            'app_id': app['id'],
            'name': app['name'],
            'start_time': app['attempts'][0]['startTime'],
            'end_time': app['attempts'][0]['endTime'],
            'duration': app['attempts'][0]['duration'],
            'spark_user': app['attempts'][0]['sparkUser'],
            'completed': app['attempts'][0]['completed'],
            'runtime': get_prop(app_env_data, 'runtime'),
            'spark_properties': get_prop(app_env_data, 'sparkProperties'),
            'spark_command': get_system_property(app_env_data, app['id'], 'sun.java.command'),
            'mode': app['mode']
        }

    @orm.reconstructor
    def compute_all_metrics(self):

        # TODO check if this was calculated before --> does not work
        # TODO plus calculate all when launching the server?? --> will not work either
        # TODO estimate how much memory will be needed
        # TODO last access attribute, set some LRU algorithm for releasing the memory if it's filled
        # TODO maybe Dogpile Cache can help

        if not hasattr(self, "is_processed") or not self.is_processed:
            self.metrics_overview = {}

            self.compute_stage_metrics()
            self.compute_job_metrics()
            self.compute_executor_metrics()
            self.compute_app_config_metrics()

            self.overall_severity = max(self.metrics_overview.values()) \
                if len(self.metrics_overview) > 0 else Severity.NONE

            self.is_processed = True

            self.duration_formatted = fmt_time(self.duration/1000)

    def compute_stage_metrics(self):
        stage_analyzer = StageAnalyzer(self)

        self.stage_failure_metric = stage_analyzer.analyze_failed_stages()
        self.stage_skew_metric = stage_analyzer.analyze_stage_skews()
        self.stage_disk_spill_metric = stage_analyzer.analyze_disk_spills()

        for metric in [self.stage_failure_metric,
                       self.stage_skew_metric,
                       self.stage_disk_spill_metric
                       ]:
            if metric.severity > Severity.NONE:
                self.metrics_overview[metric] = metric.severity

    def compute_job_metrics(self):
        job_analyzer = JobAnalyzer(self)

        self.job_metrics = job_analyzer.analyze_failed_jobs()

        for metric in [self.job_metrics]:
            if metric.severity > Severity.NONE:
                self.metrics_overview[metric] = metric.severity

    def compute_executor_metrics(self):
        executor_analyzer = ExecutorAnalyzer(self)

        self.driver_gc_time_metric = executor_analyzer.analyze_driver_gc_time()
        self.executor_gc_time_metric = executor_analyzer.analyze_executors_gc_time()

        for metric in [self.driver_gc_time_metric,
                       self.executor_gc_time_metric]:
            if metric.severity > Severity.NONE:
                self.metrics_overview[metric] = metric.severity

    def compute_app_config_metrics(self):
        app_config_analyzer = AppConfigAnalyzer(self)

        self.serializer_metric = app_config_analyzer.analyze_serializer_config()
        self.dynamic_allocation_metric = app_config_analyzer.analyze_dynamic_allocation()

        for metric in [self.serializer_metric,
                       self.dynamic_allocation_metric
                       ]:
            if metric.severity > Severity.NONE:
                self.metrics_overview[metric] = metric.severity

    def get_basic_metrics(self):
        db = Session()
        basic_metrics = {}
        runtime = self.duration/1000.0  # seconds
        cpu_time = cast_or_none(db.query(func.sum(StageEntity.executor_cpu_time) / 1.0e9)
                                .filter(StageEntity.app_id == self.app_id).scalar(), float)  # seconds

        total_gc_time = cast_or_none(db.query(func.sum(ExecutorEntity.total_gc_time) / 1.0e3)
                                     .filter(ExecutorEntity.app_id == self.app_id).scalar(), float)  # seconds

        basic_metrics["Runtime"] = fmt_time(runtime)  # seconds
        basic_metrics["CPU Time"] = fmt_time(cpu_time)  # seconds
        basic_metrics["Total Tasks Time"] = "dummy"
        basic_metrics["Executor Peak Memory"] = "dummy"
        basic_metrics["Jobs Number"] = db.query(func.count(JobEntity.job_id)).filter(JobEntity.app_id == self.app_id).scalar()
        basic_metrics["Stages Number"] = db.query(func.count(StageEntity.stage_id)).filter(StageEntity.app_id == self.app_id).scalar()
        basic_metrics["Tasks Number"] = cast_or_none(db.query(func.sum(StageEntity.num_tasks)).filter(StageEntity.app_id == self.app_id).scalar(), int)
        basic_metrics["Total GC Time"] = fmt_time(total_gc_time)  # seconds

        return basic_metrics

    def get_basic_configs(self):
        mem_props = [DRIVER_MEMORY_KEY, DRIVER_MEMORY_OVERHEAD_KEY, EXECUTOR_MEMORY_KEY, EXECUTOR_MEMORY_OVERHEAD_KEY]
        other_props = [DRIVER_MAX_RESULT_SIZE_KEY, EXECUTOR_INSTANCES_KEY, EXECUTOR_CORES_KEY, DYNAMIC_ALLOCATION_KEY]
        return {"memory": {prop: self.get_spark_property(prop) for prop in mem_props},
                "other": {prop: self.get_spark_property(prop) for prop in other_props}}

    def get_spark_property(self, property_name):
        """
        Get value of a specific Spark configuration property
        :param property_name: name of the property
        :return: value of the property, or None if it's not found
        """
        for prop in self.spark_properties:
            if property_name == prop[0]:
                return prop[1]
        return None
