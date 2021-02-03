# coding=utf-8

from sqlalchemy import Column, String, DateTime, BigInteger, Boolean, JSON, orm
from db.base import Base
from history_fetcher.utils import get_prop, get_system_property
from sparkscope_web.analyzers.job_analyzer import JobAnalyzer
from sparkscope_web.analyzers.stage_analyzer import StageAnalyzer
from sparkscope_web.metrics.severity import Severity


class Application(Base):
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

        self.stage_failure_metric = {}
        self.stage_skew_metric = {}
        self.stage_disk_spill_metric = {}
        self.job_metrics = {}

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
        self.metrics_overview = {}

        self.compute_stage_metrics()
        self.compute_job_metrics()

        self.overall_severity = max(self.metrics_overview.values()) \
            if len(self.metrics_overview) > 0 else Severity.NONE

    def compute_stage_metrics(self):
        stage_analyzer = StageAnalyzer(self)

        self.stage_failure_metric = stage_analyzer.analyze_failed_stages()
        self.stage_skew_metric = stage_analyzer.analyze_stage_skews()
        self.stage_disk_spill_metric = stage_analyzer.analyze_disk_spills()

        for metric in [self.stage_failure_metric,
                       self.stage_skew_metric,
                       self.stage_disk_spill_metric]:
            if metric.severity > Severity.NONE:
                self.metrics_overview[metric] = metric.severity

    def compute_job_metrics(self):
        job_analyzer = JobAnalyzer()

        self.job_metrics = job_analyzer.analyze(self)

        for metric in [self.job_metrics]:
            if metric.severity > Severity.NONE:
                self.metrics_overview[metric] = metric.severity
