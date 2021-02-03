import configparser
import os

from db.base import Session
import sparkscope_web.metrics.thresholds as thresholds
# from db.entities.application import Application
# from db.entities.executor import Executor
# from db.entities.job import Job
# from db.entities.stage import Stage
# from db.entities.stage_statistics import StageStatistics
# from db.entities.task import Task
from db.entities.stage import Stage


class Analyzer:
    """
    Generic Analyzer Class
    """
    def __init__(self):
        self.db = Session()

        # TODO there must be a better way of reading the config file path!
        analyzers_dir = os.path.dirname(__file__)
        self.config = configparser.ConfigParser()
        self.config.read(os.path.abspath(os.path.join(analyzers_dir, '..', 'metrics', 'thresholds.conf')))
