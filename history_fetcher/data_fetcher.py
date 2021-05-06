from concurrent.futures import as_completed

import datetime
import requests
import concurrent.futures
import threading
import configparser
import logging
import os
from sqlalchemy import func

from db.entities.application import ApplicationEntity

from db.entities.executor import ExecutorEntity
from db.entities.job import JobEntity
from db.entities.stage import StageEntity
from db.entities.stage_statistics import StageStatisticsEntity
from db.entities.stage_executor import StageExecutorEntity
from db.entities.task import TaskEntity
from history_fetcher.utils import Utils

# suppress InsecureRequestWarning while not verifying the certificates
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Create a thread specific object
thread_local = threading.local()

# Set up logger
logger = logging.getLogger(__name__)


class DataFetcher:
    """
    A class responsible for fetching data from Spark History Server (SHS) to a database
    """
    def __init__(self, db_session, test_mode=False):
        """
        Create DataFetcher object
        :param db_session: database session
        :param test_mode: True for fetching a constant number of applications metadata from SHS. If False, all the
        non-processed applications will be fetched
        """
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
        self.base_url = self.config['history_fetcher']['base_url']
        self.verify_certificates = self.config.getboolean('security', 'verify_certificates')

        self.db_session = db_session
        self.test_mode = test_mode

        self.utils = Utils()
        self.stage_job_mapping = {}

    def fetch_all_data(self):
        """
        Fetch all the levels of data (Applications, Executors, Jobs, Stages, Stage Statistics, Tasks) from the SHS and
        store them in the database
        :return: list of the fetched application_id's
        """
        app_ids = self.fetch_applications()
        self.fetch_executors(app_ids)
        self.fetch_jobs(app_ids)
        app_stage_mapping = self.fetch_stages(app_ids)
        self.fetch_stage_executors(app_stage_mapping)
        self.fetch_stage_statistics(app_stage_mapping)
        self.fetch_tasks(app_stage_mapping)

        return app_ids

    def fetch_applications(self):
        """
        Fetch the applications data from SHS and store them in the database
        :return: list of application id's
        """
        if self.test_mode:
            limit = self.config.getint('testing', 'apps_number')
            logger.info(f"Test mode active. Fetching {limit} applications.")
            app_data = self.get_json(f"{self.base_url}?status=completed&limit={limit}")
        else:
            time_filter = self.get_time_filter()
            app_data = self.get_json(f"{self.base_url}?status=completed&minEndDate={time_filter}")
            logger.info(f"Time filter: >= {time_filter}. {len(app_data)} new application records found.")
        logger.debug("Fetching application data...")

        # apps in cluster mode contain attemptId, client mode applications don't. The environment URLs then differ.
        env_urls = []
        for app in app_data:
            if 'attemptId' in app['attempts'][0]:
                env_urls.append(f"{self.base_url}/{app['id']}/{app['attempts'][0]['attemptId']}/environment")
                app['mode'] = "cluster"
            else:
                env_urls.append(f"{self.base_url}/{app['id']}/environment")
                app['mode'] = "client"

        env_data = self.get_jsons_parallel(env_urls)
        app_ids = []

        for app in app_data:
            app_ids.append(app['id'])
            app_env_data = env_data[app['id']].result()
            app_attributes = ApplicationEntity.get_fetch_dict(app, app_env_data)
            self.db_session.add(ApplicationEntity(app_attributes))
            self.db_session.flush()
        logger.info(f"Fetched {len(app_data)} applications.")
        return app_ids

    def fetch_executors(self, app_ids):
        """
        For each application being fetched, fetch data about all the executors.
        :param app_ids: list of application_id's to process
        """
        logger.debug(f"Fetching executors data...")
        urls = [f"{self.base_url}/{app_id}/allexecutors" for app_id in app_ids]
        executors_data = self.get_jsons_parallel(urls)
        executor_count = 0

        for app_id, executors_per_app in executors_data.items():
            if not executors_per_app.result():
                continue

            for executor in executors_per_app.result():
                executor_attributes = ExecutorEntity.get_fetch_dict(app_id, executor)
                self.db_session.add(ExecutorEntity(executor_attributes))
                self.db_session.flush()

            executor_count += len(executors_per_app.result())
        logger.info(f"Fetched {executor_count} executors.")

    def fetch_jobs(self, app_ids):
        """
        For each application being fetched, fetch data about all the jobs.
        :param app_ids: list of application_id's to process
        """
        logger.debug(f"Fetching jobs data...")
        urls = [f"{self.base_url}/{app_id}/jobs" for app_id in app_ids]
        jobs_data = self.get_jsons_parallel(urls)
        job_count = 0

        for app_id, jobs_per_app in jobs_data.items():
            if not jobs_per_app.result():  # job list might be empty
                continue

            for job in jobs_per_app.result():
                job_attributes = JobEntity.get_fetch_dict(app_id, job)
                self.map_jobs_to_stages(job['stageIds'], job_attributes['job_key'], app_id)
                self.db_session.add(JobEntity(job_attributes))
                self.db_session.flush()

            job_count += len(jobs_per_app.result())
        logger.info(f"Fetched {job_count} jobs.")

    def fetch_stages(self, app_ids):
        """
        For each application being fetched, fetch data about all the stages
        :param app_ids: list of application_id's to process
        :return: dictionary {application_id: List[stage_id]} mapping stages to the corresponding applications
        """
        logger.debug(f"Fetching stages data...")
        urls = [f"{self.base_url}/{app_id}/stages" for app_id in app_ids]
        stages_data = self.get_jsons_parallel(urls)
        app_stage_mapping = {}  # dict[application_id, List[stage_id]]
        stage_count = 0

        for app_id, stages_per_app in stages_data.items():
            if not stages_per_app.result():  # the stage list might be empty
                continue

            app_stage_mapping[app_id] = []

            for stage in stages_per_app.result():
                stage_attributes = StageEntity.get_fetch_dict(app_id, stage, self.stage_job_mapping)
                app_stage_mapping[app_id].append(stage_attributes['stage_id'])
                if stage_attributes['attempt_id'] == 0:
                    self.db_session.add(StageEntity(stage_attributes))
                    self.db_session.flush()

            stage_count += len(stages_per_app.result())
        logger.info(f"Fetched {stage_count} stages.")

        return app_stage_mapping

    def fetch_stage_executors(self, app_stage_mapping):
        """
        For each application and each stage being fetched, fetch data about usage of Executors within each Stage.
        :param app_stage_mapping: dictionary {application_id: List[stage_id]} mapping the stages to the corresponding
        applications
        :param app_stage_mapping:
        """
        logger.info(f"Fetching stage_executor data...")
        urls = [f"{self.base_url}/{app_id}/stages/{stage_id}/0"
                for app_id, stage_list in app_stage_mapping.items() for stage_id in stage_list]

        stage_data = self.get_jsons_parallel(urls, key="stage_key")
        stage_executor_count = 0

        for stage_key, stage_future in stage_data.items():
            stage_json = stage_future.result()
            if not stage_json:
                continue

            app_id = self.utils.get_app_id_from_stage_key(stage_key)
            executors_per_app = self.get_executors_per_app(app_id)
            executor_summary = stage_json['executorSummary']
            for executor_id, stage_executor_dict in executor_summary.items():

                stage_executor_attributes = StageExecutorEntity.get_fetch_dict(stage_key, executor_id, app_id, stage_executor_dict)

                # in some rare cases, in History Server, a Stage might contain an Executor which is not in the executors
                # endpoint. If so, add the key to the Executor table to skip it to avoid DB Integrity Violation
                executor_key = f"{app_id}_{executor_id}"
                if executor_key not in executors_per_app:
                    self.db_session.add(ExecutorEntity({"executor_key": executor_key, "app_id": app_id}))
                    self.db_session.flush()
                    executors_per_app.append(executor_key)

                self.db_session.add(StageExecutorEntity(stage_executor_attributes))
                self.db_session.flush()
                stage_executor_count += 1
        logger.info(f"Fetched {stage_executor_count} stage_executors.")

    def fetch_stage_statistics(self, app_stage_mapping):
        """
        For each application and each stage being fetched, fetch data about Task Summary / Stage Statistics.
        :param app_stage_mapping: dictionary {application_id: List[stage_id]} mapping the stages to the corresponding
        applications
        """
        logger.info(f"Fetching stage statistics data...")

        # the quantiles are intentionally hardcoded to avoid unexpected issues after modifying them
        urls = [f"{self.base_url}/{app_id}/stages/{stage_id}/0/taskSummary?quantiles=0.001,0.25,0.5,0.75,0.999"
                for app_id, stage_list in app_stage_mapping.items() for stage_id in stage_list]

        stage_statistics_data = self.get_jsons_parallel(urls, key="stage_key")
        stage_stat_count = 0

        for stage_key, stage_statistics_future in stage_statistics_data.items():
            stage_statistics = stage_statistics_future.result()
            if not stage_statistics:  # the endpoint might be empty
                continue

            stage_stat_count += 1
            stage_statistics_attributes = StageStatisticsEntity.get_fetch_dict(stage_key, stage_statistics)
            self.db_session.add(StageStatisticsEntity(stage_statistics_attributes))
            self.db_session.flush()
        logger.info(f"Fetched {stage_stat_count} stage statistics records.")

    def fetch_tasks(self, app_stage_mapping):
        """
        For each application and each stage being fetched, fetch detailed data about tasks. The number of tasks being
        fetched is read from the config file.
        :param app_stage_mapping: dictionary {application_id: List[stage_id]} mapping the stages to the corresponding
        applications
        """
        logger.debug(f"Fetching tasks data...")
        task_limit = self.config.getint('history_fetcher', 'task_limit', fallback=2147483647)
        urls = [f"{self.base_url}/{app_id}/stages/{stage_id}/0/taskList?length={task_limit}&sortBy=-runtime"
                for app_id, stage_list in app_stage_mapping.items() for stage_id in stage_list]
        tasks_data = self.get_jsons_parallel(urls, key="stage_key")
        task_count = 0

        for stage_key, tasks_future in tasks_data.items():
            tasks = tasks_future.result()
            if not tasks:
                continue

            app_id = self.utils.get_app_id_from_stage_key(stage_key)

            for task in tasks:
                tasks_attributes = TaskEntity.get_fetch_dict(stage_key, task, app_id)
                self.db_session.add(TaskEntity(tasks_attributes))
                self.db_session.flush()

            task_count += len(tasks)
        logger.info(f"Fetched {task_count} tasks.")

    def get_http_session(self):
        """
        Get a separate http session for each thread
        :return: http session
        """
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session()
        return thread_local.session

    def get_json(self, url):
        """
        Get the json payload from the specified URL.
        :param url: URL
        :return: json payload or None if not able to get the json
        """
        http_session = self.get_http_session()
        try:
            logger.trace(f">>> REQ: {url}")
            response = http_session.get(url, verify=self.verify_certificates)
            logger.trace(f"<<< RSP: {url}")
            return response.json()
        except Exception as e:
            logger.warning(f"Could not open {url}: {e}")
            return None

    def get_jsons_parallel(self, urls, key="app_id"):
        """
        Open multiple HTTP connections and collect the responses in parallel
        :param urls: a list of endpoints that should be processed
        :param key: "app_id" or "stage_key"
        :raises ValueError: if key is not in ["app_id", "stage_key"]
        :return: dictionary {application_id: payload} for key == "app_id" or dictionary {stage_key: payload} for
        key == "stage_key"
        """
        if key not in ["app_id", "stage_key"]:
            logger.error(f"Unsupported key: {key}")
            raise ValueError(f"Unsupported key: {key}")

        threadpool_size = self.config.getint('history_fetcher', 'threadpool_size')
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=threadpool_size)

        if key == "stage_key":
            result = {self.utils.get_stage_key_from_url(url): executor.submit(self.get_json, url) for url in urls}
        else:
            result = {self.utils.get_app_id_from_url(url): executor.submit(self.get_json, url) for url in urls}

        # wait until all the data are received
        for i in as_completed(result.values()):
            pass

        return result

    def get_time_filter(self):
        """
        Get timestamp, one millisecond newer than the endTime of the newest application found in the database.
        If no applications are found in the database (the initial load), 1970-01-01T00:00:00.001GMT should be returned.
        Used as a delta-logic criterion.
        :return: timestamp in format 2020-01-01T01:01:01.123GMT
        """
        max_date = self.db_session.query(func.max(ApplicationEntity.end_time))[0][0]
        if max_date is None:
            max_date = datetime.datetime(1970, 1, 1, 0, 0, 0, 0)
        fmt = "%Y-%m-%dT%H:%M:%S.%f"  # example: 2020-10-23T12:34:56.012345
        time_filter_preformatted = (max_date + datetime.timedelta(milliseconds=1)).strftime(fmt)
        return f"{time_filter_preformatted[:-3]}GMT"

    def map_jobs_to_stages(self, stage_ids, job_key, app_id):
        """
        Map the job_key to the corresponding job_key. Add new data to the dictionary {stage_key: job_key}.
        Note: stage_key is formed as {application_id}_{stage_id}
        :param stage_ids: list of stage_id's
        :param job_key: job_key
        :param app_id: application_id
        :return:
        """
        for stage_id in stage_ids:
            self.stage_job_mapping[f"{app_id}_{stage_id}"] = job_key

    def get_executors_per_app(self, app_id):
        executors = self.db_session.query(ExecutorEntity).filter_by(app_id=app_id).all()
        return [executor.executor_key for executor in executors]

