import ssl
import json
import re
from concurrent.futures import wait, as_completed

import datetime
import requests
import concurrent.futures
import threading
import configparser
import logging
import os
from sqlalchemy import func

from db.entities.application import Application

from db.entities.executor import Executor
from db.entities.job import Job
from db.entities.stage import Stage
from db.entities.stage_statistics import StageStatistics
from history_fetcher.utils import Utils

# TODO suppress InsecureRequestWarning while not verifying the certificates
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

"""
Create a thread specific object
"""
thread_local = threading.local()

"""
Set up logger
"""
logger = logging.getLogger(__name__)


class DataFetcher:
    def __init__(self, db_session):
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))

        self.base_url = self.config['history_fetcher']['base_url']

        # FIXME currently, the certificate verification is disabled!!!
        self.verify_certificates = self.config.getboolean('history_fetcher', 'verify_certificates')

        self.db_session = db_session

        self.utils = Utils()

        self.stage_job_mapping = {}

    def fetch_all_data(self):
        app_ids = self.fetch_applications()
        self.fetch_executors(app_ids)
        self.fetch_jobs(app_ids)
        app_stage_mapping = self.fetch_stages(app_ids)
        self.fetch_stage_statistics(app_stage_mapping)


    def fetch_applications(self):
        """

        :return: list of application id's
        """
        logger.info("fetching application data")

        time_filter = self.get_time_filter()
        # app_data = self.get_json(f"{self.base_url}?status=completed&minEndDate={time_filter}")
        app_data = self.get_json(f"{self.base_url}?status=completed&limit=3")
        logger.info(f"Time filter: >= {time_filter}. {len(app_data)} new application records found.")

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
        for i in as_completed(env_data.values()):
            pass

        app_ids = []

        for app in app_data:
            app_ids.append(app['id'])
            app_env_data = env_data[app['id']].result()
            app_attributes = {
                'app_id': app['id'],
                'name': app['name'],
                'start_time': app['attempts'][0]['startTime'],
                'end_time': app['attempts'][0]['endTime'],
                'duration': app['attempts'][0]['duration'],
                'spark_user': app['attempts'][0]['sparkUser'],
                'completed': app['attempts'][0]['completed'],
                'runtime': self.utils.get_prop(app_env_data, 'runtime'),
                'spark_properties': self.utils.get_prop(app_env_data, 'sparkProperties'),
                'spark_command': self.utils.get_sun_java_command(app_env_data, app['id']),
                'mode': app['mode']
            }
            self.db_session.add(Application(app_attributes))

        return app_ids

    def fetch_executors(self, app_ids):
        logger.info(f"Fetching executors data")

        urls = [f"{self.base_url}/{app_id}/executors" for app_id in app_ids]

        executors_data = self.get_jsons_parallel(urls)

        # wait until all the data are received
        for i in as_completed(executors_data.values()):
            pass

        for app_id, executors_per_app in executors_data.items():
            if not executors_per_app.result():
                continue

            for executor in executors_per_app.result():
                executor_attributes = {
                    'executor_key': f"{app_id}_{executor['id']}",
                    'app_id': app_id,
                    'id': executor['id'],
                    'host_port': executor['hostPort'],
                    'is_active': executor['isActive'],
                    'rdd_blocks': executor['rddBlocks'],
                    'memory_used': executor['memoryUsed'],
                    'disk_used': executor['diskUsed'],
                    'total_cores': executor['totalCores'],
                    'max_tasks': executor['maxTasks'],
                    'active_tasks': executor['activeTasks'],
                    'failed_tasks': executor['failedTasks'],
                    'total_duration': executor['totalDuration'],
                    'total_gc_time': executor['totalGCTime'],
                    'total_input_bytes': executor['totalInputBytes'],
                    'total_shuffle_read': executor['totalShuffleRead'],
                    'total_shuffle_write': executor['totalShuffleWrite'],
                    'is_blacklisted': executor['isBlacklisted'],
                    'max_memory': executor['maxMemory'],
                    'add_time': executor['addTime'],
                    'executor_stdout_log': self.utils.get_prop(executor['executorLogs'], 'stdout'),
                    'executor_stderr_log': self.utils.get_prop(executor['executorLogs'], 'stderr'),
                    'used_on_heap_storage_memory': self.utils.get_prop(executor['memoryMetrics'], 'usedOnHeapStorageMemory'),
                    'used_off_heap_storage_memory': self.utils.get_prop(executor['memoryMetrics'], 'usedOffHeapStorageMemory'),
                    'total_on_heap_storage_memory': self.utils.get_prop(executor['memoryMetrics'], 'totalOnHeapStorageMemory'),
                    'total_off_heap_storage_memory': self.utils.get_prop(executor['memoryMetrics'], 'totalOffHeapStorageMemory'),
                    'blacklisted_in_stages': executor['blacklistedInStages']
                }
                self.db_session.add(Executor(executor_attributes))

    def fetch_jobs(self, app_ids):
        logger.info(f"Fetching jobs data")

        urls = [f"{self.base_url}/{app_id}/jobs" for app_id in app_ids]

        jobs_data = self.get_jsons_parallel(urls)

        # wait until all the data are received
        for i in as_completed(jobs_data.values()):
            pass

        for app_id, jobs_per_app in jobs_data.items():
            if not jobs_per_app.result():  # job list might be empty
                continue

            for job in jobs_per_app.result():
                job_attributes = {
                    'job_key': f"{app_id}_{job['jobId']}",
                    'app_id': app_id,
                    'job_id': job['jobId'],
                    'submission_time': job['submissionTime'],
                    'completion_time': job['completionTime'],
                    'status': job['status'],
                    'num_tasks': job['numTasks'],
                    'num_active_tasks': job['numActiveTasks'],
                    'num_completed_tasks': job['numCompletedTasks'],
                    'num_skipped_tasks': job['numSkippedTasks'],
                    'num_failed_tasks': job['numFailedTasks'],
                    'num_killed_tasks': job['numKilledTasks'],
                    'num_completed_indices': job['numCompletedIndices'],
                    'num_active_stages': job['numActiveStages'],
                    'num_completed_stages': job['numCompletedStages'],
                    'num_skipped_stages': job['numSkippedStages'],
                    'num_failed_stages': job['numFailedStages'],
                    'killed_tasks_summary': job['killedTasksSummary']
                }
                self.map_jobs_to_stages(job['stageIds'], job_attributes['job_key'], app_id)

                self.db_session.add(Job(job_attributes))

    def fetch_stages(self, app_ids):
        logger.info(f"Fetching stages data")

        urls = [f"{self.base_url}/{app_id}/stages" for app_id in app_ids]

        stages_data = self.get_jsons_parallel(urls)

        # wait until all the data are received
        for i in as_completed(stages_data.values()):
            pass

        app_stage_mapping = {}

        for app_id, stages_per_app in stages_data.items():
            if not stages_per_app.result():
                continue

            app_stage_mapping[app_id] = []

            for stage in stages_per_app.result():
                stage_attributes = {
                    'stage_key': f"{app_id}_{stage['stageId']}",
                    'app_id': app_id,
                    'status': stage['status'],
                    'stage_id': stage['stageId'],
                    'attempt_id': stage['attemptId'],
                    'job_key': self.stage_job_mapping[f"{app_id}_{stage['stageId']}"],
                    'num_tasks': stage['numTasks'],
                    'num_active_tasks': stage['numActiveTasks'],
                    'num_complete_tasks': stage['numCompleteTasks'],
                    'num_failed_tasks': stage['numFailedTasks'],
                    'num_killed_tasks': stage['numKilledTasks'],
                    'num_completed_indices': stage['numCompletedIndices'],
                    'executor_run_time': stage['executorRunTime'],
                    'executor_cpu_time': stage['executorCpuTime'],
                    'submission_time': self.utils.get_prop(stage, 'submissionTime'),
                    'first_task_launched_time': self.utils.get_prop(stage, 'firstTaskLaunchedTime'),
                    'completion_time': self.utils.get_prop(stage, 'completionTime'),
                    'input_bytes': stage['inputBytes'],
                    'input_records': stage['inputRecords'],
                    'output_bytes': stage['outputBytes'],
                    'output_records': stage['outputRecords'],
                    'shuffle_read_bytes': stage['shuffleReadBytes'],
                    'shuffle_read_records': stage['shuffleReadRecords'],
                    'shuffle_write_bytes': stage['shuffleWriteBytes'],
                    'shuffle_write_records': stage['shuffleWriteRecords'],
                    'memory_bytes_spilled': stage['memoryBytesSpilled'],
                    'disk_bytes_spilled': stage['diskBytesSpilled'],
                    'name': stage['name'],
                    'details': stage['details'],
                    'scheduling_pool': stage['schedulingPool'],
                    'rdd_ids': stage['rddIds'],
                    'accumulator_updates': stage['accumulatorUpdates'],
                    'killed_tasks_summary': stage['killedTasksSummary']
                }
                app_stage_mapping[app_id].append(stage_attributes['stage_id'])
                self.db_session.add(Stage(stage_attributes))

        return app_stage_mapping

    def fetch_stage_statistics(self, app_stage_mapping):
        logger.info(f"Fetching stage statistics data")

        urls = [f"{self.base_url}/{app_id}/stages/{stage_id}/0/taskSummary"
                for app_id, stage_list in app_stage_mapping.items() for stage_id in stage_list]

        stage_statistics_data = self.get_jsons_parallel(urls, key="stage_key")

        # wait until all the data are received
        for i in as_completed(stage_statistics_data.values()):
            pass

        for stage_key, stage_statistics_future in stage_statistics_data.items():
            stage_statistics = stage_statistics_future.result()
            if not stage_statistics:
                continue

            stage_statistics_attributes = {
                'stage_key': stage_key,
                'quantiles': stage_statistics["quantiles"],
                'executor_deserialize_time': stage_statistics["executorDeserializeTime"],
                'executor_deserialize_cpu_time': stage_statistics["executorDeserializeCpuTime"],
                'executor_run_time': stage_statistics["executorRunTime"],
                'executor_cpu_time': stage_statistics["executorCpuTime"],
                'result_size': stage_statistics["resultSize"],
                'jvm_gc_time': stage_statistics["jvmGcTime"],
                'result_serialization_time': stage_statistics["resultSerializationTime"],
                'getting_result_time': stage_statistics["gettingResultTime"],
                'scheduler_delay': stage_statistics["schedulerDelay"],
                'peak_execution_memory': stage_statistics["peakExecutionMemory"],
                'memory_bytes_spilled': stage_statistics["memoryBytesSpilled"],
                'disk_bytes_spilled': stage_statistics["diskBytesSpilled"],
                'bytes_read': stage_statistics["inputMetrics"]["bytesRead"],
                'records_read': stage_statistics["inputMetrics"]["recordsRead"],
                'bytes_written': stage_statistics["outputMetrics"]["bytesWritten"],
                'records_written': stage_statistics["outputMetrics"]["recordsWritten"],
                'shuffle_read_bytes': stage_statistics["shuffleReadMetrics"]["readBytes"],
                'shuffle_read_records': stage_statistics["shuffleReadMetrics"]["readRecords"],
                'shuffle_remote_blocks_fetched': stage_statistics["shuffleReadMetrics"]["remoteBlocksFetched"],
                'shuffle_local_blocks_fetched': stage_statistics["shuffleReadMetrics"]["localBlocksFetched"],
                'shuffle_fetch_wait_time': stage_statistics["shuffleReadMetrics"]["fetchWaitTime"],
                'shuffle_remote_bytes_read': stage_statistics["shuffleReadMetrics"]["remoteBytesRead"],
                'shuffle_remote_bytes_read_to_disk': stage_statistics["shuffleReadMetrics"]["remoteBytesReadToDisk"],
                'shuffle_total_blocks_fetched': stage_statistics["shuffleReadMetrics"]["totalBlocksFetched"],
                'shuffle_write_bytes': stage_statistics["shuffleWriteMetrics"]["writeBytes"],
                'shuffle_write_records': stage_statistics["shuffleWriteMetrics"]["writeRecords"],
                'shuffle_write_time': stage_statistics["shuffleWriteMetrics"]["writeTime"],
            }
            self.db_session.add(StageStatistics(stage_statistics_attributes))

    def fetch_tasks(self, app_stage_id):
        pass

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
        Gets the json payload from the specified URL.
        :param url: URL
        :return: json payload or None if not able to get the json
        """
        http_session = self.get_http_session()
        try:
            logger.debug(f">>> REQ: {self.utils.get_app_id_from_url(url)}")
            response = http_session.get(url, verify=self.verify_certificates)
            logger.debug(f"<<< RSP: {self.utils.get_app_id_from_url(url)}")
            return response.json()
        except Exception as e:
            logger.warning(f"Could not open {url}: {e}")
            return None

    def get_jsons_parallel(self, urls, key="app_id"):
        """
        Launches ThreadPoolExecutor, opens multiple http connections to History Server and gets the payload for each url
        :param urls: list of urls to access
        :return: dictionary {id: payload} TODO
        """
        if key not in ["app_id", "stage_key"]:
            raise ValueError(f"Unsupported key: {key}")

        threadpool_size = self.config.getint('history_fetcher', 'threadpool_size')
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=threadpool_size)

        if key == "stage_key":
            result = {self.utils.get_stage_key_from_url(url): executor.submit(self.get_json, url) for url in urls}
        else:
            result = {self.utils.get_app_id_from_url(url): executor.submit(self.get_json, url) for url in urls}

        return result

    def get_time_filter(self):
        """
        Gets timestamp, one millisecond newer than the endTime of the newest application found on Spark History Server.
        :return: timestamp in format 2020-01-01T01:01:01.123GMT
        """
        max_date = self.db_session.query(func.max(Application.end_time))[0][0]
        if max_date is None:
            max_date = datetime.datetime(1970, 1, 1, 0, 0, 0, 0)
        fmt = "%Y-%m-%dT%H:%M:%S.%f" # example: 2020-10-23T12:34:56.012345
        time_filter_preformatted = (max_date + datetime.timedelta(milliseconds=1)).strftime(fmt)
        return f"{time_filter_preformatted[:-3]}GMT"

    def map_jobs_to_stages(self, stage_ids, job_key, app_id):
        for stage_id in stage_ids:
            self.stage_job_mapping[f"{app_id}_{stage_id}"] = job_key

