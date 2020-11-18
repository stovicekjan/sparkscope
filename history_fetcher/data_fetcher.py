from concurrent.futures import as_completed

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
from db.entities.task import Task
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
                'spark_command': self.utils.get_system_property(app_env_data, app['id'], 'sun.java.command'),
                'mode': app['mode']
            }
            self.db_session.add(Application(app_attributes))
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
                    'remove_time': self.utils.get_prop(executor, 'removeTime'),
                    'remove_reason': self.utils.get_prop(executor, 'removeReason'),
                    'executor_stdout_log': self.utils.get_prop(executor, 'executorLogs', 'stdout'),
                    'executor_stderr_log': self.utils.get_prop(executor, 'executorLogs', 'stderr'),
                    'used_on_heap_storage_memory': self.utils.get_prop(executor, 'memoryMetrics', 'usedOnHeapStorageMemory'),
                    'used_off_heap_storage_memory': self.utils.get_prop(executor, 'memoryMetrics', 'usedOffHeapStorageMemory'),
                    'total_on_heap_storage_memory': self.utils.get_prop(executor, 'memoryMetrics', 'totalOnHeapStorageMemory'),
                    'total_off_heap_storage_memory': self.utils.get_prop(executor, 'memoryMetrics', 'totalOffHeapStorageMemory'),
                    'blacklisted_in_stages': executor['blacklistedInStages']
                }
                self.db_session.add(Executor(executor_attributes))

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

            job_count += len(jobs_per_app.result())

        logger.info(f"Fetched {job_count} jobs.")

    def fetch_stages(self, app_ids):
        """
        For each application being fetched, fetch data about all the jobs.
        :param app_ids: list of application_id's to process
        :return: dictionary {application_id: List[stage_id]} mapping stages to the corresponding applications
        """
        logger.debug(f"Fetching stages data...")

        urls = [f"{self.base_url}/{app_id}/stages" for app_id in app_ids]

        stages_data = self.get_jsons_parallel(urls)

        app_stage_mapping = {}  # dict[application_id, List[stage_id]]

        stage_count = 0

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

            stage_count += len(stages_per_app.result())

        logger.info(f"Fetched {stage_count} stages.")

        return app_stage_mapping

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
            if not stage_statistics:
                continue

            stage_stat_count += 1

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
                tasks_attributes = {
                    'task_key': f"{stage_key}_{task['taskId']}",
                    'stage_key': stage_key,
                    'task_id': task['taskId'],
                    'index': task['index'],
                    'attempt': task['attempt'],
                    'launch_time': task['launchTime'],
                    'duration': task['duration'],
                    'executor_key': f"{app_id}_{task['executorId']}",
                    'host': task['host'],
                    'status': task['status'],
                    'task_locality': task['taskLocality'],
                    'speculative': task['speculative'],
                    'accumulator_updates': task['accumulatorUpdates'],
                    'executor_deserialize_time': task['taskMetrics']['executorDeserializeTime'],
                    'executor_deserialize_cpu_time': task['taskMetrics']['executorDeserializeCpuTime'],
                    'executor_run_time': task['taskMetrics']['executorRunTime'],
                    'executor_cpu_time': task['taskMetrics']['executorCpuTime'],
                    'result_size': task['taskMetrics']['resultSize'],
                    'jvm_gc_time': task['taskMetrics']['jvmGcTime'],
                    'result_serialization_time': task['taskMetrics']['resultSerializationTime'],
                    'memory_bytes_spilled': task['taskMetrics']['memoryBytesSpilled'],
                    'disk_bytes_spilled': task['taskMetrics']['diskBytesSpilled'],
                    'peak_execution_memory': task['taskMetrics']['peakExecutionMemory'],
                    'bytes_read': task['taskMetrics']['inputMetrics']['bytesRead'],
                    'records_read': task['taskMetrics']['inputMetrics']['recordsRead'],
                    'bytes_written': task['taskMetrics']['outputMetrics']['bytesWritten'],
                    'records_written': task['taskMetrics']['outputMetrics']['recordsWritten'],
                    'shuffle_remote_blocks_fetched': task['taskMetrics']['shuffleReadMetrics']['remoteBlocksFetched'],
                    'shuffle_local_blocks_fetched': task['taskMetrics']['shuffleReadMetrics']['localBlocksFetched'],
                    'shuffle_fetch_wait_time': task['taskMetrics']['shuffleReadMetrics']['fetchWaitTime'],
                    'shuffle_remote_bytes_read': task['taskMetrics']['shuffleReadMetrics']['remoteBytesRead'],
                    'shuffle_remote_bytes_read_to_disk': task['taskMetrics']['shuffleReadMetrics']['remoteBytesReadToDisk'],
                    'shuffle_local_bytes_read': task['taskMetrics']['shuffleReadMetrics']['localBytesRead'],
                    'shuffle_records_read': task['taskMetrics']['shuffleReadMetrics']['recordsRead'],
                    'shuffle_bytes_written': task['taskMetrics']['shuffleWriteMetrics']['bytesWritten'],
                    'shuffle_write_time': task['taskMetrics']['shuffleWriteMetrics']['writeTime'],
                    'shuffle_records_written': task['taskMetrics']['shuffleWriteMetrics']['recordsWritten']
                }
                self.db_session.add(Task(tasks_attributes))

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
        max_date = self.db_session.query(func.max(Application.end_time))[0][0]
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

