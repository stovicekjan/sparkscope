import configparser

class UrlCreator:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read("config.ini")

        self.base_url = config['history_fetcher']['base_url']

    def create_apps_url(self):
        return f"{self.base_url}?status=completed&limit=10"

    def create_env_url(self, app_id, mode, attempt_id=None):
        if mode == "client":
            return f"{self.base_url}/{app_id}/environment"
        elif mode == "cluster":
            if attempt_id is None:
                raise ValueError(f"Missing attempt_id for {app_id}")
            return f"{self.base_url}/{app_id}/{attempt_id}/environment"
        else:
            raise ValueError(f"Unsupported mode: {mode} for {app_id}")

    def create_executor_url(self, app_id):
        return f"{self.base_url}/{app_id}/executors"

    def create_job_url(self, app_id):
        return f"{self.base_url}/{app_id}/jobs"

    def create_stage_url(self, app_id):
        return f"{self.base_url}/{app_id}/stages"

    def create_stage_statistics_url(self, app_id, stage_id):
        return f"{self.base_url}/{app_id}/stages/{stage_id}/taskSummary"

    def create_task_url(self, app_id, stage_id):
        return f"{self.base_url}/{app_id}/stages/{stage_id}/taskList"
