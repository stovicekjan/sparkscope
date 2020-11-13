import re
import os
import logging
import configparser
from logger.logger import SparkscopeLogger

"""
Set up logger
"""
# logger = logging.getLogger(__name__)
logger = SparkscopeLogger(__name__)


class Utils:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
        self.base_url = self.config['history_fetcher']['base_url']

    def get_app_id_from_url(self, url):
        """
        Gets application_id from url
        :param url: url with pattern 'https://history.server:port/api/v1/applications/<app_id>/something_more'
        :return: application_id
        """
        parse = re.search(rf'{self.base_url}/([a-zA-Z0-9\-_]*)(/.*)*', url.rstrip("/"))
        return None if parse is None else parse.group(1)

    def get_stage_key_from_url(self, url):
        """
        Gets stage_key from url.
        :param url: url with pattern 'https://history.server:port/api/v1/applications/<app_id>/stages/<stage_id>
        :return: stage_key
        """
        parse = re.search(rf'{self.base_url}/([a-zA-Z0-9\-_]+)/stages/([0-9]+)(/.*)*', url.rstrip("/"))
        stage_key = None if parse is None else f"{parse.group(1)}_{parse.group(2)}"
        return stage_key

    def get_app_id_from_stage_key(self, stage_key):
        parse = re.search(rf'([a-zA-Z0-9\-_]+)_([0-9]+)', stage_key)
        app_id = None if parse is None else parse.group(1)
        return app_id

    def get_prop(self, obj, prop):
        """
        Get property value if property exists
        :param obj: dictionary
        :param prop: the property key
        :return:
        """
        try:
            return obj[prop]
        except:
            return None

    # TODO refactor to make this more generic (get_attribute_from_array or so)
    def get_sun_java_command(self, env_data, app_id):
        """
        Extracts sun.java.command (spark-submit with all the parameters) from environment json
        :param env_data: environment json
        :param app_id: application_id
        :return: string with sun.java.command (spark-submit)
        """
        system_properties = self.get_prop(env_data, 'systemProperties')
        if system_properties is None:
            return None
        command = [v for [k, v] in system_properties if k == 'sun.java.command']
        if len(command) != 1:
            logger.warning(f"cannot obtain sun.java.command for {app_id}")
            return None
        return command[0]

