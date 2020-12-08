import re
import os
import logging
import configparser
from logger.logger import SparkscopeLogger

# Set up logger
logger = logging.getLogger(__name__)


class Utils:
    """
    Utilities for History Fetcher
    """
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
        self.base_url = self.config['history_fetcher']['base_url']

    def get_app_id_from_url(self, url):
        """
        Get application_id from url.
        :param url: url with pattern 'https://history.server:port/api/v1/applications/<app_id>/something_more'
        :return: application_id or None (if the url is invalid or malformed)
        """
        parse = re.search(rf'{self.base_url}/([a-zA-Z0-9\-_]*)(/.*)*', url.rstrip("/"))
        return None if parse is None else parse.group(1)

    def get_stage_key_from_url(self, url):
        """
        Get stage_key from url.
        :param url: url with pattern 'https://history.server:port/api/v1/applications/<app_id>/stages/<stage_id>/...
        :return: stage_key or None (if the url is invalid or malformed)
        """
        parse = re.search(rf'{self.base_url}/([a-zA-Z0-9\-_]+)/stages/([0-9]+)(/.*)*', url.rstrip("/"))
        stage_key = None if parse is None else f"{parse.group(1)}_{parse.group(2)}"
        return stage_key

    def get_app_id_from_stage_key(self, stage_key):
        """
        Get application_id from stage_key (as a substring)
        :param stage_key: A stage key with pattern <application_id>_<number>
        :return: application_id or None (if the stage_key is malformed)
        """
        parse = re.search(rf'([a-zA-Z0-9\-_]+)_([0-9]+)', stage_key)
        app_id = None if parse is None else parse.group(1)
        return app_id


def get_prop(obj, *prop_list):
    """
    Get property value if property exists. Works recursively with a list representation of the property hierarchy.
    E.g. with obj = {'a': 1, 'b': {'c': {'d': 2}}}, calling get_prop(obj, 'b', 'c', 'd') returns 2.
    :param obj: dictionary
    :param prop_list: list of the keys
    :return: the property value or None if the property doesn't exist
    """
    if len(prop_list) == 1:
        if prop_list[0] in obj.keys():
            return obj[prop_list[0]]
        else:
            return None

    if prop_list[0] in obj.keys():
        return get_prop(obj[prop_list[0]], *prop_list[1:])
    else:
        return None


def get_system_property(env_data, app_id, property_name):
    """
    Extracts the desired systemProperty, which is a part of Environment endpoint
    :param env_data: environment json
    :param app_id: application_id
    :param property_name: name of the property that should be found
    :return: the value of the property
    """
    system_properties = get_prop(env_data, 'systemProperties')
    if system_properties is None:
        return None
    command = [v for [k, v] in system_properties if k == property_name]
    if len(command) != 1:
        logger.warning(f"cannot obtain {property_name} for {app_id}")
        return None
    return command[0]
