import configparser
import os


class Config(object):
    SECRET_KEY = os.environ.get('SECRET KEY') or 'the-secret-key'  # CSRF protection key

    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), '..', 'history_fetcher', 'config.ini'))
    HISTORY_SERVER_BASE_URL = config.get('history_fetcher', 'url')
