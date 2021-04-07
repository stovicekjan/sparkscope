import os


class Config(object):
    SECRET_KEY = os.environ.get('SECRET KEY') or 'the-secret-key'  # CSRF protection key
    HISTORY_SERVER_BASE_URL = "https://tmv2768.devlab.de.tmo:18488"
