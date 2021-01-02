import os


class Config(object):
    SECRET_KEY = os.environ.get('SECRET KEY') or 'the-secret-key'  # CSRF protection key
