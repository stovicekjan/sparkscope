import pytest
import time

from history_fetcher.utils import Utils
import history_fetcher.utils as utils

u = Utils()
u.base_url = "https://spark.history.server.com:18488/api/v1/applications"


def test_get_app_id_from_url():
    urls = [
        "https://spark.history.server.com:18488/api/v1/applications",
        "https://spark.history.server.com:18488/api/v1/applications/app_id_12345",
        "https://spark.history.server.com:18488/api/v1/applications/app_id_12345/",
        "https://spark.history.server.com:18488/api/v1/applications/app_id_12345/environment",
        "https://www.google.com",
        "https://spark.history.server.com:18488/api/v1/applications/application_1602836119886_0201",
    ]

    assert u.get_app_id_from_url(urls[0]) is None
    assert u.get_app_id_from_url(urls[1]) == "app_id_12345"
    assert u.get_app_id_from_url(urls[2]) == "app_id_12345"
    assert u.get_app_id_from_url(urls[3]) == "app_id_12345"
    assert u.get_app_id_from_url(urls[4]) is None
    assert u.get_app_id_from_url(urls[5]) == "application_1602836119886_0201"


def test_get_stage_key_from_url():
    urls = [
        "https://spark.history.server.com:18488/api/v1/applications/app_id_12345/stages/10",
        "https://spark.history.server.com:18488/api/v1/applications/app_id_12345/stages/10/taskSummary",
        "https://spark.history.server.com:18488/api/v1/applications",
        "https://spark.history.server.com:18488/api/v1/applications/app_id_12345/",
        "https://spark.history.server.com:18488/api/v1/applications/app_id_12345/stages/",
        "https://spark.history.server.com:18488/api/v1/applications/application_1602836119886_0201",
    ]

    assert u.get_stage_key_from_url(urls[0]) == "app_id_12345_10"
    assert u.get_stage_key_from_url(urls[1]) == "app_id_12345_10"
    assert u.get_stage_key_from_url(urls[2]) is None
    assert u.get_stage_key_from_url(urls[3]) is None
    assert u.get_stage_key_from_url(urls[4]) is None
    assert u.get_stage_key_from_url(urls[5]) is None


def test_get_app_id_from_stage_key():
    stage_keys = [
        "applicationId_123_456_555",
        "local-123_444",
        "aaa_444"
    ]

    assert u.get_app_id_from_stage_key(stage_keys[0]) == "applicationId_123_456"
    assert u.get_app_id_from_stage_key(stage_keys[1]) == "local-123"
    assert u.get_app_id_from_stage_key(stage_keys[2]) == "aaa"


def test_get_prop():
    obj1 = {
        'prop1': 'val1',
        'prop2': {
            'sub1': 'val3',
            'sub2': {
                'subsub1': 'val5',
                'subsub2': 'val6'
            }
        }
    }

    assert utils.get_prop(obj1, "prop1") == "val1"
    assert utils.get_prop(obj1, "prop3") is None
    assert utils.get_prop(obj1, 'prop2', 'sub1') == 'val3'
    assert utils.get_prop(obj1, 'prop2', 'sub3') is None
    assert utils.get_prop(obj1, 'prop2', 'sub2', 'subsub2') is 'val6'



test_get_app_id_from_url()
test_get_stage_key_from_url()
test_get_app_id_from_stage_key()
test_get_prop()
