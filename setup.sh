#!/usr/bin/env bash

cp db/db_config.ini.template db/db_config.ini
cp history_fetcher/config.ini.template history_fetcher/config.ini
cp history_fetcher/logger.conf.template history_fetcher/logger.conf
cp sparkscope_web/metrics/user_config.conf.template history_fetcher/user_config.conf

export PYTHONPATH=$PYTHONPATH:`pwd`