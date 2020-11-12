# Sparkscope

A tool for diagnostics and support of optimization of Apache Spark applications. It should consist of two parts:
* History Fetcher - a component for retrieving the Spark applications metadata from Spark History Server and storing them in PostgreSQL database.
* A web application for processing and visualization of these data

## Installation guide

1. Initialize database
    * Create new PostgreSQL database (preferably named `sparkscope`)
    * Keep the database empty (the content is handled by SQLAlchemy framework)
    
2. Clone or download the project from GitHub

3. Create the config files from templates
    * `cp db/db_config.ini.template db/db_config.ini`
    * `cp history_fetcher/config.ini.template history_fetcher/config.ini`
    * `cp history_fetcher/logger.conf.template history_fetcher/logger.conf`

4. Modify the config files if necessary (hints inside)

## Usage

#### A. History Fetcher

`python3 history_fetcher/main.py [-h | --help] [--test-mode] [--truncate]`

Arguments

 Argument  | Description 
---------- | -----------
-h, --help | Show the help message end exit
--test-mode| Fetch only some defined number of the newest application records, instead of fetching either all non-processed applications
--truncate | Truncate all tables in the database before fetching the new records

#### B. TBD