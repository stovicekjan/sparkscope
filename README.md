# Sparkscope

A tool for diagnostics and support of optimization of Apache Spark applications. It should consist of two parts:
* History Fetcher - a component for retrieving the Spark applications metadata from Spark History Server and storing them in PostgreSQL database.
* A web application for processing and visualization of these data

## Installation guide

Before the installation of Sparkscope, you need the following prerequisities:
* Python 3.6 installed
* pip, a package installer for Python
* PostgreSQL with admin console access
* Linux environment
* Running instance of Spark History Server

In order to install and start Sparkscope, you should do the following steps:

1. Get the Sparkscope files to the target location, either from the installation medium, or by cloning from Github with the following command:

    `git clone https://github.com/stovicekjan/sparkscope.git` 

2. Open the target directory (its subdirectories should be `db`, `history_fetcher`, `logger`, `sparkscope_web` and`test`)

3. Create Python virtual environment with the following command:

    `python3 -m venv venv` 

4. Run the `setup.sh` script to initiate the the configuration files and prepare the setup of the `PYTHONPATH`.

5. Activate venv with the following command:

    `source venv/bin/activate`

6. Adjust the `db/setup_db.sql` script (modify the password if necessary), and afterwards, run the script.

7. Edit the `db/db_config.ini` file and fill in the PostgreSQL password (use the value from the previous point) and the database hostname.

8. Edit the `history_fetcher/config.ini` file and fill in `base_url` and `url` parameters, so that they point to your Spark History Server instance.

9. Edit the `history_fetcher/logger.conf` file. In the `handler_fileHandler` section, modify the `args` parameter and specify the path where the logs should be stored. You also need to create this path if it does not exist.

10. When the venv is activated, run the following command to install the necessary Python packages:

    `pip install -r requirements.txt` 

11. Load an initial data with History Fetcher 

12. Start Sparkscope web application with the following command:

    `python3 sparkscope_web/main.py`

## Usage

#### A. History Fetcher

`python3 history_fetcher/main.py [-h | --help] [--test-mode] [--truncate]`

Arguments

 Argument  | Description 
---------- | -----------
-h, --help | Show the help message end exit
--test-mode| Fetch only some defined number of the newest application records, instead of fetching either all non-processed applications
--truncate | Truncate all tables in the database before fetching the new records


#### B. Sparkscope web application

The usage of the web application if pretty straightforward. Nevertheless, you might want to check the `sparkscope_web/metrics/user_config.conf` file and configure the metrics base on your needs.