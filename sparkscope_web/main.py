from flask import Flask, render_template, jsonify, request

from db.base import Session, engine, Base
from db.entities.application import ApplicationEntity
from db.entities.executor import ExecutorEntity
from db.entities.job import JobEntity
from db.entities.stage import StageEntity
from db.entities.stage_statistics import StageStatisticsEntity
from db.entities.task import TaskEntity
from sparkscope_web.analyzers.stage_analyzer import StageAnalyzer
from sparkscope_web.config import Config
from sparkscope_web.graphs import BarChartCreator
from sparkscope_web.forms import SearchForm, CompareForm, HistoryForm

import requests

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
app = Flask(__name__)
app.config.from_object(Config)

# Initialize the database
Base.metadata.create_all(engine)
session = Session()


@app.route('/')
def home():
    all_counts = [
        {'entity': 'applications', 'count': session.query(ApplicationEntity).count()},
        {'entity': 'executors', 'count': session.query(ExecutorEntity).count()},
        {'entity': 'jobs', 'count': session.query(JobEntity).count()},
        {'entity': 'stage', 'count': session.query(StageEntity).count()},
        {'entity': 'stage_statistics', 'count': session.query(StageStatisticsEntity).count()},
        {'entity': 'tasks', 'count': session.query(TaskEntity).count()},
    ]
    bcc = BarChartCreator()
    bar_chart = bcc.create_chart(all_counts, 'entity', 'count')
    return render_template('index.html', chart=bar_chart)


@app.route('/search', methods=['GET', 'POST'])
def search():
    search_form = SearchForm()
    apps = session.query(ApplicationEntity).order_by(ApplicationEntity.start_time.desc())
    if request.method == "POST" and search_form.validate_on_submit():
        apps = search_form.apply_filters(apps)
    apps = apps.limit(50)
    return render_template('search.html', form=search_form, apps=apps)


@app.route('/compare', methods=['GET', 'POST'])
def compare():
    compare_form = CompareForm(session=session)
    if request.method == "POST" and compare_form.validate_on_submit():
        pass
    return render_template('compare.html', form=compare_form)


@app.route('/apphistory', methods=["GET", "POST"])
def app_history():
    history_form = HistoryForm(session=session)
    if request.method == "POST" and history_form.validate_on_submit():
        pass
    return render_template('app_history.html', form=history_form)


@app.route('/app/<app_id>')
def application(app_id):
    search_form = SearchForm()
    spark_app = session.query(ApplicationEntity).get(app_id)
    basic_metrics = spark_app.get_basic_metrics()
    all_configs_json = dict(spark_app.spark_properties)
    return render_template('application.html',
                           form=search_form,
                           app=spark_app,
                           basic_metrics=basic_metrics,
                           all_configs_json=all_configs_json)


if __name__ == '__main__':
    app.run(debug=True)
