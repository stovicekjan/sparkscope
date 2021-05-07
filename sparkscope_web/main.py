from flask import Flask, render_template, jsonify, request, url_for
from sqlalchemy import func
from werkzeug.utils import redirect

from db.base import Session, engine, Base
from db.entities.application import ApplicationEntity
from db.entities.executor import ExecutorEntity
from db.entities.job import JobEntity
from db.entities.stage import StageEntity
from db.entities.task import TaskEntity
from sparkscope_web.config import Config
from sparkscope_web.graphs import GraphCreator
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
    """
    Home page
    """
    counts = {
        'applications': session.query(func.count(ApplicationEntity.app_id)).scalar(),
        'executors': session.query(func.count(ExecutorEntity.app_id)).scalar(),
        'jobs': session.query(func.count(JobEntity.app_id)).scalar(),
        'stages': session.query(func.count(StageEntity.app_id)).scalar(),
        'tasks': session.query(func.sum(StageEntity.num_complete_tasks)).scalar(),
    }
    newest_date = session.query(func.max(ApplicationEntity.end_time)).scalar().strftime("%Y-%m-%d %X")
    return render_template('index.html', counts=counts, newest_date=newest_date)


@app.route('/search', methods=['GET', 'POST'])
def search():
    """
    Search page
    """
    search_form = SearchForm()
    apps = session.query(ApplicationEntity).order_by(ApplicationEntity.start_time.desc())
    if request.method == "POST" and search_form.validate_on_submit():
        apps = search_form.apply_filters(apps)
    apps = apps.limit(50)
    return render_template('search.html', form=search_form, apps=apps)


@app.route('/compare', methods=['GET', 'POST'])
def compare():
    """
    Compare page
    """
    compare_form = CompareForm(session=session)
    if request.method == "POST" and compare_form.validate_on_submit():
        spark_app_1 = session.query(ApplicationEntity).get(compare_form.app_id_1.data)
        spark_app_2 = session.query(ApplicationEntity).get(compare_form.app_id_2.data)
        basic_metrics_1 = spark_app_1.get_basic_metrics()
        basic_metrics_2 = spark_app_2.get_basic_metrics()
        basic_configs_1 = spark_app_1.get_basic_configs()
        basic_configs_2 = spark_app_2.get_basic_configs()
        all_configs_json_1 = spark_app_1.get_spark_properties_as_dict()
        all_configs_json_2 = spark_app_2.get_spark_properties_as_dict()
        return render_template('compare.html',
                               form=compare_form,
                               spark_app_1=spark_app_1,
                               spark_app_2=spark_app_2,
                               basic_metrics_1=basic_metrics_1,
                               basic_metrics_2=basic_metrics_2,
                               basic_configs_1=basic_configs_1,
                               basic_configs_2=basic_configs_2,
                               all_configs_json_1=all_configs_json_1,
                               all_configs_json_2=all_configs_json_2,
                               )
    return render_template('compare.html', form=compare_form)


@app.route('/historyform', methods=["GET", "POST"])
def history():
    """
    Empty History page (with form to be filled)
    """
    history_form = HistoryForm(session=session)
    if request.method == "POST" and history_form.validate_on_submit():
        return redirect(url_for('app_history', app_name=history_form.app_name.data))
    return render_template('app_history.html', form=history_form, app_name=None)


@app.route('/apphistory/<app_name>', methods=["GET", "POST"])
def app_history(app_name):
    """
    History page for a specific application
    :param app_name: application name (string)
    """
    history_form = HistoryForm(session=session)
    spark_apps = session.query(
                               ApplicationEntity.start_time,
                               ApplicationEntity.duration,
                               ApplicationEntity.app_id
                               ) \
                        .filter(ApplicationEntity.name == app_name) \
                        .order_by(ApplicationEntity.start_time) \
                        .all()
    app_data = [{
        'start_time': app.start_time,
        'duration': app.duration,
        'app_id': app.app_id,
    } for app in spark_apps]
    plot = GraphCreator.create_line_chart(app_data)
    if request.method == "POST":
        if history_form.validate_on_submit():
            return render_template('app_history.html', form=history_form, plot=plot, app_name=app_name)
        else:
            return None
    if request.method == "GET":
        return render_template('app_history.html', form=history_form, plot=plot, app_name=app_name)


@app.route('/app/<app_id>')
def application(app_id):
    """
    Application page
    :param app_id: application id (string)
    :return:
    """
    search_form = SearchForm()
    spark_app = session.query(ApplicationEntity).get(app_id)
    basic_metrics = spark_app.get_basic_metrics()
    basic_configs = spark_app.get_basic_configs()
    all_configs_json = spark_app.get_spark_properties_as_dict()
    return render_template('application.html',
                           form=search_form,
                           app=spark_app,
                           basic_metrics=basic_metrics,
                           basic_configs=basic_configs,
                           all_configs_json=all_configs_json)


if __name__ == '__main__':
    app.run(debug=False)
