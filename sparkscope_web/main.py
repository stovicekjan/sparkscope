from flask import Flask, render_template, jsonify, request

from db.base import Session, engine, Base
from db.entities.application import Application
from db.entities.executor import Executor
from db.entities.job import Job
from db.entities.stage import Stage
from db.entities.stage_statistics import StageStatistics
from db.entities.task import Task
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
        {'entity': 'applications', 'count': session.query(Application).count()},
        {'entity': 'executors', 'count': session.query(Executor).count()},
        {'entity': 'jobs', 'count': session.query(Job).count()},
        {'entity': 'stage', 'count': session.query(Stage).count()},
        {'entity': 'stage_statistics', 'count': session.query(StageStatistics).count()},
        {'entity': 'tasks', 'count': session.query(Task).count()},
    ]
    bcc = BarChartCreator()
    bar_chart = bcc.create_chart(all_counts, 'entity', 'count')
    return render_template('index.html', chart=bar_chart)


@app.route('/search', methods=['GET', 'POST'])
def search():
    search_form = SearchForm()
    apps = session.query(Application).order_by(Application.start_time.desc())
    if request.method == "POST" and search_form.validate_on_submit():
        search_app_name = f"%{search_form.app_name.data}%"
        search_app_id = f"%{search_form.app_id.data}%"
        search_username = f"%{search_form.username.data}%"
        start_from = search_form.start_from.data
        start_to = search_form.start_to.data
        end_from = search_form.end_from.data
        end_to = search_form.end_to.data
        apps = apps.filter(Application.name.like(search_app_name)) \
                   .filter(Application.app_id.like(search_app_id)) \
                   .filter(Application.spark_user.like(search_username))
        apps = apps.filter(Application.start_time >= start_from) if start_from else apps
        apps = apps.filter(Application.start_time <= start_to) if start_to else apps
        apps = apps.filter(Application.end_time >= end_from) if end_from else apps
        apps = apps.filter(Application.end_time <= end_to) if end_to else apps
    apps = apps.limit(50)
    return render_template('search.html', form=search_form, apps=apps)


@app.route('/compare', methods=["GET", "POST"])
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
    return render_template('application.html', form=search_form)

if __name__ == '__main__':
    app.run(debug=True)
