from flask import Flask, render_template, jsonify

from db.base import Session, engine, Base
from db.entities.application import Application
from db.entities.executor import Executor
from db.entities.job import Job
from db.entities.stage import Stage
from db.entities.stage_statistics import StageStatistics
from db.entities.task import Task
from sparkscope_web.graphs import BarChartCreator


import requests

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
app = Flask(__name__)

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


@app.route('/search')
def search():
    return render_template('search.html')


@app.route('/compare')
def compare():
    return render_template('compare.html')


@app.route('/apphistory')
def app_history():
    return render_template('app_history.html')

if __name__ == '__main__':
    app.run(debug=True)
