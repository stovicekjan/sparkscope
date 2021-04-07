from flask import Flask, render_template, jsonify, request, url_for
from werkzeug.utils import redirect

from db.base import Session, engine, Base
from db.entities.application import ApplicationEntity
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
    # all_counts = [
    #     {'entity': 'applications', 'count': 6},
    #     {'entity': 'executors', 'count': 5},
    #     {'entity': 'jobs', 'count': 1},
    #     {'entity': 'stage', 'count': 0},
    #     {'entity': 'stage_statistics', 'count': 3},
    #     {'entity': 'tasks', 'count': 3},
    # ]
    # bar_chart = GraphCreator.create_bar_chart(all_counts, 'entity', 'count')
    # return render_template('index.html', chart=bar_chart)
    return render_template('index.html')


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
    history_form = HistoryForm(session=session)
    if request.method == "POST" and history_form.validate_on_submit():
        return redirect(url_for('app_history', app_name=history_form.app_name.data))
    return render_template('app_history.html', form=history_form, app_name=None)


@app.route('/apphistory/<app_name>', methods=["GET", "POST"])
def app_history(app_name):
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
    app.run(debug=True)
