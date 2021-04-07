import pandas as pd
import plotly
import plotly.graph_objs as go
import numpy
import json
import plotly.express as px
from flask import url_for
from sparkscope_web.metrics.helpers import fmt_time


class GraphCreator:
    @staticmethod
    def create_bar_chart(json_data, x_coord_name, y_coord_name):
        x_data = [dataset[x_coord_name] for dataset in json_data]
        y_data = [dataset[y_coord_name] for dataset in json_data]
        df = pd.DataFrame({'entity': x_data, 'count': y_data})
        data = [go.Bar(x=df[x_coord_name], y=df[y_coord_name])]
        return json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

    @staticmethod
    def create_line_chart(json_data):
        x_data = [dataset['start_time'] for dataset in json_data]
        y_data = [dataset['duration']/1000 for dataset in json_data]
        app_ids = [dataset['app_id'] for dataset in json_data]
        x_label = "Start Time"
        y_label = "Duration (seconds)"
        data = [go.Scatter(x=x_data, y=y_data,
                           hovertemplate="%{text}<extra></extra>",
                           text=[f"App id: {app_id}<br>Start Time: {start_time}<br>Duration: {fmt_time(duration)}"
                                 for (app_id, start_time, duration) in zip(app_ids, x_data, y_data)],
                           showlegend=False
                           )]
        annotations = []
        for i in range(len(x_data)):
            annotations.append(dict(x=x_data[i],
                                    y=y_data[i],
                                    text=f"""<a href="{url_for('application', app_id=app_ids[i])}"> </a>""",
                                    font_size=18,
                                    showarrow=False))

        layout = go.Layout(annotations=annotations, hovermode="x", paper_bgcolor='rgba(0,0,0,0)')
        fig = go.Figure(data=data, layout=layout)
        fig.update_xaxes(rangeslider_visible=True, title_text=x_label)
        fig.update_yaxes(title_text=y_label)
        fig.update_traces(mode='lines+markers', marker_size=12)
        fig.update_traces(hovertext="a")
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
