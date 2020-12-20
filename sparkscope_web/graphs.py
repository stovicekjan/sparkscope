import pandas as pd
import plotly
import plotly.graph_objs as go
import numpy
import json


class GraphCreator:
    def create_chart(self, json_data, x_coord_name, y_coord_name):
        """
        abstract method
        :return:
        """
        pass


class BarChartCreator(GraphCreator):
    def create_chart(self, json_data, x_coord_name, y_coord_name):
        x_data = [dataset[x_coord_name] for dataset in json_data]
        y_data = [dataset[y_coord_name] for dataset in json_data]
        df = pd.DataFrame({'entity': x_data, 'count': y_data})

        data = [go.Bar(x=df[x_coord_name], y=df[y_coord_name])]

        return json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)