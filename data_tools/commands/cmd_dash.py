#!/usr/bin/env python
import click
from data_tools.cli import pass_context
import data_tools.configs.mongodb
from pymongo import MongoClient
import dash
from dash.dependencies import Output, Event
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
from pandas.io.json import json_normalize
import plotly
import plotly.graph_objs as go
from collections import deque
from datetime import datetime, timedelta
from colorama import Fore



@click.option('--refresh', '-r', default=None, type=int,
              help='Refresh value in seconds')
@click.option('--tz_offset', '-tz', default=0, type=int,
              help='Time Zone offset (db times are GMT)')
@click.option('--offset', '-o', default=None, type=int,
              help='Time frame of query - X hours offset before end_time')
@click.option('--end_time', '-e', default=None,
              help='Query start datetime "2018-02-19 16:20"')
@click.option('--start_time', '-s', default=None,
              help='Query start datetime "2018-02-18 16:20"')
@click.option('--limit', '-l', default=10,
              help='Limit query results')
@click.group()
@pass_context
def cli(ctx, offset, end_time, start_time, limit, tz_offset, refresh):
    '''Dash GUI'''
    ctx.end_time = end_time
    ctx.start_time = start_time
    ctx.limit = limit
    ctx.refresh = refresh
    ctx.tz_offset = tz_offset
    ctx.offset = offset

@cli.command('dash_demo', short_help='Dash GUI.')
@pass_context    
def dash_demo(ctx):
    """Dash GUI"""
    
    ctx.log('Initialized Dash GUI Demo')

    X = deque(maxlen=20)
    Y = deque(maxlen=20)
    X.append(1)
    Y.append(1)

    app = dash.Dash(__name__)

    app.layout = html.Div([
        dcc.Graph(id='livegraph', animate=True),
        dcc.Interval(
            id='graph-update',
            interval=1000
            )
        ])

    @app.callback(
        Output(component_id='output', component_property='children'),
        [Input(component_id='input', component_property='value')]
        )   
    def update_value(input_data):
        try:
            return str(float(input_data)**2)
        except:
            return "Some error."
    app.run_server(debug=True)


'''
DASHBOARD APP

'''


@cli.command('dashboard', short_help='dash stuff')
@pass_context
def dashboard(ctx):
    '''Dash Gui'''

    app = dash.Dash(__name__)
    app.layout = html.Div(
        [   html.H2('Live Twitter Sentiment'),
            dcc.Graph(id='live-graph', animate=True),
            dcc.Interval(
                id='graph-update',
                interval=5*1000
            ),
        ]
    )   

    @app.callback(Output('live-graph', 'figure'),
              events=[Event('graph-update', 'interval')])
    def update_graph_scatter():
        try:
            ctx.vlog(Fore.LIGHTMAGENTA_EX + 'Dashboard')
            
            client_host = data_tools.configs.mongodb.MONGO_HOST
            client_port = data_tools.configs.mongodb.MONGO_PORT
            client_db = data_tools.configs.mongodb.MONGO_DB
            client_col = data_tools.configs.mongodb.MONGO_COL
            
            ctx.vlog(Fore.LIGHTCYAN_EX + 'Host: ' + Fore.LIGHTYELLOW_EX + '%s', client_host)
            ctx.vlog(Fore.LIGHTCYAN_EX + 'Port: ' + Fore.LIGHTYELLOW_EX + '%s',client_port)
            ctx.vlog(Fore.LIGHTCYAN_EX + 'DB: ' + Fore.LIGHTYELLOW_EX + '%s',client_db)
            ctx.vlog(Fore.LIGHTCYAN_EX + 'Collection: ' + Fore.LIGHTYELLOW_EX + '%s',client_col)

            client = MongoClient(client_host, client_port)
            db = client[client_db]
            ctx.vlog(Fore.LIGHTMAGENTA_EX + 'MongoDB connected...')
            
            query = { '$query' : {}, '$orderby' : { '$natural' : -1 }}
            projection = { '_id' : 0, 'timestamp_ms' : 1, 'text': 1, 'sentiment_polarity' : 1 ,'sentiment_subjectivity' : 1}
            max_results = 5000
        
            cursor = db[client_col].find(query, projection).limit(max_results)
                
            df = pd.DataFrame(list(cursor))
            df.sort_values('timestamp_ms', inplace=True)
            df['date'] = pd.to_datetime(df['timestamp_ms'], unit='ms')
            df.set_index('date', inplace=True)
            df = df.resample('1s').mean()
            df['smoothed_polarity'] = df['sentiment_polarity'].rolling(int(len(df)/5)).mean()
            df['smoothed_subjectivity'] = df['sentiment_subjectivity'].rolling(int(len(df)/5)).mean()
            
            X = df.index[-100:]
            Y = df.smoothed_polarity.values[-100:]
            Y1 = df.smoothed_subjectivity.values[-100:]
            
            ctx.vlog(Fore.LIGHTCYAN_EX + 'X: ' + Fore.LIGHTYELLOW_EX + '%s', X)
            ctx.vlog(Fore.LIGHTCYAN_EX + 'Y: ' + Fore.LIGHTYELLOW_EX + '%s', Y)
            ctx.vlog(Fore.LIGHTCYAN_EX + 'Y1: ' + Fore.LIGHTYELLOW_EX + '%s', Y1)
            
            data1 = {
                'x': X,
                'y': Y,
                    "marker": {

                    }, 
                "mode": "lines + markers", 
                "name": "polarity, y", 
                "type": "scatter", 
                }

            data2 = {
                'x': X,
                'y': Y1,
                    "marker": {
                        
                    }, 
                "mode": "lines + markers", 
                "name": "subjectivity, y", 
                "type": "scatter", 
                "xaxis": "x", 
                "yaxis": "y2",
                }
            
            return {'data': [data1, data2],'layout' : go.Layout(autosize=True, hovermode='closest',
                                                        showlegend=True,
                                                        xaxis={
                                                            "autorange": True, 
                                                            "domain": [0, 1], 
                                                            "range": [min(X), max(X)], 
                                                            "title": "Time", 
                                                            "type": "date"
                                                        },
                                                        yaxis={
                                                            "autorange": True, 
                                                            "range": [min(Y), max(Y)], 
                                                            "title": "Polarity", 
                                                            "type": "linear"
                                                        },
                                                        yaxis2={
                                                            "anchor": "x", 
                                                            "autorange": True, 
                                                            "overlaying": "y", 
                                                            "range": [min(Y1), max(Y1)], 
                                                            "side": "right", 
                                                            "title": "subjectivity", 
                                                            "type": "linear"
                                                        },
                                                        )}

        except Exception as err:
            ctx.log(err)
        
    app.run_server(debug=True)