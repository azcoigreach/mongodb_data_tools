#!/usr/bin/env python
import click
from data_tools.cli import pass_context
import dash
import dash_core_components as dcc
import dash_html_components as html

'''Dash GUI'''

@click.command('dash', short_help='Dash GUI.')
# @click.argument('path', required=False, type=click.Path(resolve_path=True))
@pass_context
def cli(ctx):
    """Dash GUI"""
    # if path is None:
    #     path = ctx.home
    ctx.log('Initialized Dash GUI')
    # ctx.log('Initialized the repository in %s',
    #         click.format_filename(path))

    app = dash.Dash()

    app.layout = html.Div(children=[
        html.H1('Dash tutorials'),
        dcc.Graph(id='example',
            figure ={
                'data': [
                    {'x':[1,2,3,4,5], 'y':[1,2,3,4,5], 'type':'line', 'name':'boats'},
                    {'x':[1,2,3,4,5], 'y':[1,2,3,4,5], 'type':'bar', 'name':'cars'},
                     ],
                'layout': {
                    'title': 'Basic Example',
                }
            })
        ])

    app.run_server(debug=True)