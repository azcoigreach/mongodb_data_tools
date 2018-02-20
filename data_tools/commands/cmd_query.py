# coding: utf-8
import sys
import click
from data_tools.cli import pass_context
import data_tools.configs.mongodb
from pymongo import MongoClient
import pprint
from datetime import datetime, timedelta
import json
import string
import math
import time
import random

try:
    range_type = xrange
except NameError:
    range_type = range
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
def cli(ctx, offset, end_time, start_time, limit, tz_offset):
    '''Common variable context'''
    ctx.limit = limit
    

    if end_time == None:
        ctx.end_time = (datetime.now() + timedelta(hours=tz_offset))
        
    else:
        ctx.end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M')
    
    if (offset == None) and (start_time == None):
        offset = 24
        ctx.start_time = (end_time - timedelta(hours=offset))
    elif (offset != None) and (start_time != None):
        ctx.log('Can not use OFFSET and START_TIME together. Pick one.')
        sys.exit(-1)
    elif (offset != None):
        start_time = (end_time - timedelta(hours=offset))
    elif (start_time != None):
        ctx.start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M')
    
    ctx.vlog('limit: %s', ctx.limit)
    ctx.vlog('end_time: %s', ctx.end_time)
    ctx.vlog('start_time: %s', ctx.start_time)
    ctx.vlog('offset: %s', offset)
    ctx.vlog('tz_offset: %s', tz_offset)

@cli.command('top_users', short_help='top_users stuff')
@pass_context
def top_users(ctx):
    '''Query top users doc!!!'''

    try:
        ctx.log('top users log')
        ctx.vlog('top users, debug info')
        
        client_host = data_tools.configs.mongodb.MONGO_HOST
        client_port = data_tools.configs.mongodb.MONGO_PORT
        client_db = data_tools.configs.mongodb.MONGO_DB
        client_col = data_tools.configs.mongodb.MONGO_COL
        
        ctx.vlog(client_host)
        ctx.vlog(client_port)
        ctx.vlog(client_db)
        ctx.vlog(client_col)

        client = MongoClient(client_host, client_port)
        db = client[client_db]
        ctx.log('MongoDB connected...')

    except Exception as err:
        ctx.log(err)
    
   
    query = db[client_col].aggregate([
            {'$match': {'created_at': {'$gte': ctx.start_time,
                                       '$lte': ctx.end_time}}},
            {'$group': {'_id': '$user.screen_name',
                        'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit' : ctx.limit}])
    try: 
        tweets = iter(query)
        for i in tweets:
            ctx.log(i)
            
    except TypeError, te:
        ctx.log(te)
        ctx.log('***')
        ctx.log(query)


@cli.command('count', short_help='count tweets in db')
@pass_context
def top_users(ctx):
    '''count tweets in db'''

    try:
        ctx.log('count tweets')
        ctx.vlog('count tweets, debug info')
        
        client_host = data_tools.configs.mongodb.MONGO_HOST
        client_port = data_tools.configs.mongodb.MONGO_PORT
        client_db = data_tools.configs.mongodb.MONGO_DB
        client_col = data_tools.configs.mongodb.MONGO_COL
        
        ctx.vlog(client_host)
        ctx.vlog(client_port)
        ctx.vlog(client_db)
        ctx.vlog(client_col)

        client = MongoClient(client_host, client_port)
        db = client[client_db]
        ctx.log('MongoDB connected...')

    except Exception as err:
        ctx.log(err)

    
   
    query = db[client_col].find({'created_at': {'$gte': ctx.start_time,
                                                '$lte': ctx.end_time}}).count()
    ctx.log('count: %s', query)
            


@cli.command()
def clear():
    """Clears the entire screen."""
    click.clear()
