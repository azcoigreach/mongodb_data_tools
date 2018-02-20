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
import pickle
from colorama import Fore

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
    ctx.vlog(Fore.LIGHTMAGENTA_EX + 'CLI Options')

    if end_time == None:
        end_time = (datetime.now() + timedelta(hours=tz_offset))
        
    else:
        try:
            end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M')
        except ValueError as ve:
            ctx.log(Fore.LIGHTRED_EX + 'Error: %s', ve)
            sys.exit(-1)
    if (offset == None) and (start_time == None):
        offset = 24
        start_time = (end_time - timedelta(hours=offset))
    elif (offset != None) and (start_time != None):
        ctx.log('Can not use OFFSET and START_TIME together. Pick one.')
        sys.exit(-1)
    elif (offset != None):
        start_time = (end_time - timedelta(hours=offset))
    elif (start_time != None):
        try:
            start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M')
        except ValueError as ve:
            ctx.log(Fore.LIGHTRED_EX + 'Error: %s', ve)
            sys.exit(-1)
    
    ctx.end_time = end_time
    ctx.start_time = start_time
    ctx.limit = limit
    ctx.vlog(Fore.LIGHTGREEN_EX + 'Limit: ' + Fore.LIGHTYELLOW_EX + '%s', ctx.limit)
    ctx.vlog(Fore.LIGHTGREEN_EX + 'End Time: ' + Fore.LIGHTYELLOW_EX + '%s', ctx.end_time)
    ctx.vlog(Fore.LIGHTGREEN_EX + 'Start Time: ' + Fore.LIGHTYELLOW_EX + '%s', ctx.start_time)
    ctx.vlog(Fore.LIGHTGREEN_EX + 'Offset: ' + Fore.LIGHTYELLOW_EX + '%s', offset)
    ctx.vlog(Fore.LIGHTGREEN_EX + 'TZ Offset: ' + Fore.LIGHTYELLOW_EX + '%s', tz_offset)


@cli.command('top_users', short_help='top_users stuff')
@pass_context
def top_users(ctx):
    '''Query top users doc!!!'''

    try:
        ctx.vlog(Fore.LIGHTMAGENTA_EX + 'Top Users')
        
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

    except Exception as err:
        ctx.log(err)
    
   
    query = db[client_col].aggregate([
            {'$match': {'created_at': {'$gte': ctx.start_time,
                                       '$lte': ctx.end_time}}},
            {'$group': {'_id': '$user.screen_name',
                        'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit' : ctx.limit}])
    
    users = []
    with open(ctx.home + '/top_users.pickle', 'wb') as f:
            
        try: 
            for i in iter(query):
                ctx.log(Fore.LIGHTRED_EX + i['_id'] + ' : ' + str(i['count']))
                users.append(i)
            
            pickle.dump(users, f)

        except TypeError as te:
            ctx.log(Fore.LIGHTRED_EX + 'Error: %s', te)
            ctx.log(Fore.LIGHTYELLOW_EX + '***')
            ctx.log(Fore.LIGHTCYAN_EX + query)


@cli.command('count', short_help='count tweets in db')
@pass_context
def count(ctx):
    '''count tweets in db'''

    try:
        ctx.vlog(Fore.LIGHTMAGENTA_EX + 'Count Tweets')
        
        client_host = data_tools.configs.mongodb.MONGO_HOST
        client_port = data_tools.configs.mongodb.MONGO_PORT
        client_db = data_tools.configs.mongodb.MONGO_DB
        client_col = data_tools.configs.mongodb.MONGO_COL
        
        ctx.vlog(Fore.LIGHTCYAN_EX + 'Host: ' + Fore.LIGHTYELLOW_EX + '%s', client_host)
        ctx.vlog(Fore.LIGHTCYAN_EX + 'Port: ' + Fore.LIGHTYELLOW_EX + '%s', client_port)
        ctx.vlog(Fore.LIGHTCYAN_EX + 'DB: ' + Fore.LIGHTYELLOW_EX + '%s', client_db)
        ctx.vlog(Fore.LIGHTCYAN_EX + 'Collection: ' + Fore.LIGHTYELLOW_EX + '%s', client_col)

        client = MongoClient(client_host, client_port)
        db = client[client_db]
        ctx.vlog(Fore.LIGHTMAGENTA_EX + 'MongoDB connected...')

    except Exception as err:
        ctx.log(err)
   
    query = db[client_col].find({'created_at': {'$gte': ctx.start_time,
                                                '$lte': ctx.end_time}}).count()
    ctx.log(Fore.LIGHTRED_EX + 'count: ' + Fore.LIGHTYELLOW_EX + '%s', query)


@cli.command()
def clear():
    """Clears the entire screen."""
    click.clear()
