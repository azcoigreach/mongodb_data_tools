#!/usr/bin/env python
import click
from data_tools.cli import pass_context
import data_tools.configs.mongodb
from pymongo import MongoClient
from datetime import datetime
import dateutil.parser
import json

@click.command('fix_dates', short_help='Fix Twitter Unicode Dates in MongoDB')
@pass_context
def cli(ctx):
    '''Repairs Unicode dates in MongoDB Twitter Database and replaces fields with datetime method'''
    ctx.log('fix_date app...')
    try: 
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
        
        def getDatetimeFromISO(s):
            d = dateutil.parser.parse(s)
            return d

        def fix_dates():
            results = db[client_db].find({})
            for tweet in results:
                tweet_date = tweet['created_at']
                if (type(tweet_date) == unicode):
                    proper_date = getDatetimeFromISO(tweet['created_at'])
                    tweet['created_at'] = proper_date
                    pointer = tweet['_id']
                    db[client_col].update({'_id': pointer}, {'$set': {'created_at': proper_date}})
                    ctx.log('updated %s', tweet['created_at'])
                else:
                    ctx.log('no unicode date - skipping')
                    
    except Exception as err:
        ctx.log(err)

