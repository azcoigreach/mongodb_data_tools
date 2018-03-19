#!/usr/bin/env python
import click
from data_tools.cli import pass_context
import data_tools.configs.mongodb
from pymongo import MongoClient
from datetime import datetime
import dateutil.parser
import json
from colorama import Fore

def getDatetimeFromISO(s):
            d = dateutil.parser.parse(s)
            return d

@click.command()
@click.confirmation_option(help='Execute fix_dates without prompt.',
                           prompt='This operation may take a while, are you sure you want to continue?')
@pass_context
def cli(ctx):
    '''Repairs Unicode dates in Twitter DB and replaces fields with datetime method'''
    ctx.log(Fore.LIGHTMAGENTA_EX + 'fix_date app...')
    try: 
        client_host = data_tools.configs.mongodb.MONGO_HOST
        client_port = data_tools.configs.mongodb.MONGO_PORT
        client_db = data_tools.configs.mongodb.MONGO_DB
        client_col = data_tools.configs.mongodb.MONGO_COL
        
        ctx.log(Fore.LIGHTCYAN_EX + 'Host: ' + Fore.LIGHTYELLOW_EX + '%s', client_host)
        ctx.log(Fore.LIGHTCYAN_EX + 'Port: ' + Fore.LIGHTYELLOW_EX + '%s',client_port)
        ctx.log(Fore.LIGHTCYAN_EX + 'DB: ' + Fore.LIGHTYELLOW_EX + '%s',client_db)
        ctx.log(Fore.LIGHTCYAN_EX + 'Collection: ' + Fore.LIGHTYELLOW_EX + '%s',client_col)


        client = MongoClient(client_host, client_port)
        db = client[client_db]
        ctx.log('MongoDB connected...')
        
        def getDatetimeFromISO(s):
            d = dateutil.parser.parse(s)
            return d
        
    
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

