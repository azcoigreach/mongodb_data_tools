#!/usr/bin/env python
import click
from data_tools.cli import pass_context
import data_tools.configs.mongodb
from pymongo import MongoClient
from datetime import datetime
import dateutil.parser
import json
from colorama import Fore
from textblob import TextBlob
from unidecode import unidecode

@click.command()
@click.option('--limit', '-l', default=None,
              help='Limit query results')
@click.confirmation_option('-y', help='Execute calc_sentiment without prompt.',
                           prompt='This operation may take a while, are you sure you want to continue?')
@pass_context
def cli(ctx, limit):
    '''Calculate Setiment for the TEXT field in Twitter DB and adds Polarity and Sentiment'''
    ctx.log(Fore.LIGHTMAGENTA_EX + 'calc_sentiment app...')
    ctx.limit = limit
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
               
        if limit is not None:
            ctx.vlog('Limit = %s', ctx.limit)
            results = db[client_col].find({ "sentiment_polarity" : { "$exists" : False } }).limit(int(ctx.limit))

        else:
            ctx.vlog('Limit = None')
            results = db[client_col].find({ "sentiment_polarity" : { "$exists" : False } })
                
        for i in results:
            pointer = i['_id']
            tweet = unidecode(i['text'])
            ctx.log(tweet)
            analysis = TextBlob(tweet)
            sentiment_polarity = analysis.sentiment.polarity
            sentiment_subjectivity = analysis.sentiment.subjectivity
            ctx.log('ID: %s Polarity: %s | Subjectivity %s', pointer, sentiment_polarity, sentiment_subjectivity)
            db[client_col].update(
                                {'_id': pointer}, 
                                {'$set': {'sentiment_polarity': sentiment_polarity, 
                                'sentiment_subjectivity': sentiment_subjectivity}})
    
    except Exception as err:
        ctx.log(err)

