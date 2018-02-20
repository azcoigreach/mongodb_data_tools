#!/usr/bin/env python
from pymongo import MongoClient
import pprint
from datetime import datetime, timedelta
import json
import string
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


client = MongoClient('192.168.1.240', 27017)
db = client.twitter_stream

def count_tweets():
    start_time = (datetime.now() - timedelta(hours=24)).strftime('%a %b %d %H:%M:%S +0000 %Y')
    print(start_time)
    end_time = datetime.now().strftime('%a %b %d %H:%M:%S +0000 %Y')
    print(end_time)
    results = db.twitter_query.find({'created_at' : {'$gte' : start_time}}).count()
    return results

def last_tweets():
    results = db.twitter_query.find().limit(5)

    return results
    # for tweet in db_tweets:
    #     # tweet = str(tweet).encode('utf-8')
    #     print('-----')
    #     print('User ' + tweet['user']['screen_name'] + ' said ' + tweet['text'])

def most_tweets():
    results = db.twitter_query.aggregate([
                { "$group" : { "_id" : "$user.screen_name",
                               "count" : { "$sum" : 1 } } },
                { "$sort" : { "count" : -1} } ])
    return results


def project_tweets():
    query = { 'in_reply_to_screen_name' : 'realDonaldTrump' }
    projection = { '_id' : 0, 'user.screen_name' : 1 }
    results = db.twitter_query.find(query, projection)
    
    return results


def operator_tweets():
    query = { 'in_reply_to_screen_name' : 'realDonaldTrump' , 'created_at' : {'$gte' : datetime(2017,12,17), '$lte' : datetime(2017,12,15)}} #
    projection = { '_id' : 0, 'user.screen_name' : 1 , 'created_at' : 1 }
    results = db.twitter_query.find(query, projection)

    return results

def hashtag_tweets():
    query = {'entities.hashtags':{'$ne':[]}}
    projection = {'entities.hashtags.text':1, '_id':0}
    results = db.twitter_query.find(query, projection).count() #remove count to play with arrays

    return results


def match_tweets():
    results = db.twitter_query.aggregate([
                { "$match" : { "user.friends_count" : {"$gt":0},
                                "user.followers_count": {"$gt":0}}},
                {'$project': {'_id':0, 'ratio': {'$divide':['$user.followers_count',
                                                    '$user.friends_count']},
                                'screen_name':'$user.screen_name'}},
                { "$sort" : { "ratio" : -1} },
                {'$limit' :50 } ])
    return results


def unique_hashtags_tweets():
    results = db.twitter_query.aggregate([
                { '$unwind' : '$entities.hashtags'},
                {'$group': {'_id': '$user.screen_name',
                             'unique_hashtags': {
                                 '$addToSet': '$entities.hashtags.text'
                                 }}},
                { "$sort" : { "_id" : -1} } ])
    return results

def popular_hastags_tweets():
    start_time = (datetime.now() - timedelta(hours=24)).strftime('%a %b %d %H:%M:%S +0000 %Y')
    print(start_time)
    end_time = datetime.now().strftime('%a %b %d %H:%M:%S +0000 %Y')
    print(end_time)
    results = db.twitter_query.aggregate([
                { '$match' : {'created_at' : {'$gte' : start_time
                                                , '$lte' : end_time}}},
                { '$unwind' : '$entities.hashtags'},
                {'$group': {'_id': '$entities.hashtags.text',
                             'count' : { '$sum' : 1}}},
                { "$sort" : { 'count' : -1} },
                { '$limit' : 25 } ])
    return results

def popular_hastags_words():
    start_time = (datetime.now() - timedelta(hours=24)).strftime('%a %b %d %H:%M:%S +0000 %Y')
    print(start_time)
    end_time = datetime.now().strftime('%a %b %d %H:%M:%S +0000 %Y')
    print(end_time)
    results = db.twitter_query.aggregate([
                { '$match' : {'created_at' : {'$gte' : start_time
                                                , '$lte' : end_time}}},
                { '$unwind' : '$entities.hashtags'},
                {'$project': {'_id':0, 'entities.hashtags.text': 1, 'count': 1}},
                { '$limit' : 10 } ])
    return results


def unique_user_mentions_tweets():
    results = db.twitter_query.aggregate([
                { '$unwind' : '$entities.user_mentions'},
                { '$group': {'_id': '$user.screen_name',
                             'mset': {
                                 '$addToSet': '$entities.user_mentions.screen_name'
                                 }}},
                { '$unwind' : '$mset'},
                { '$group' : { '_id' : '$_id', 'count' : { '$sum' : 1}}},
                { '$sort' : { 'count' : -1} },
                { '$limit' : 10 } ])
    return results

def earliest_tweet():
    query = { '$query' : {}, '$orderby' : { 'created_at' : 1 } }
    projection = { '_id' : 0, 'user.screen_name' : 1, 'text' : 1 ,'created_at' : 1}
    result = db.twitter_query.find_one(query, projection)
    logger.debug(str(result['user']['screen_name'] + ': '+ result['created_at']).encode('utf-8','ignore'))
    

def sandbox_tweets():
    query = { '$query' : {}, '$orderby' : { '$natural' : -1 } }
    projection = { '_id' : 0, 'user.screen_name' : 1, 'text' : 1 ,'created_at' : 1}
    result = db.twitter_query.find_one(query, projection)
    output = str(result['user']['screen_name'] + ': ' + result['text'] + ': ' + result['created_at']).encode('ascii','ignore')
    print(output)
   

def indexed_hastags_tweets():
    start_time = (datetime.now() - timedelta(hours=24)).strftime('%a %b %d %H:%M:%S +0000 %Y')
    print(start_time)
    end_time = datetime.now().strftime('%a %b %d %H:%M:%S +0000 %Y')
    print(end_time)
    results = db.twitter_query.aggregate([
                { '$match' : {'_id_created_at' : {'$gte' : start_time
                                                , '$lte' : end_time}}},
                {'$group': {'_id': '$_id_hashtag',
                             'count' : { '$sum' : 1}}},
                { "$sort" : { 'count' : -1} },
                { '$limit' : 25 } ])
    return results

def datetime_tweet():
    query = { '$query' : {}, '$orderby' : { '$natural' : -1 } }
    projection = { '_id' : 0, 'user.screen_name' : 1, 'text' : 1 ,'created_at' : 1}
    result = db.twitter_query.find_one(query, projection)
    printable = set(string.printable)
    text_filter = filter(lambda x: x in printable, result['text'])
    
    tweet_date = datetime.strftime(result['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    output = str(result['user']['screen_name'] + ': ' + text_filter + ': ' + tweet_date).encode('ascii', errors='ignore')
                
    logger.debug('Twitter String Output: %s', output)

def top_users():
    start_time = '2018-02-18 07:00'
    end_time = '2018-02-19 07:00'
    start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M')
    end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M')
    limit = 100
    query = db.twitter_query.aggregate([
            {'$match': {'created_at': {'$gte': start_time,
                                       '$lte': end_time}}},
            {'$group': {'_id': '$user.screen_name',
                        'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit' : limit}])
    return query


if __name__ == "__main__":
    results = top_users()
    try: 
        tweets = iter(results)
        for i in tweets:
            pprint.pprint(i)
    except TypeError, te:
        print('***')
        pprint.pprint(results)