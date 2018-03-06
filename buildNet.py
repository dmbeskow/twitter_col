# -*- coding: utf-8 -*-
"""
Created on Mon Mar  5 13:15:13 2018

@author: dmbes
"""
import pandas as pd

From = []
To = []

infile = open('temp.json', 'r')
for line in infile:
    tweet = json.loads(line)
     m = get_mention(tweet, kind = 'id_str')
     if len(m) > 0:
         for mention in m:
             From.append(tweet['id_str'])
             To.append(mention)
     if if tweet['in_reply_to_user_id_str'] != None:
         From.append(tweet['in_reply_to_user_id_str'])
         To.append(tweet['id_str'])
     if 'retweeted_status' in tweet.keys():
         From.append(tweet['user'][name])
         To.append(tweet['retweeted_status']['user'][name])
         
data = {'from': From,
        'to': To}

df = pd.DataFrame(data)
df.to_csv('edgelist.csv', index = False)