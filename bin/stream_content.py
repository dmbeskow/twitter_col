#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 22 14:22:11 2017

@author: rstudio
"""

from stream_listener import SListener
from http.client import IncompleteRead
import time, tweepy, sys, json
import argparse

## authentication
import tweepy

from pathlib import Path
home = str(Path.home())

with open(home + '/Dropbox/CMU_cloud/creds/keys.json', 'r') as infile:
    keys = json.load(infile)

keys = keys['three']

consumer_key = keys['consumer_key']
consumer_secret = keys['consumer_secret']
access_key = keys['access_token']
access_secret = keys['access_secret']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)

api = tweepy.API(auth, wait_on_rate_limit=True,
				   wait_on_rate_limit_notify=True)

def main():
    parser=argparse.ArgumentParser(description="HW5: Yelp Ratings with Logistic Regression and liblinear SVM")
    parser.add_argument("-train",help="File Path for Training Data" , type=str, required=True)
    parser.add_argument("-test",help="File Path for Testing Data" , type=str, required=True)
    parser.add_argument("-model",help="algorithm: 'log' or 'svm'" ,type=str, required = True)
    parser.add_argument("-features",help="features for model: 'ctf' or 'df' or 'enhanced'" ,type=str, required = True)
    args=parser.parse_args()
    print(args)
    
    print('...')
    print('Parsing Training Data...')
 
    listen = SListener(api, 'RT_Sputnik')
    stream = tweepy.Stream(auth, listen)

    print("Streaming started...")
    
    while True:
        try: 
            stream.filter(track=['@RTUKnews','@RT_America','@RT_Deutsch','@RTenfrancais','@de_sputnik','@RTarabic','@sputnik_ar','@SputnikNewsUS'], async = True, stall_warnings = True)
        except KeyboardInterrupt:
            break
        
        except:
            print("error!")
            time.sleep(15*60)
            continue
        
        #stream.disconnect()

if __name__ == '__main__':
    main()
