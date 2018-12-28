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



def main():
    parser=argparse.ArgumentParser(description="Streaming Twitter API based on content")
    parser.add_argument("-keys",help="JSON File with Keys" , type=str, required=True)
    parser.add_argument("-search_terms",help="Text file with search terms (1 per line)" , type=str, required=True)
    parser.add_argument("-tag",help="Tag that will be prepended to file names" , type=str, required=True)
    args=parser.parse_args()
    print(args)
    
    print('...')
    print('Parsing Training Data...')
    
    keys = json.load(args.keys)
    
    consumer_key = keys['consumer_key']
    consumer_secret = keys['consumer_secret']
    access_key = keys['access_token']
    access_secret = keys['access_secret']
    
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    
    api = tweepy.API(auth, wait_on_rate_limit=True,
    				   wait_on_rate_limit_notify=True)
 
    listen = SListener(api, args.tag)
    stream = tweepy.Stream(auth, listen)

    print("Streaming started...")
    
    while True:
        try: 
            stream.filter(track=['@RTUKnews'], async = True, stall_warnings = True)
        except KeyboardInterrupt:
            break
        
        except:
            print("error!")
            time.sleep(15*60)
            continue
        

if __name__ == '__main__':
    main()
