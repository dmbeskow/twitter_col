#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 15:35:25 2017

@author: dbeskow
"""

from tweepy import StreamListener
import json, time, sys, gzip, shutil

class SListener(StreamListener):

    def __init__(self, api = None, fprefix = 'streamer'):
        self.api = api or API()
        self.counter = 0
        self.fprefix = fprefix
        self.output  = gzip.open('json/'+fprefix + '.' 
                            + time.strftime('%Y%m%d-%H%M%S') + '.json.gz', 'wt')
        self.delout  = open('delete.txt', 'a')

    def on_data(self, data):

        if  'in_reply_to_status' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print(warning['message'])
            return(False)

    def on_status(self, status):
        self.output.write(status + "\n")

        self.counter += 1
        if self.counter % 1000 == 0:
          print("Total Tweets:", self.counter)

        if self.counter >= 20000:
            print("Starting New File...", time.strftime('%Y%m%d-%H%M%S'))
            self.output.close()
		
            shutil.move(self.output.name, "/usr0/home/dbeskow/Dropbox/Voting Data/Ukraine_GeoFence/"+self.output.name)
		
            self.output = gzip.open('json/'+self.fprefix + '.' 
                               + time.strftime('%Y%m%d-%H%M%S') + '.json.gz', 'wt')
            self.counter = 0

        return

    def on_delete(self, status_id, user_id):
        self.delout.write( str(status_id) + "\n")
        return

    def on_limit(self, track):
        sys.stderr.write(track + "\n")
        return

    def on_error(self, status_code):
        print(str(status_code))
        sys.stderr.write('Error: ' + str(status_code) + "\n")
        return True

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return 
