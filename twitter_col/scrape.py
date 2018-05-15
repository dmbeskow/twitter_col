# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


def rest_scrape(api, searchQuery = ['#datascience'], prefix = 'twitter',sinceId = None, max_id = -1 ):
    """ conduct REST Scrape for hashtags or terms
	This code will conduct a scrape the last 7 days for the terms in the
	searchQuery list

	The code will create files for each term in the working directory.  The prefix string
	is added to the front of each file name to distinguish this scrape.
    """
    import tweepy
    import time
    import sys
    import json
    
    if (not api):
	    print ("Can't Authenticate")
	    sys.exit(-1)
        
    maxTweets = 10000000 # Some arbitrary large number
    tweetsPerQry = 100  # this is the max the API permits
    
    count = 0
    for query in searchQuery:
	    sinceId = None
	    max_id = -1 # 965975379323379712
	    count =+1
	    fName = prefix + str(count) + '_'+time.strftime("%Y-%m-%d")+'.json'
	    tweetCount = 0
	    print("Downloading max {0} tweets".format(maxTweets))
	    with open(fName, 'w') as f:
	        while tweetCount < maxTweets:
	            try:
	                if (max_id <= 0):
	                    if (not sinceId):
	                        new_tweets = api.search(q=query, count=tweetsPerQry)
	                    else:
	                        new_tweets = api.search(q=query, count=tweetsPerQry,
	                                                since_id=sinceId)
	                else:
	                    if (not sinceId):
	                        new_tweets = api.search(q=query, count=tweetsPerQry,
	                                                max_id=str(max_id - 1))
	                    else:
	                        new_tweets = api.search(q=query, count=tweetsPerQry,
	                                                max_id=str(max_id - 1),
	                                                since_id=sinceId)
	                if not new_tweets:
	                    print("No more tweets found")
	                    break
	                for tweet in new_tweets:
	                    out = json.dumps(tweet._json)
	                    f.write(out + '\n')
	                tweetCount += len(new_tweets)
	                print("Downloaded {0} tweets".format(tweetCount))
	                max_id = new_tweets[-1].id
	            except tweepy.TweepError as e:
	                # Just exit if any error
	                print("some error : " + str(e))
	                break

	    print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))
