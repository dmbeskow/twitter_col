def get_hash(tweet):
    """
    Returns list of hashtags in a tweet.  If no hashtags, 
    it returns an empty list.
    
    """
    ht = []
    for h in tweet['entities']['hashtags']:
            ht.append(h['text'])
    return (ht)


#%%
def get_mention(tweet, kind = 'id_str'):
    """
    Returns list of mentions in a tweet.  If no hashtags, 
    it returns an empty list.
    """
    men = []
    if len(tweet['entities']['user_mentions']) > 0:
        for m in tweet['entities']['user_mentions']:
            men.append(m[kind])
    return(men)
    
#%%
def extract_mentions(files, file_prefix = 'twitter', name = 'id_str', to_csv = True):
    """
   Creates mention edgelist.  Can return data.frame or write to csv.  
   
    """
    import json, gzip, io, time
    import pandas as pd
    if not isinstance(files, list):
       files = [files]
    final = {'date': [],'tweet_id': [],'mention': [], 'user': [] }
    for f in files:
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        for line in infile:
            tweet = json.loads(line)
            m = get_mention(tweet, kind = name)
            if len(m) > 0:
                for mention in m:
                    final['user'].append(tweet['user'][name])
                    final['mention'].append(mention)
                    final['tweet_id'].append(tweet['id_str'])
                    final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_mentions_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8', columns = ['user', 'mention', 'tweet_id','date'])
    else:
        return(df[['user', 'mention', 'tweet_id','date']])
    
    
#%%
def extract_hashtags(files, file_prefix = 'twitter', name = 'id_str', 
                     to_csv = True):
    """
   Creates hashtag edgelist (either user to hashtag OR comention).  
   Can return data.frame or write to csv.  
   
    """
    import json, io, gzip, time
    import pandas as pd
    if type(files) != 'list':
       files = [files]
    final = {'date': [],'hashtag': [], 'user': [] , 'tweet_id': []}
    for f in files:
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        for line in infile:
            tweet = json.loads(line)
            h = get_hash(tweet)
            if len(h) > 0:
                for hashtag in h:
                    final['user'].append(tweet['user'][name])
                    final['hashtag'].append(hashtag)
                    final['tweet_id'].append(tweet['id_str'])
                    final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_hashtags_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False ,  encoding = 'utf-8', columns = ['user', 'hashtag', 'tweet_id','date'])
    else:
        return(df[['user', 'hashtag', 'tweet_id','date']])
        
#%%
def extract_hash_comention(files, file_prefix = 'twitter', name = 'id_str', 
                     to_csv = True):
    """
   Creates hashtag edgelist (either user to hashtag OR comention).  
   Can return data.frame or write to csv.  
   
    """
    import json, io, gzip, time
    import pandas as pd
    import itertools
    import progressbar
    if type(files) != 'list':
       files = [files]
    final = {'hash1': [],'hash2': [], 'user': [] , 'tweet_id': [], 'date':  []}
    bar = progressbar.ProgressBar()
    for f in bar(files):
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        for line in infile:
            tweet = json.loads(line)
            h = get_hash(tweet)
            if len(h) > 1:
                combo = list(itertools.combinations(h, 2))
                for pair in combo:
                    final['user'].append(tweet['user'][name])
                    final['hash1'].append(pair[0])
                    final['hash2'].append(pair[1])
                    final['tweet_id'].append(tweet['id_str'])
                    final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_hashComention_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8', columns = ['user', 'hash1', 'hash2', 'tweet_id','date'])
    else:
        return(df[['user', 'hash1', 'hash2', 'tweet_id','date']])
#%%           
def extract_retweet_network(files, file_prefix = 'twitter', name = 'id_str', to_csv = True):
    """
   Creates retweet edgelist.  Can return data.frame or write to csv.  
   
    """
    import pandas as pd
    import time, json, io, gzip
    if type(files) != 'list':
       files = [files]
    final = {'date': [],'tweet_id': [],'retweeter': [], 'retweeted': [] }
    for f in files:
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        for line in infile:
            tweet = json.loads(line)
            if 'retweeted_status' in tweet.keys():
                final['retweeter'].append(tweet['user'][name])
                final['retweeted'].append(tweet['retweeted_status']['user'][name])
                final['tweet_id'].append(tweet['id_str'])
                final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_retweetNetwork_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8', columns = ['retweeter', 'retweeted', 'tweet_id','date'])
    else:
        return(df[['retweeter', 'retweeted', 'tweet_id','date']])
#%%     
def extract_reply_network(files, file_prefix = 'twitter', name = 'id_str', to_csv = True):
    """
   Creates reply edgelist.  Can return data.frame or write to csv.  
   
    """
    import pandas as pd
    import time, json, io, gzip
    if type(files) != 'list':
       files = [files]
    final = {'date': [],'tweet_id': [],'reply_from': [], 'reply_to': [] }
    for f in files:
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        for line in infile:
            tweet = json.loads(line)
            if tweet['in_reply_to_user_id_str'] != None:
                final['reply_from'].append(tweet['user'][name])
                final['reply_to'].append(tweet['iin_reply_to_user_id_str'])
                final['tweet_id'].append(tweet['id_str'])
                final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_replyNetwork_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8', columns = ['date','tweet_id', 'reply_from', 'reply_to'])
    else:
        return(df[['date','tweet_id', 'reply_from', 'reply_to']])
#%%
def convert_dates(date_list):
    """
    This function converts twitter dates to python date objects
    """
    import progressbar
    import dateutil
    dates2 = []
    bar = progressbar.ProgressBar()
    for d in bar(date_list):
        dates2.append(dateutil.parser.parse(d))  
    return(dates2)
    
#%%
def get_all_network_files( files, file_prefix = 'twitter', name = 'id_str'):
    """
    This is a single command to get the hashtag network, comention network, and 
    retweet network
    """
    print("Getting hashtags....")
    extract_hashtags(files, file_prefix=file_prefix, name=name, to_csv=True)
    print("Getting hashtag comention network....")
    extract_hash_comention(files, file_prefix=file_prefix, name=name, to_csv=True)
    print("Getting mention network....")
    extract_mentions(files, file_prefix=file_prefix, name=name, to_csv=True)
    print("Getting retweet network....")
    extract_retweet_network(files, file_prefix=file_prefix, name=name, to_csv=True)
#%%
    
def parse_twitter_json(files, file_prefix = 'twitter', to_csv = False):
    """
    This parses 'tweet' json to a pandas dataFrame. 'name' should be either
    'id_str' or 'screen_name'.  This will choose which object is selected for
    reply and retweet id.
    """
    import pandas as pd
    import json, time, io, gzip
    
    if not isinstance(files, list):
       files = [files]
    data = { "id_str" : [],
        "name" : [],
        "screen_name" : [],
        "location" : [],
        "url" : [],
        "description" : [],
        "protected" : [],
        "verified" : [],
        "followers_count" : [],
        "friends_count" : [],
        "listed_count" : [],
        "favourites_count" : [],
        "statuses_count" : [],
        "created_at" : [],
        "utc_offset" : [],
        "time_zone" : [],
        "geo_enabled" : [],
        "lang" : [],
        "contributors_enabled" : [],
        "is_translator" : [],
        "status_text" : [],
        "status_source" : [],
        "status_coordinates" : [],
        "status_possibly_sensitive" : [],
         "status_isretweet" : [],
          "status_lang" : [],
          "status_id" : [],
          "status_created_at": [],
          "retweet_status_id": [],
          "reply_to_user_id": [],
          "reply_to_status_id": []
          }
    for f in files:
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        for line in infile:
            if line != '\n':
                t = json.loads(line)
                if 'status' in t.keys():
                    temp = t['status']
                    getRid = t.pop('status', 'Entry not found')
                    temp['user'] = t
                    t = temp
                if 'user' in t.keys():
                    data['id_str'].append(t['user']['id_str'])
                    data['name'].append(t['user']['name'])
                    data['screen_name'].append(t['user']['screen_name'])
                    data['location'].append(t['user']['location'])
                    data['url'].append(t['user']['url'])
                    data['description'].append(t['user']['description'])
                    data['protected'].append(t['user']['protected'])
                    data['verified'].append(t['user']['verified'])
                    data['followers_count'].append(t['user']['followers_count'])
                    data['friends_count'].append(t['user']['friends_count'])
                    data['listed_count'].append(t['user']['listed_count'])
                    data['favourites_count'].append(t['user']['favourites_count'])
                    data['statuses_count'].append(t['user']['statuses_count'])
                    data['created_at'].append(t['user']['created_at'])
                    data['utc_offset'].append(t['user']['utc_offset'])
                    data['time_zone'].append(t['user']['time_zone'])
                    data['geo_enabled'].append(t['user']['geo_enabled'])
                    data['lang'].append(t['user']['lang'])
                    data['contributors_enabled'].append(t['user']['contributors_enabled'])
                    data['is_translator'].append(t['user']['is_translator'])
                    data['status_text'].append(t['text'])
                    data['status_source'].append(t['source'])
                    data['status_coordinates'].append(t['coordinates'])
                    data['status_lang'].append(t['lang'])
                    data['status_id'].append(t['id'])
                    data['status_created_at'].append(t['created_at'])
                    data['reply_to_user_id'].append(t['in_reply_to_user_id_str'])
                    data['reply_to_status_id'].append(t['in_reply_to_status_id_str'])
            
                    if 'possibly_sensitive' in t.keys():
                         data['status_possibly_sensitive'].append(t['possibly_sensitive'])
                    else:
                        data['status_possibly_sensitive'].append(False)
            
            
                    if 'retweeted_status' in t.keys():
                        data['retweet_status_id'].append(t['retweeted_status']['id_str'])
                        data['status_isretweet'].append(True)
                    else:
                        data['status_isretweet'].append(False)
                        data['retweet_status_id'].append(None)
        
    df = pd.DataFrame(data)
    if to_csv:
        df.to_csv(file_prefix + '_parsedTweetData_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8')
    else:
        return(df)
        

#%%
        
def get_edgelist(file, mentions = True, replies = True, retweets = True, to_csv = True):
    import pandas as pd
    import json
    import gzip
    import progressbar
    import io

    From = []
    To = []
    
    if '.gz' in file:
        infile = io.TextIOWrapper(gzip.open(file, 'r'))
    else:
        infile = open(file, 'r')
    bar = progressbar.ProgressBar()
    for line in bar(infile):
        if line == '\n':
            continue
        tweet = json.loads(line)
        m = get_mention(tweet, kind = 'id_str')
        if len(m) > 0:
             for mention in m:
                 From.append(tweet['id_str'])
                 To.append(mention)
        if tweet['in_reply_to_user_id_str'] != None:
             From.append(tweet['in_reply_to_user_id_str'])
             To.append(tweet['id_str'])
        if 'retweeted_status' in tweet.keys():
             From.append(tweet['user']['id_str'])
             To.append(tweet['retweeted_status']['user']['id_str'])
    infile.close()        
    data = {'from': From,
            'to': To}
    df = pd.DataFrame(data)
    df.to_csv(file.rstrip('.json')+'_edgelist.csv', index = False)
#%%

def fetch_profiles(api, screen_names = [], ids = []):
    """
    I copied this from:
        https://github.com/unitedstates/congress-legislators/blob/master/scripts/social/twitter.py
        
    A wrapper method around tweepy.API.lookup_users that handles the batch lookup of
      screen_names. Assuming number of screen_names < 10000, this should not typically
      run afoul of API limits (i.e. it's a good enough hack for now)
    `api` is a tweepy.API handle
    `screen_names` is a list of twitter screen names
    Returns: a list of dicts representing Twitter profiles
    """
    import tweepy
    TWITTER_PROFILE_BATCH_SIZE = 100
    from math import ceil
    
    profiles = []
    key, lookups = ['user_ids', ids] if ids else ['screen_names', screen_names]
    for batch_idx in range(ceil(len(lookups) / TWITTER_PROFILE_BATCH_SIZE)):
        offset = batch_idx * TWITTER_PROFILE_BATCH_SIZE
        # break lookups list into batches of TWITTER_PROFILE_BATCH_SIZE
        batch = lookups[offset:(offset + TWITTER_PROFILE_BATCH_SIZE)]
        try:
            for user in api.lookup_users(**{key: batch}):
                profiles.append(user._json)
        # catch situation in which none of the names in the batch are found
        # or else Tweepy will error out
        except tweepy.error.TweepError as e:
            if e.response.status_code == 404:
                pass
            else: # some other error, raise the exception
                raise e
        print("Batch", batch_idx)
    return profiles 
#%%
#def parse_user_json(files, file_prefix = 'twitter', to_csv = False):
#    """
#    This parses 'tweet' json to a pandas dataFrame.
#    """
#    import pandas as pd
#    import json, time
#    
#    files = [files]
#    data = { "id_str" : [],
#            "name" : [],
#            "screen_name" : [],
#            "location" : [],
#            "url" : [],
#            "description" : [],
#            "translator_type" : [],
#            "protected" : [],
#            "verified" : [],
#            "followers_count" : [],
#            "friends_count" : [],
#            "listed_count" : [],
#            "favourites_count" : [],
#            "statuses_count" : [],
#            "created_at" : [],
#            "utc_offset" : [],
#            "time_zone" : [],
#            "geo_enabled" : [],
#            "lang" : [],
#            "contributors_enabled" : [],
#            "is_translator" : [],
#            "profile_background_image_url" : [],
#            "profile_background_tile" : [],
#            "profile_use_background_image" : [],
#            "profile_image_url" : [],
#            "profile_image_url_https" : [],
#            "status_text" : [],
#            "status_source" : [],
#            "status_coordinates" : [],
#            "status_possibly_sensitive" : [],
#             "status_isretweet" : [],
#             "status_lang" : []
#    }
#    for f in files:
#        infile = open(f, 'r')
#        for line in infile:
#            if line != '\n' :
#                t = json.loads(line)
#                if 'status' in t.keys():
#                    data['id_str'].append(t['id_str'])
#                    data['name'].append(t['name'])
#                    data['screen_name'].append(t['screen_name'])
#                    data['location'].append(t['location'])
#                    data['url'].append(t['url'])
#                    data['description'].append(t['description'])
#                    data['translator_type'].append(t['translator_type'])
#                    data['protected'].append(t['protected'])
#                    data['verified'].append(t['verified'])
#                    data['followers_count'].append(t['followers_count'])
#                    data['friends_count'].append(t['friends_count'])
#                    data['listed_count'].append(t['listed_count'])
#                    data['favourites_count'].append(t['favourites_count'])
#                    data['statuses_count'].append(t['statuses_count'])
#                    data['created_at'].append(t['created_at'])
#                    data['utc_offset'].append(t['utc_offset'])
#                    data['time_zone'].append(t['time_zone'])
#                    data['geo_enabled'].append(t['geo_enabled'])
#                    data['lang'].append(t['lang'])
#                    data['contributors_enabled'].append(t['contributors_enabled'])
#                    data['is_translator'].append(t['is_translator'])
#                    data['profile_background_image_url'].append(t['profile_background_image_url'])
#                    data['profile_background_tile'].append(t['profile_background_tile'])
#                    data['profile_use_background_image'].append(t['profile_use_background_image'])
#                    data['profile_image_url'].append(t['profile_image_url'])
#                    data['profile_image_url_https'].append(t['profile_image_url_https'])
#                    data['status_text'].append(t['status']['text'])
#                    data['status_source'].append(t['status']['source'])
#                    data['status_coordinates'].append(t['status']['coordinates'])
#                    data['status_lang'].append(t['status']['lang'])
#                    
#                    if 'possibly_sensitive' in t['status'].keys():
#                         data['status_possibly_sensitive'].append(t['status']['possibly_sensitive'])
#                    else: 
#                        data['status_possibly_sensitive'].append(False)
#                       
#                    
#                    if 'retweeted_status' in t['status'].keys():
#                        data['status_isretweet'].append(True)
#                    else: 
#                        data['status_isretweet'].append(False)
#        
#    df = pd.DataFrame(data)
#    if to_csv:
#        df.to_csv(file_prefix + '_parsedUserData_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
#                  index = False , encoding = 'utf-8')
#    else:
#        return(df)
#For testing

#import json
#infile = open('data.json', 'r')
#h = []
#m = []
#for line in infile:
#    tweet = json.loads(line)
#    h.append(get_hash(tweet))
#    m.append(get_mention(tweet, kind = 'screen_name'))
#
#extract_mentions('data.json', name = 'screen_name')
#
#extract_hashtags('data.json', name = 'screen_name')
#
#df = extract_hash_comention('data.json', to_csv = False)
#df.head()
#
#df = extract_retweet_network('data.json', to_csv = False)
#df.head()
#
#df = parse_tweet_json('data.json')
