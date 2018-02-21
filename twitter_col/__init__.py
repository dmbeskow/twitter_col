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
    import json
    import pandas as pd
    import time
    if type(files) != 'list':
       files = [files]
    final = {'date': [],'tweet_id': [],'mention': [], 'user': [] }
    for f in files:
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
    import json
    import pandas as pd
    import time
    if type(files) != 'list':
       files = [files]
    final = {'date': [],'hashtag': [], 'user': [] , 'tweet_id': []}
    for f in files:
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
    import json
    import pandas as pd
    import time
    import itertools
    import progressbar
    if type(files) != 'list':
       files = [files]
    final = {'hash1': [],'hash2': [], 'user': [] , 'tweet_id': [], 'date':  []}
    bar = progressbar.ProgressBar()
    for f in bar(files):
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
    import json
    import pandas as pd
    import time
    if type(files) != 'list':
       files = [files]
    final = {'date': [],'tweet_id': [],'retweeter': [], 'retweeted': [] }
    for f in files:
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
    
def parse_tweet_json(files, file_prefix = 'twitter', to_csv = False):
    """
    This parses 'tweet' json to a pandas dataFrame.
    """
    import pandas as pd
    import json, time
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
          "status_created_at": []
          }
    for f in files:
        infile = open(f, 'r')
        for line in infile:
            t = json.loads(line)
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
        
                if 'possibly_sensitive' in t.keys():
                     data['status_possibly_sensitive'].append(t['possibly_sensitive'])
                else:
                    data['status_possibly_sensitive'].append(False)
        
        
                if 'retweeted_status' in t.keys():
                    data['status_isretweet'].append(True)
                else:
                    data['status_isretweet'].append(False)
        
    df = pd.DataFrame(data)
    
    df.to_csv(file_prefix + '_parsedTweetData_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8')

#%%    
# For testing
#
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