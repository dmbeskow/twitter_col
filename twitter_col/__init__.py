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