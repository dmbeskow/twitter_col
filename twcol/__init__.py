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
        df.to_csv(file_prefix + '_mentions_' + time.strftime('%Y%m%d-%H%M%S'), 
                  index = False , columns = ['user', 'mention', 'tweet_id','date'])
    else:
        return(df)
    
    
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
        df.to_csv(file_prefix + '_mentions_' + time.strftime('%Y%m%d-%H%M%S'), 
                  index = False , columns = ['user', 'mention', 'tweet_id','date'])
    else:
        return(df)
            
    
#%%

import json
infile = open('data.json', 'r')
h = []
m = []
for line in infile:
    tweet = json.loads(line)
    h.append(get_hash(tweet))
    m.append(get_mention(tweet, kind = 'screen_name'))

extract_mentions('data.json', name = 'screen_name')
