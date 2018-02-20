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
#Extract mentions
def extract_mentions_toCSV(files, name = 'id_str'):
    import json
    import pandas as pd
    files = list(files)
    final = {'user': [], 'mention': [],'tweet_id': [],'date': [],  }
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
                    final[']
            
            
            
    
#%%

import json
infile = open('data.json', 'r')
h = []
m = []
for line in infile:
    tweet = json.loads(line)
    h.append(get_hash(tweet))
    m.append(get_mention(tweet, kind = 'screen_name'))
