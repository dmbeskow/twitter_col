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
def get_urls(tweet):
    """
    Returns list of expanded urls in a tweet.  If no urls, returns an
    empty list.
    """
    url = []
    if len(tweet['entities']['urls']) > 0:
        for u in tweet['entities']['urls']:
            url.append(u['expanded_url'])
    return(url)
    
#%%
def get_reply_conversation(files, status_ids):
    """
    Recursively extracts replies and replies to replies in order to pull all
    replies that are connected to a given status_id(s)
    """
    import json, gzip, io
    import pandas as pd
    import progressbar
    if not isinstance(files, list):
       files = [files]
    data = {'status_id': [],
            'reply_to_status_id':[]}

    for f in files:
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        bar = progressbar.ProgressBar()
        for line in bar(infile):
            tweet = json.loads(line)
            data['status_id'].append(tweet['id_str'])
            data['reply_to_status_id'].append(tweet['in_reply_to_status_id_str'])
    
    df = pd.DataFrame(data, dtype = str)
    
    conversation = []
    reply = df[df['reply_to_status_id'].isin(status_ids)]
    conversation.extend(reply['status_id'].tolist())
    count = 1
    while len(reply.index) > 0:
        reply = df[df['reply_to_status_id'].isin(reply['status_id'])]
        conversation.extend(reply['status_id'].tolist())
        count += 1
        
    print('Total of',count,'levels in the conversation')
        
    return(conversation)
        
    
    
#%%
def extract_mentions(files, file_prefix = 'twitter', name = 'id_str', to_csv = True):
    """
   Creates mention edgelist.  Can return data.frame or write to csv.  
   
    """
    import json, gzip, io, time
    import pandas as pd
    if not isinstance(files, list):
       files = [files]
    final = {'date': [],'status_id': [],'mention': [], 'user': [] }
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
                    final['status_id'].append(tweet['id_str'])
                    final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_mentions_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8', columns = ['user', 'mention', 'status_id','date'])
    else:
        return(df[['user', 'mention', 'status_id','date']])
    
    
#%%
def extract_hashtags(files, file_prefix = 'twitter', name = 'id_str', 
                     to_csv = True):
    """
   Creates hashtag edgelist (either user to hashtag OR comention).  
   Can return data.frame or write to csv.  
   
    """
    import json, io, gzip, time
    import pandas as pd
    if not isinstance(files, list):
       files = [files]
    final = {'date': [],'hashtag': [], 'user': [] , 'status_id': []}
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
                    final['status_id'].append(tweet['id_str'])
                    final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_hashtags_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False ,  encoding = 'utf-8', columns = ['user', 'hashtag', 'status_id','date'])
    else:
        return(df[['user', 'hashtag', 'status_id','date']])
#%%     
def extract_urls(files, file_prefix = 'twitter',  to_csv = True, name = 'id_str'):
    """
   Creates  csv containing all URLS in a set of tweets 
   
    """
    import json, io, gzip, time
    import pandas as pd
    if not isinstance(files, list):
       files = [files]
    final = {'date': [],'url': [], 'user': [] , 'status_id': []}
    for f in files:
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        for line in infile:
            try:
                tweet = json.loads(line)
            except:
                continue
            u = get_urls(tweet)
            if len(u) > 0:
                for url in u:
                    final['user'].append(tweet['user'][name])
                    final['url'].append(url)
                    final['status_id'].append(tweet['id_str'])
                    final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_urls_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False ,  encoding = 'utf-8', columns = ['user', 'url', 'status_id','date'])
    else:
        return(df[['user', 'url', 'status_id','date']])
#%%     
def extract_media(files,   file_prefix = 'twitter',to_csv = True, name = 'id_str'):
    """
   Creates  csv containing all URLS in a set of tweets 
   
    """
    import json, io, gzip, time
    import pandas as pd
    import progressbar
    if type(files) != 'list':
       files = [files]
    final = {'date': [],'type':[] , 'display_url': [], 'expanded_url': [], 'media_url': [], 'media_url_https': [],  'user': [] , 'status_id': []}
    for f in files:
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        bar =  progressbar.ProgressBar()
        for line in bar(infile):
            tweet = json.loads(line)
            if 'extended_entities' in tweet.keys():
                if 'media' in tweet['extended_entities']:
                    for m in tweet['extended_entities']['media']:
                        final['user'].append(tweet['user'][name])
                        final['type'].append(m['type'])
                        final['display_url'].append(m['display_url'])
                        final['expanded_url'].append(m['expanded_url'])
                        final['media_url'].append(m['media_url'])
                        final['media_url_https'].append(m['media_url_https'])
                        final['status_id'].append(tweet['id_str'])
                        final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_media_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False ,  encoding = 'utf-8', columns = ['user', 'type','display_url',
                  'expanded_url','media_url','media_url_https','user','status_id','date'])
    else:
        return(df[['user', 'type','display_url',
                  'expanded_url','media_url','media_url_https','user','status_id','date']])        
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
    final = {'hash1': [],'hash2': [], 'user': [] , 'status_id': [], 'date':  []}
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
                    final['status_id'].append(tweet['id_str'])
                    final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_hashComention_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8', columns = ['user', 'hash1', 'hash2', 'status_id','date'])
    else:
        return(df[['user', 'hash1', 'hash2', 'status_id','date']])
#%%           
def extract_retweet_network(files, file_prefix = 'twitter', name = 'id_str', to_csv = True):
    """
   Creates retweet edgelist.  Can return data.frame or write to csv.  
   
    """
    import pandas as pd
    import time, json, io, gzip
    if type(files) != 'list':
       files = [files]
    final = {'date': [],'status_id': [],'retweeter': [], 'retweeted': [] }
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
                final['status_id'].append(tweet['id_str'])
                final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_retweetNetwork_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8', columns = ['retweeter', 'retweeted', 'status_id','date'])
    else:
        return(df[['retweeter', 'retweeted', 'status_id','date']])
#%%     
def extract_reply_network(files, file_prefix = 'twitter', name = 'id_str', to_csv = True):
    """
   Creates reply edgelist.  Can return data.frame or write to csv.  
   
    """
    import pandas as pd
    import time, json, io, gzip
    if type(files) != 'list':
       files = [files]
    final = {'date': [],'status_id': [],'reply_from': [], 'reply_to': [] }
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
                final['status_id'].append(tweet['id_str'])
                final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_replyNetwork_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8', columns = ['date','status_id', 'reply_from', 'reply_to'])
    else:
        return(df[['date','status_id', 'reply_from', 'reply_to']])
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
        try:
            dates2.append(dateutil.parser.parse(d))  
        except:
            print("Error with date:",d)
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
    
def parse_twitter_json(files, file_prefix = 'twitter', to_csv = False, sentiment = False):
    """
    This parses 'tweet' json to a pandas dataFrame. 'name' should be either
    'id_str' or 'screen_name'.  This will choose which object is selected for
    reply and retweet id.
    """
    import pandas as pd
    from textblob import TextBlob
    import json, time, io, gzip
    import progressbar
    
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
        "lat" : [],
        "lon" : [],
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
        bar = progressbar.ProgressBar()
        for line in bar(infile):
            if line != '\n':
                try:
                    t = json.loads(line)
                except:
                    continue
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
                    data['status_source'].append(t['source'])
                    data['status_lang'].append(t['lang'])
                    data['status_id'].append(t['id'])
                    data['status_created_at'].append(t['created_at'])
                    data['reply_to_user_id'].append(t['in_reply_to_user_id_str'])
                    data['reply_to_status_id'].append(t['in_reply_to_status_id_str'])
                    
                    if 'extended_tweet' in t.keys():
                        if 'full_text' in t['extended_tweet']:
                            data['status_text'].append(t['extended_tweet']['full_text'])
                    elif 'full_text' in t.keys():
                        data['status_text'].append(t['full_text'])
                    else:
                        data['status_text'].append(t['text'])
            
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
                        
                    coords = t["coordinates"]
                    if coords is not None:
                        data['lon'].append(coords["coordinates"][0])
                        data['lat'].append(coords["coordinates"][1])
                    else:
                        data['lon'].append(None)
                        data['lat'].append(None)

        
    df = pd.DataFrame(data, dtype = str)
    
    if sentiment:
        sent = []
        for t in df['status_text'].tolist():
            text = TextBlob(t)
            sent.append(text.sentiment.polarity)
        df['sentiment_score'] = sent
        df['sentiment_label'] = df['sentiment_score'].apply(get_sensitivity)
            
    if to_csv:
        df.to_csv(file_prefix + '_parsedTweetData_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8')
    else:
        return(df)
        
#%%
    
def parse_twitter_list(List, file_prefix = 'twitter', to_csv = False, sentiment = False):
    """
    This parses 'tweet' json to a pandas dataFrame. 'name' should be either
    'id_str' or 'screen_name'.  This will choose which object is selected for
    reply and retweet id.
    """
    import pandas as pd
    from textblob import TextBlob
    import json, time, io, gzip
    import progressbar

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
        "lat" : [],
        "lon" : [],
        "status_possibly_sensitive" : [],
         "status_isretweet" : [],
          "status_lang" : [],
          "status_id" : [],
          "status_created_at": [],
          "retweet_status_id": [],
          "reply_to_user_id": [],
          "reply_to_status_id": []
          }

    bar = progressbar.ProgressBar()
    for t in bar(List):
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
            data['status_source'].append(t['source'])
            data['status_lang'].append(t['lang'])
            data['status_id'].append(t['id'])
            data['status_created_at'].append(t['created_at'])
            data['reply_to_user_id'].append(t['in_reply_to_user_id_str'])
            data['reply_to_status_id'].append(t['in_reply_to_status_id_str'])
            
            if 'extended_tweet' in t.keys():
                if 'full_text' in t['extended_tweet']:
                    data['status_text'].append(t['extended_tweet']['full_text'])
            elif 'full_text' in t.keys():
                data['status_text'].append(t['full_text'])
            else:
                data['status_text'].append(t['text'])
    
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
                
            coords = t["coordinates"]
            if coords is not None:
                data['lon'].append(coords["coordinates"][0])
                data['lat'].append(coords["coordinates"][1])
            else:
                data['lon'].append(None)
                data['lat'].append(None)

        
    df = pd.DataFrame(data, dtype = str)
    
    if sentiment:
        sent = []
        for t in df['status_text'].tolist():
            text = TextBlob(t)
            sent.append(text.sentiment.polarity)
        df['sentiment_score'] = sent
        df['sentiment_label'] = df['sentiment_score'].apply(get_sensitivity)
            
    if to_csv:
        df.to_csv(file_prefix + '_parsedTweetData_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8')
    else:
        return(df)
        

#%%

        
def get_edgelist_file(file, mentions = True, replies = True, retweets = True, 
                 urls = False, hashtags = False, to_csv = True):
    ''' 
    Builds an agent x agent edgelist of a Tweet json (normal or gzipped)
    '''
    import pandas as pd
    import json
    import gzip
    import progressbar
    import io

    From = []
    To = []
    Type = []
    Time = []
    ID = []
    
    if '.gz' in file:
        infile = io.TextIOWrapper(gzip.open(file, 'r'))
    else:
        infile = open(file, 'r')
    bar = progressbar.ProgressBar()
    for line in bar(infile):
        if line == '\n':
            continue
        tweet = json.loads(line)
        dateTime = tweet['created_at']
        m = get_mention(tweet, kind = 'id_str')
        if len(m) > 0 and mentions:
             for mention in m:
                 From.append(tweet['user']['id_str'])
                 To.append(mention)
                 Type.append('mention')
                 Time.append(dateTime)
                 ID.append(tweet['id_str'])
        if tweet['in_reply_to_user_id_str'] != None and replies:
             To.append(tweet['in_reply_to_user_id_str'])
             From.append(tweet['user']['id_str'])
             Type.append('reply')
             Time.append(dateTime)
             ID.append(tweet['id_str'])
        if 'retweeted_status' in tweet.keys() and retweets:
             From.append(tweet['user']['id_str'])
             To.append(tweet['retweeted_status']['user']['id_str'])
             Type.append('retweet')
             Time.append(dateTime)
             ID.append(tweet['id_str'])
        u = get_urls(tweet)
        if len(u) > 0 and urls:
             for url in u:
                 From.append(tweet['user']['id_str'])
                 To.append(url)
                 Type.append('url')
                 Time.append(dateTime)
                 ID.append(tweet['id_str'])
        h = get_hash(tweet)
        if len(h) > 0 and hashtags:
             for Hash in h:
                 From.append(tweet['user']['id_str'])
                 To.append(Hash)
                 Type.append('hashtag')
                 Time.append(dateTime)
                 ID.append(tweet['id_str'])

    infile.close()        
    data = {'from': From,
            'to': To,
            'type': Type,
            'created_at': Time,
            'status_id': ID}
    
    df = pd.DataFrame(data)
    if to_csv:
        df.to_csv(file.rstrip('.json')+'_edgelist.csv', index = False)
    else:
        return(df)
#%%
        
def get_edgelist_from_list(tweet_list, mentions = True, replies = True, retweets = True, 
                 urls = False, hashtags = False, to_csv = True):
    ''' 
    Builds an agent x agent edgelist of a tweet list.
    '''
    import pandas as pd
    import progressbar

    From = []
    To = []
    Type = []
    Time = []
    ID = []
    
    bar = progressbar.ProgressBar()
    for tweet in bar(tweet_list):
        dateTime = tweet['created_at']
        m = get_mention(tweet, kind = 'id_str')
        if len(m) > 0 and mentions:
             for mention in m:
                 From.append(tweet['user']['id_str'])
                 To.append(mention)
                 Type.append('mention')
                 Time.append(dateTime)
                 ID.append(tweet['id_str'])
        if tweet['in_reply_to_user_id_str'] != None and replies:
             To.append(tweet['in_reply_to_user_id_str'])
             From.append(tweet['user']['id_str'])
             Type.append('reply')
             Time.append(dateTime)
             ID.append(tweet['id_str'])
        if 'retweeted_status' in tweet.keys() and retweets:
             From.append(tweet['user']['id_str'])
             To.append(tweet['retweeted_status']['user']['id_str'])
             Type.append('retweet')
             Time.append(dateTime)
             ID.append(tweet['id_str'])
        u = get_urls(tweet)
        if len(u) > 0 and urls:
             for url in u:
                 From.append(tweet['user']['id_str'])
                 To.append(url)
                 Type.append('url')
                 Time.append(dateTime)
                 ID.append(tweet['id_str'])
        h = get_hash(tweet)
        if len(h) > 0 and hashtags:
             for Hash in h:
                 From.append(tweet['user']['id_str'])
                 To.append(Hash)
                 Type.append('hashtag')
                 Time.append(dateTime)
                 ID.append(tweet['id_str'])
      
    data = {'from': From,
            'to': To,
            'type': Type,
            'created_at': Time,
            'status_id': ID}
    
    df = pd.DataFrame(data)
#    if to_csv:
#        df.to_csv(file.rstrip('.json')+'_edgelist.csv', index = False)
#    else:
    return(df)
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
def get_sensitivity(value):
    if value < 0:
        return("negative")
    if value == 0:
        return('neutral')
    else:
        return('positive')
#%%
def rehydrate(api,  ids = []):
    """
    I copied this from:
        https://github.com/unitedstates/congress-legislators/blob/master/scripts/social/twitter.py
        
    A wrapper method around tweepy.API.lookup_users that handles the batch lookup of
      Tweet IDs. 
    Returns: a list of dicts representing Twitter profiles
    """
    import tweepy
    TWITTER_PROFILE_BATCH_SIZE = 100
    from math import ceil
    
    profiles = []
    key, lookups = ['id', ids] 
    for batch_idx in range(ceil(len(lookups) / TWITTER_PROFILE_BATCH_SIZE)):
        offset = batch_idx * TWITTER_PROFILE_BATCH_SIZE
        # break lookups list into batches of TWITTER_PROFILE_BATCH_SIZE
        batch = lookups[offset:(offset + TWITTER_PROFILE_BATCH_SIZE)]
        try:
            for user in api.statuses_lookup(**{key: batch}, tweet_mode='extended'):
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
def check_inactive(api, uids):
    """ Check inactive account, one by one.
    Parameters
    ---------------
    uids : list
        A list of inactive account

    Returns
    ----------
        list of tuple (uid, reason). Where `uid` is the account id,
        and `reason` is a string.
    """
    import progressbar
    import tweepy
    final = []
    bar = progressbar.ProgressBar()
    for uid in bar(uids):
        try:
            u = api.get_user(user_id=uid)
            final.append( (uid,"Status OK"))
        except tweepy.error.TweepError as e:
            final.append((uid, e.reason))
            
    return(final)
    
#%%
def dedupe_twitter(list_of_tweets):
    import progressbar
    seen = {}
    final = []
    bar = progressbar.ProgressBar()
    for tweet in bar(list_of_tweets):
        try:
            id = tweet["id"]
            if id not in seen:
                seen[id] = True
                final.append(tweet)
        except:
            continue
        
    return(final)
    
#%%
def extract_gender(file, to_csv = False):
    import pandas as pd
    import json, io, gzip
    import gender_guesser.detector as gender
    import progressbar
    
    data = { "status_id" : [],
        "name" : [],
        "screen_name" : [],
        "id_str" : [],
        "gender" : []
          }
    
    if '.gz' in file:
        infile = io.TextIOWrapper(gzip.open(file, 'r'))
    else:
        infile = open(file, 'r')
    bar = progressbar.ProgressBar()
    d = gender.Detector(case_sensitive=False)
    for line in bar(infile):
        if line != '\n':
            try:
                t = json.loads(line)
            except:
                continue
            if 'status' in t.keys():
                temp = t['status']
                getRid = t.pop('status', 'Entry not found')
                temp['user'] = t
                t = temp
            if 'user' in t.keys():
                name = t['user']['name']
                first_name = name.split(" ")[0]
                gender = d.get_gender(first_name)
                
                data['status_id'].append(t['id_str'])
                data['name'].append(t['user']['name'])
                data['screen_name'].append(t['user']['screen_name'])
                data['id_str'].append(t['user']['id_str'])
                data['gender'].append(gender)
    df = pd.DataFrame(data)
    if to_csv:
        df.to_csv(file.rstrip('.json')+'_edgelist.csv', index = False)
    else:
        return(df)
    
    
def get_followers(api, ID):
    import tweepy
    try:
        followers = api.followers_ids(ID)
        followers = fetch_profiles(api,  ids = followers)
        return(followers)
    except:
        print('Scraping Error')
        return([])


#%%
def get_followers_for_id(api, iuser_id):
    '''
    Gets ALL follower IDS for a given user
    Adapted from Mike K's code.  
    '''
    
    import tweepy
    
    followers = []
    
    try:
        for page in tweepy.Cursor(api.followers_ids, iuser_id).pages():
            followers += page
    except tweepy.TweepError as ex:
        print("get_followers_for_id(): could not get friends for user: " + str(iuser_id))
        print("\terror message: " + str(ex))
        
    return(followers)
    
#%%
def get_friends_for_id(api, iuser_id):
    '''
    Gets ALL friend IDS for a given user
    Adapted from Mike K's code.  
    '''
    
    import tweepy
    
    friends = []
    
    try:
        for page in tweepy.Cursor(api.friends_ids, iuser_id).pages():
            friends += page
    except tweepy.TweepError as ex:
        print("get_followers_for_id(): could not get friends for user: " + str(iuser_id))
        print("\terror message: " + str(ex))
        
    return(friends)
#%%
def get_all_tweets(api, id_str):
    #Twitter only allows access to a users most recent 3240 tweets with this method

    	
    #initialize a list to hold all the tweepy Tweets
    alltweets = []	
    	
    #make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.user_timeline(user_id = id_str,count=200)
    	
    #save most recent tweets
    alltweets.extend(new_tweets)
    	
    #save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1
    	
    #keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print("getting tweets before %s" % (oldest))
        
        #all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(user_id = id_str ,count=200,max_id=oldest)
        #save most recent tweets
        alltweets.extend(new_tweets)
        #update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1
        
    print("...%s tweets downloaded so far" % (len(alltweets)))
    
    return(alltweets)
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
