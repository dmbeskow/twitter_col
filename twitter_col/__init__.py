def check_tweet(tweet):
    """
    Takes user objects and reverses them to create status objects
    
    """
    import twitter_col
    if 'status' not in tweet.keys():
        if 'friends_count' in tweet.keys():
            tweet['status'] = twitter_col.get_empty_status()
    if 'status' in tweet.keys():
        temp = tweet['status']
        getRid = tweet.pop('status', 'Entry not found')
        temp['user'] = tweet
        tweet = temp
        return(tweet)
        
#%%

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
    if 'user_mentions' in tweet['entities']:
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
    if 'urls' in tweet['entities']: 
        if len(tweet['entities']['urls']) > 0:
            for u in tweet['entities']['urls']:
                url.append(u['expanded_url'])
    return(url)
#%%
def get_emojis(tweet):
    """
    Returns list of emoji's for a tweet.  If no emoji's, returns empty list
    """
    import emoji
    string = tweet['text']
    return [c for c in string if c in emoji.UNICODE_EMOJI]
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
def extract_mentions(files, file_prefix = 'twitter', name = 'id_str', to_csv = False):
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
            if line != '\n':
                try:
                    tweet = json.loads(line)
                except:
                    continue
                if 'status' in tweet.keys():
                    temp = tweet['status']
                    getRid = tweet.pop('status', 'Entry not found')
                    temp['user'] = tweet
                    tweet = temp
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
                     to_csv = False):
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
            if line != '\n':
                try:
                    tweet = json.loads(line)
                except:
                    continue
                if 'status' in tweet.keys():
                    temp = tweet['status']
                    getRid = tweet.pop('status', 'Entry not found')
                    temp['user'] = tweet
                    tweet = temp
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
def extract_emoji(files, file_prefix = 'twitter',  to_csv = False, name = 'id_str'):
    """
   Creates  csv containing all emojis in a set of tweets 
   
    """
    import json, io, gzip, time
    import pandas as pd
    if not isinstance(files, list):
       files = [files]
    final = {'date': [],'emoji': [], 'user': [] , 'status_id': []}
    for f in files:
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        for line in infile:
             if line != '\n':
                try:
                    tweet = check_tweet(json.loads(line))
                except:
                    continue
                E = get_emojis(tweet)
                if len(E) > 0:
                    for emoj in E:
                        final['user'].append(tweet['user'][name])
                        final['emoji'].append(emoj)
                        final['status_id'].append(tweet['id_str'])
                        final['date'].append(tweet['created_at'])
    df = pd.DataFrame(final)
    if to_csv:
        df.to_csv(file_prefix + '_emojis_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False ,  encoding = 'utf-8', columns = ['user', 'emoji', 'status_id','date'])
    else:
        return(df[['user', 'emoji', 'status_id','date']])
        
#%%
def extract_urls(files, file_prefix = 'twitter',  to_csv = False, name = 'id_str'):
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
             if line != '\n':
                try:
                    tweet = json.loads(line)
                except:
                    continue
                if tweet is not None:
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
    if not isinstance(files, list):
       files = [files]
    final = {'date': [],'type':[] , 'display_url': [], 'expanded_url': [], 'media_url': [], 'media_url_https': [],  'user': [] , 'status_id': []}
    for f in files:
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        bar =  progressbar.ProgressBar()
        for line in bar(infile):
            if line != '\n':
                try:
                    tweet = json.loads(line)
                except:
                    continue
                if 'status' in tweet.keys():
                    temp = tweet['status']
                    getRid = tweet.pop('status', 'Entry not found')
                    temp['user'] = tweet
                    tweet = temp
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
                     to_csv = False):
    """
   Creates hashtag edgelist (either user to hashtag OR comention).  
   Can return data.frame or write to csv.  
   
    """
    import json, io, gzip, time
    import pandas as pd
    import itertools
    import progressbar
    if not isinstance(files, list):
       files = [files]
    final = {'hash1': [],'hash2': [], 'user': [] , 'status_id': [], 'date':  []}
    bar = progressbar.ProgressBar()
    for f in bar(files):
        if '.gz' in f:
            infile = io.TextIOWrapper(gzip.open(f, 'r'))
        else:
            infile = open(f, 'r')
        for line in infile:
            if line != '\n':
                try:
                    tweet = json.loads(line)
                except:
                    continue
                h = get_hash(tweet)
                if len(h) > 1:
                    combo = list(itertools.combinations(h, 2))
                    for pair in combo:
                        final['user'].append(tweet['user'][name])
                        final['hash1'].append(pair[0])
                        final['hash2'].append(pair[1])
                        final['status_id'].append(tweet['id_str'])
                        final['date'].append(tweet['created_at'])
#            except:
#                continue
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
    if not isinstance(files, list):
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
    if not isinstance(files, list):
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
    
def parse_twitter_json(files, file_prefix = 'twitter', to_csv = False,
                       sentiment = False, keep_empty_status = True):
    """
    This parses 'tweet' json to a pandas dataFrame.

    Most field names are self explanatory, except 'id_str' is user ID.
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
         "status_isquote" : [],
          "status_lang" : [],
          "status_id" : [],
          "status_created_at": [],
          "status_retweet_count": [],
          "status_favorite_count": [],
          "retweet_status_id": [],
          "reply_to_user_id": [],
          "reply_to_status_id": [],
          "has_default_profile" : []
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
                if keep_empty_status:
                    if 'status' not in t.keys():
                        if 'friends_count' in t.keys():
                                t['status'] = get_empty_status()

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
                    data['status_retweet_count'].append(t['retweet_count'])
                    data['status_favorite_count'].append(t['favorite_count'])
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

                    if 'quoted_status' in t:
                        data['status_isquote'].append(True)
                    else:
                        data['status_isquote'].append(False)

                            

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
                    if t['user']['profile_image_url'] == "http://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png":
                        data['has_default_profile'].append(True)
                    else:
                        data['has_default_profile'].append(False)


    df = pd.DataFrame(data, dtype = str)
    df.loc[df['status_isquote'] == 'True', 'status_isretweet'] = 'False'

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
    This parses 'tweet' json to a pandas dataFrame. 
    
    Most field names are self explanatory, except 'id_str' is user ID.
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
         "status_isquote" : [],
          "status_lang" : [],
          "status_id" : [],
          "status_created_at": [],
          "status_retweet_count": [],
          "status_favorite_count": [],
          "retweet_status_id": [],
          "reply_to_user_id": [],
          "reply_to_status_id": [],
          "has_default_profile" : []
          }

    bar = progressbar.ProgressBar()
    for t in bar(List):
        if 'status' not in t.keys():
            if 'friends_count' in t.keys():
                t['status'] = get_empty_status()
                    
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
            data['status_retweet_count'].append(t['retweet_count'])
            data['status_favorite_count'].append(t['favorite_count'])
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
                
            if 'quoted_status' in t:
                data['status_isquote'].append(True)
            else:
                data['status_isquote'].append(False)
    
    
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
                
            if t['user']['profile_image_url'] == "http://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png":
                data['has_default_profile'].append(True)
            else:
                data['has_default_profile'].append(False)

        
    df = pd.DataFrame(data, dtype = str)
    df.loc[df['status_isquote'] == 'True', 'status_isretweet'] = 'False'
    
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
    
def parse_only_text(files, file_prefix = 'twitter', to_csv = False, 
                       sentiment = False, keep_empty_status = True):
    """
    This parses 'tweet' json to a pandas dataFrame, but only gets the text, user id, 
    tweet id, and language settings. 
    
    Most field names are self explanatory, except 'id_str' is user ID.
    """
    import pandas as pd
    from textblob import TextBlob
    import json, time, io, gzip
    import progressbar
    
    if not isinstance(files, list):
       files = [files]
    data = { "id_str" : [],
        "lang" : [],
        "status_text" : [],
          "status_lang" : [],
          "status_id" : [],
          "status_created_at": []
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
                if keep_empty_status:
                    if 'status' not in t.keys():
                        if 'friends_count' in t.keys():
                                t['status'] = get_empty_status()
                    
                if 'status' in t.keys():
                    temp = t['status']
                    getRid = t.pop('status', 'Entry not found')
                    temp['user'] = t
                    t = temp
                if 'user' in t.keys():
                    data['id_str'].append(t['user']['id_str'])
                    data['lang'].append(t['user']['lang'])
                    data['status_lang'].append(t['lang'])
                    data['status_id'].append(t['id'])
                    data['status_created_at'].append(t['created_at'])
                    
                    if 'extended_tweet' in t.keys():
                        if 'full_text' in t['extended_tweet']:
                            data['status_text'].append(t['extended_tweet']['full_text'])
                    elif 'full_text' in t.keys():
                        data['status_text'].append(t['full_text'])
                    else:
                        data['status_text'].append(t['text'])
            
        
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
                 urls = False, hashtags = False, to_csv = False, kind = 'screen_name'):
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
    count = 0
    for line in bar(infile):
        if line == '\n':
            continue
        try:
            tweet = json.loads(line)
        except:
            count += 1
        dateTime = tweet['created_at']
        m = get_mention(tweet, kind = kind)
        if len(m) > 0 and mentions:
             for mention in m:
                 From.append(tweet['user'][kind])
                 To.append(mention)
                 Type.append('mention')
                 Time.append(dateTime)
                 ID.append(tweet['id_str'])
        if tweet['in_reply_to_user_id_str'] != None and replies:
            if kind == 'id_str':
                To.append(tweet['in_reply_to_user_id_str'])
            else:
                To.append(tweet['in_reply_to_screen_name' ])                
            From.append(tweet['user'][kind])
            Type.append('reply')
            Time.append(dateTime)
            ID.append(tweet['id_str'])
        if 'retweeted_status' in tweet.keys() and retweets:
             From.append(tweet['user'][kind])
             To.append(tweet['retweeted_status']['user'][kind])
             Type.append('retweet')
             Time.append(dateTime)
             ID.append(tweet['id_str'])
        u = get_urls(tweet)
        if len(u) > 0 and urls:
             for url in u:
                 From.append(tweet['user'][kind])
                 To.append(url)
                 Type.append('url')
                 Time.append(dateTime)
                 ID.append(tweet['id_str'])
        h = get_hash(tweet)
        if len(h) > 0 and hashtags:
             for Hash in h:
                 From.append(tweet['user'][kind])
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
    print("Total of ", count, "JSON load errors")
    if to_csv:
        df.to_csv(file.rstrip('.json')+'_edgelist.csv', index = False)
    else:
        return(df)
#%%
        
def get_edgelist_from_list(tweet_list, mentions = True, replies = True, retweets = True, 
                 urls = False, hashtags = False, to_csv = False):
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
    
    Note that if your data will be large, use 'fetch_profiles_file' which writes
    to disk as it goes rather than storing it in memory.
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
def fetch_profiles_file(api, screen_names = [] , ids = [] , prefix = 'user_profiles' ):
    """
    I copied this from:
        https://github.com/unitedstates/congress-legislators/blob/master/scripts/social/twitter.py
        
    A wrapper method around tweepy.API.lookup_users that handles the batch lookup of
      screen_names. Assuming number of screen_names < 10000, this should not typically
      run afoul of API limits (i.e. it's a good enough hack for now)
    `api` is a tweepy.API handle
    `screen_names` is a list of twitter screen names
    
    Returns: writes to disk as it goes
    """
    import tweepy, json, time
    TWITTER_PROFILE_BATCH_SIZE = 100
    from math import ceil
    
    with open(prefix + '_' + time.strftime('%Y%m%d') + '.json', 'w') as outfile:
        key, lookups = ['user_ids', ids] if ids else ['screen_names', screen_names]
        for batch_idx in range(ceil(len(lookups) / TWITTER_PROFILE_BATCH_SIZE)):
            offset = batch_idx * TWITTER_PROFILE_BATCH_SIZE
            # break lookups list into batches of TWITTER_PROFILE_BATCH_SIZE
            batch = lookups[offset:(offset + TWITTER_PROFILE_BATCH_SIZE)]
            try:
                for user in api.lookup_users(**{key: batch}):
                    outfile.write(json.dumps(user._json) + '\n')
            # catch situation in which none of the names in the batch are found
            # or else Tweepy will error out
            except tweepy.error.TweepError as e:
                if e.response.status_code == 404:
                    pass
                else: # some other error, raise the exception
                    raise e
            print("Batch", batch_idx)

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
   I adapted this from :
       https://github.com/unitedstates/congress-legislators/blob/master/scripts/social/twitter.py
       
   A wrapper method around tweepy.API.statuses_lookup that handles the batch lookup of
     Tweet IDs. 
   Returns: a list of dicts representing Twitter profiles
   """
   import tweepy
   TWITTER_PROFILE_BATCH_SIZE = 100
   from math import ceil
   
   statuses = []
   key, lookups = ['id_', ids] 
   for batch_idx in range(ceil(len(lookups) / TWITTER_PROFILE_BATCH_SIZE)):
       offset = batch_idx * TWITTER_PROFILE_BATCH_SIZE
       # break lookups list into batches of TWITTER_PROFILE_BATCH_SIZE
       batch = lookups[offset:(offset + TWITTER_PROFILE_BATCH_SIZE)]
       try:
           for s in api.statuses_lookup(**{key: batch}):
               statuses.append(s._json)
       # catch situation in which none of the names in the batch are found
       # or else Tweepy will error out
       except tweepy.error.TweepError as e:
           if e.response.status_code == 404:
               pass
           else: # some other error, raise the exception
               raise e
       print("Batch", batch_idx)
   return statuses 

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
    import tweepy, ast
    import pandas as pd
    final = {'id_str' : [], 'reason': []}
    bar = progressbar.ProgressBar()
    for uid in bar(uids):
        final['id_str'].append(uid)
        try:
            u = api.get_user(user_id=uid)
            final['reason'].append("Status OK")
        except tweepy.error.TweepError as e:
            e2 = ast.literal_eval(e.reason)[0]['message']
            final['reason'].append(e2)
    return(pd.DataFrame(final))
    
#%%
def dedupe_twitter(list_of_tweets):
    """
    This function dedupes a list of tweets based on tweet ID.
    """
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
    """
    This function will try to guess the gender of the Tweet user based on name.
    
    This function uses the gender_guesser package.
    
    """
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
    
#%%   
#def get_followers(api, ID):
#    """
#    Gets all follower IDs for a given id, and then fetches the full user object.
#    
#    Returns list of user objects.
#    """
#    import tweepy
#    try:
#        followers = api.followers_ids(ID)
#        followers = fetch_profiles(api,  ids = followers)
#        return(followers)
#    except:
#        print('Scraping Error')
#        return([])


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
    """
    Gets most recent 3240 tweets for a given user. 
    
    Returns list of tweets.
    
    """
    #Twitter only allows access to a users most recent 3240 tweets with this method

    	
    #initialize a list to hold all the tweepy Tweets
    alltweets = []	
    	
    #make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.user_timeline(user_id = id_str,count=200)
    	
    #save most recent tweets
    alltweets.extend(new_tweets)
    
    if len(alltweets) > 100:
    #save the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1
        	
        #keep grabbing tweets until there are no tweets left to grab
        while len(new_tweets) > 0:
#            print("getting tweets before %s" % (oldest))
            
            #all subsiquent requests use the max_id param to prevent duplicates
            new_tweets = api.user_timeline(user_id = id_str ,count=200,max_id=oldest)
            #save most recent tweets
            alltweets.extend(new_tweets)
            #update the id of the oldest tweet less one
            oldest = alltweets[-1].id - 1
            
    final = []
    for tweet in alltweets:
        final.append(tweet._json)
        
    print("...%s tweets downloaded so far" % (len(alltweets)))
    
    return(final)
    
#%%

def remove_bad_json_data(files):
    '''
    Goes through each file and removes ill formed json 
    
    Example: twitter_col.remove_bad_json_data(files)
    '''
    import io, gzip, json
    import progressbar
    
    if not isinstance(files, list):
        files = [files]
    for file in files:
        count = 0
        if '.gz' in file:
            infile = io.TextIOWrapper(gzip.open(file, 'r'))
            outfile = gzip.open('fixed_' + file, 'wt')
        else:
            infile = open(file, 'r')
            outfile = open('fixed_' + file, 'w')
        
        bar = progressbar.ProgressBar()
        count = 0
        for line in bar(infile):
            if line == '\n':
                continue
            try:
                tweet = json.loads(line)
                out = json.dumps(tweet)
                outfile.write(out + '\n')
                
            except:
                count += 1
        infile.close()
        outfile.close()
        print(file, 'has', str(count),'errors')
    
   #%%
   
   
def filter_tweets_by_date(files, start , stop, file_name):
    '''
    Filters tweets in a single file or list of files so that they fall within
    a given time window.  
    
    Example: twitter_col.filter_tweets_by_date(files, '2018-01-01', '2018-06-01','outfile.json')
    '''
    import io, gzip, json
    import progressbar
    import dateutil.parser
    from pytz import timezone
    
    start = dateutil.parser.parse(start).replace(tzinfo=timezone('UTC'))
    stop = dateutil.parser.parse(stop).replace(tzinfo=timezone('UTC'))
    
    if not isinstance(files, list):
        files = [files]
    with open(file_name, 'w') as outfile:
        bar = progressbar.ProgressBar()
        for file in bar(files):
            count = 0
            if '.gz' in file:
                infile = io.TextIOWrapper(gzip.open(file, 'r'))
            else:
                infile = open(file, 'r')
            
            bar = progressbar.ProgressBar()
            count = 0
            for line in infile:
                if line == '\n':
                    continue
                try:
                    tweet = json.loads(line)
                    created_at = dateutil.parser.parse(tweet['created_at'])
                    if created_at > start:
                        if created_at < stop:
                            
                            out = json.dumps(tweet)
                            outfile.write(out + '\n')
                    
                except:
                    count += 1
            infile.close()
            print(file, 'has', str(count),'errors')
#%%
def get_empty_status():          
    '''
    This function returns an empty or Null status.  This is used to attach to
    the dictionary of any account that has never tweeted
    '''
    
    status = {'contributors': None,
              'coordinates': None, 
              'created_at': None,
              'entities': {'hashtags': [],
                           'symbols': [],
                           'urls': [],
                           'user_mentions': []},
               'favorite_count': None,
               'favorited': None,
               'geo': None,
               'id': None,
               'id_str': None,
               'in_reply_to_screen_name': None,
               'in_reply_to_status_id': None,
               'in_reply_to_status_id_str': None,
               'in_reply_to_user_id': None,
               'in_reply_to_user_id_str': None,
               'is_quote_status': False,
               'lang': None,
               'place': None,
               'retweet_count': None,
               'retweeted': None,
               'source': None,
               'text': None,
               'truncated': None}
    return(status)
#%% 
def get_friend_follower_edgelist(fol_directory, frd_directory, follower_tag = 'followers', friend_tag = 'friends',column_label = 'id_str'):
    '''
    This function loops through a directory and builds friend/follower network 
    in an edgelist format. 
    
    Files must be uncompressed (not gzipped)
    
    kind can be 'both', 'follower', 'friend'
    
    Returns an edgelist pandas data frame.
    '''
    
    import os
    import pandas as pd
    import progressbar
    import re
    
    frd_files = os.listdir(frd_directory)
    fol_files = os.listdir(fol_directory)
    
    frd_files = [x for x in frd_files if friend_tag in x]
    frd_files = [frd_directory + '/' + x for x in frd_files]
    
    fol_files = [x for x in fol_files if follower_tag in x]
    fol_files = [fol_directory + '/' + x for x in fol_files]
    
    final = {'from': [],
             'to' : [],
             'type' : []}
    
    print('Getting friend links...\n')
    bar = progressbar.ProgressBar()
    for file in bar(frd_files):
        account = re.findall(r'\d+',file)
        account = max(account, key = len)
        temp = pd.read_csv(file)[column_label].tolist()
        for item in temp:
            final['from'].append(account)
            final['to'].append(item)
            final['type'].append('friend')
            
    print('Getting follower links...\n')
    bar = progressbar.ProgressBar()        
    for file in bar(fol_files):
        account = re.findall(r'\d+',file)
        account = max(account, key = len)
        temp = pd.read_csv(file)[column_label].tolist()
        for item in temp:
            final['to'].append(account)
            final['from'].append(item)
            final['type'].append('follower')
            
    final = pd.DataFrame(final)
    
    return(final)
            
#%%
def plot_time(file, breaks = 'D', field = 'status_created_at',  file_name = 'time.png' ):
    """
    This is a quick plot function that will create the data density for tweets in a single file
    or list of tiles.
    
    Prints matplotlib to screen
    """
    import pandas as pd
    import matplotlib.pyplot as pyplot
    df = parse_twitter_json(file, to_csv = False)
    df['date'] = pd.to_datetime(df[field])
    df.index = df.date
    ax = df['date'].resample('D').count().plot(title = "Twitter Timeline")
    fig = ax.get_figure()
    fig.savefig(file_name)            
        
            
#%%
def parse_only_ids(files):
    """
    This parses 'tweet' json to a pandas dataFrame, but only gets the text, user id,
    tweet id, and language settings. 'name' should be either
    'id_str' or 'screen_name'.
    """
    import pandas as pd
    from textblob import TextBlob
    import json, time, io, gzip
    import progressbar

    if not isinstance(files, list):
       files = [files]
    data = { "id_str" : []}
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

                data['id_str'].append(t['id_str'])
    df = pd.DataFrame(data, dtype = str)
    return(df)

#%%
    
def extract_coordinates(files, to_csv = False, file_prefix = 'topic'):
    """
    This parses 'tweet' json to a pandas dataFrame, but focuses on the point geo
    coordinates.  Also includes basic user data, date, and text of tweet.
    """
    import pandas as pd
    import json, time, io, gzip
    import progressbar
    
    if not isinstance(files, list):
       files = [files]
    data = { "id_str" : [],
        "screen_name" : [],
        "status_text" : [],
          "status_id" : [],
          "status_created_at": [], 
          "lat": [],
          "lon": []
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
                    if t['coordinates'] != None:
                        data['id_str'].append(t['user']['id_str'])
                        data['screen_name'].append(t['user']['screen_name'])
                        data['status_id'].append(t['id'])
                        data['status_created_at'].append(t['created_at'])
                        
                        if 'extended_tweet' in t.keys():
                            if 'full_text' in t['extended_tweet']:
                                data['status_text'].append(t['extended_tweet']['full_text'])
                        elif 'full_text' in t.keys():
                            data['status_text'].append(t['full_text'])
                        else:
                            data['status_text'].append(t['text'])
                            
                        data['lon'].append(t['coordinates']['coordinates'][0])
                        data['lat'].append(t['coordinates']['coordinates'][1])
            
        
    df = pd.DataFrame(data, dtype = str)
    
            
    if to_csv:
        df.to_csv(file_prefix + '_geo_coordinates_' + time.strftime('%Y%m%d-%H%M%S')+'.csv', 
                  index = False , encoding = 'utf-8')
    else:
        return(df)


#%%

def combine_dedupe(list_of_files, prefix = 'total_tweets'):
    import json, io, gzip
    import progressbar
    
    seen = {}
    with gzip.open(prefix + '.json.gz', 'wt') as outfile:
        bar = progressbar.ProgressBar()
        for f in bar(list_of_files):
            if '.gz' in f:
                infile = io.TextIOWrapper(gzip.open(f, 'r'))
            else:
                infile = open(f, 'r')
    
            for line in infile:
                if line != '\n':
                    try:
                        t = json.loads(line)
                        if 'status' in t.keys():
                            temp = t['status']
                            getRid = t.pop('status', 'Entry not found')
                            temp['user'] = t
                            t = temp
                        
                        tweet_id = t['id']
                        if tweet_id not in seen:
                            seen[tweet_id] = True
                            outfile.write(json.dumps(t) + '\n')
                    except:
                        continue
                        

#%%
def get_user_map(files):
    '''
    Function provided by Tom Magelinski
    
    Creates mapping from old screen names to new screen names based on retweet data
    '''
    if not isinstance(files, list):
        files = [files]
    user_map = {}
    for file in files:
        with gzip.open(file) as f:
            for line in f:
                t = json.loads(line)
                if 'retweeted_status' in t.keys():
                    if 'extended_tweet' in t.keys():
                        if 'full_text' in t['extended_tweet']:
                            text = t['extended_tweet']['full_text']
                    elif 'full_text' in t.keys():
                        text = t['full_text']
                    else:
                        text = t['text']

                    old_username = text.split('RT ')[1].split(':')[0][1:]
                    new_username = t['retweeted_status']['user']['screen_name']
                    if old_username != new_username:
                        if old_username not in user_map.keys():
                            user_map[old_username] = [new_username]
                        if new_username not in user_map[old_username]:
                            user_map[old_username] = user_map[old_username] + [new_username] 
    return user_map


#%%
def break_into_weeks(file, directory = 'weeks'):
    import io, gzip, json
    import dateutil.parser
    
    final = {}
    with io.TextIOWrapper(gzip.open(file, 'r')) as infile:
        for line in infile:
            t = json.loads(line)
            created_at = dateutil.parser.parse(t['created_at'])
            month = created_at.isocalendar()
            week = str(month[0]) + '_' + str(month[1])
            if week in final:
                final[week].append(t)
            else:
                final[week] = [t]
    for month in final:
        with open(directory + '/' + month + '.json', 'w') as outfile:
            for tweet in final[month]:
                outfile.write(json.dumps(tweet) + '\n')