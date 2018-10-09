## twitter_col: Twitter Collection, Analysis, and Visualization in Python

Still in development...

### Installation

```bash
pip install --user --upgrade git+git://github.com/dmbeskow/twitter_col.git
```

### Data Parsing Examples

Here's a basic example of how to parse a JSON file ane extract the most used fields
into either a pandas data frame (or write to csv by adding `to_csv = True`).  Also
note that this function can handle gzip json files.

```python
import twitter_col
df = twitter_col.parse_twitter_json('myFile.json', to_csv = False, sentiment = False)
```
### Network Extraction Examples

Here's an example of how to get an edgelist of communication actions (retweet, reply, mention) between agents.  You can also  also add url/hashtag mentions, though these aren't included by default.

```python
import twitter_col
df = twitter_col.get_edgelist_file('florida_schoolShooting_2018-02-17.json',
  mentions = True,   replies = True,   retweets = True,
  urls = False, hashtags = False, to_csv = True)
```
The code below will return the hashtag comention edgelist for a JSON file.

```python
edgelist = twitter_col.extract_hash_comention(files, name = 'id_str', to_csv = False)
```
The code below will extract hashtags from tweets with other metadata that facilitates general hashtag analysis.

```python
df = twitter_col.extract_hashtags(files, name = 'id_str', to_csv = False)
ha['hashtag'].value_counts().head(n = 15).plot(kind = 'bar', title = 'Top Hashtags')
```

And the same for URLS:

```python
url = twitter_col.extract_urls(files,  name = 'id_str')
url['url'].value_counts().head(n = 15).sort_values().plot(kind = 'barh', title = 'Top 15 URLs')

```

### Scraping Utilities

The code below assumes that you've authenticated with the `tweepy` Python package with an object called 'api'.  

Below is a basic script for a content related REST Scrape.  This script will take a list of terms (no limit in number) and individually search twitter for any time these terms appear in the last week.  It will save these into separate files by term.  Note that each file will not contain duplicates, but that combining the files will create duplicates.  The 'prefix' adds a string to the file names that allows you to differentiate separate projects (i.e. 'NBA' vs 'NFL' scrapes).

```python
import tweepy
import sys
from twitter_col import scrape

from pathlib import Path
home = str(Path.home())

# Replace the API_KEY and API_SECRET with your application's key and secret.
auth = tweepy.AppAuthHandler( 'consumer_key', 'consumer_secret')

api = tweepy.API(auth, wait_on_rate_limit=True,
                                   wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)



terms = ['#NBA','#basketball','#jordan']

scrape.rest_scrape(api, searchQuery = terms, prefix = 'NBA',sinceId = None, max_id = -1 )


```



Below we grab the JSON data for a list of users (either screen name or id).  It will return a list of user objects, which can be written to file.

```python
import json
profiles = twitter_col.fetch_profiles(api, screen_names = [], ids = [])

with open('myfile.json', 'w') as outfile:
  for user in profiles:
    out = json.dumps(user)
    outfile.write(out + '\n')
```

The code below will get up to the last 3200 tweets for a user.

```python
timelin = twitter_col.get_all_tweets(api, id_str)                     
```

### General Utilities

Below are some general utilities.

This will convert twitter date strings to python date-time objects.  We can captures this and bring it back into Python

```python
dates = twitter_col.convert_dates(df['status_created_at'].tolist())
df['dates'] = dates
```
