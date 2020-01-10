## twitter_col: Twitter Collection, Analysis, and Visualization in Python

Warning...still in active development.

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

## Streaming Data

I've built these CLI's to work in a virtual environment.  While all the rest of the normal functions are available even if you aren't in a virtual environment, the streaming command line interfaces will only work with a virtual environment.  

We will create the virtual environment from the terminal in Mac or Linux or using the Windows Linux Subsystem (WSL) in Windows:

```bash
virtualenv -p python3 twitter-env
```

Then we activate the environment with the command

```bash
source twitter-env/bin/activate
```



### Streaming based on content

Both of the command line interfaces require the user to provide the path to a JSON file with their Twitter credentials.  Having created your Twitter credentials, place them in a json file with the format below:

```json
{
  "consumer_key": "XXXXXXXXXXXXXXXXXXXXXXXXXXXX",
  "consumer_secret": "XXXXXXXXXXXXXXXXXXXXXXXXXXXX",
   "access_token": "XXXXXXXXXXXXXXXXXXXXXXXXXXXX",
   "access_secret": "XXXXXXXXXXXXXXXXXXXXXXXXXXXX"
 }

```

In this section I will introduce the `stream_content` command line interface (CLI) that facilitates easy access to the streaming API with content filtering.  This allows you to filter the Streaming API by any token (hashtag, screen name, text, etc).

Let's say I want to stream content during the Worldcup in regards to Germany, France, and Spain.  I could use their country hashtags with the CLI command:


```python
stream_content key.json '#GER,#FRA,#ESP'
```

This CLI tool will create a new file every 20K tweets.  

In this case, the resulting file will be named '#GER_#FRA_#ESP_YYMMDD-hhmmss.json.gz'.  In general I find it is helpful to keep your search terms in the name of the file so you can remember how you obtained the data (remember, on the command line your parameters aren't nicely stored in a file).  

However, there are some times when you have a very messy list of search terms and you don't want them concatenated together to create your file name.  In this case you can invoke the -tag optional parameter to create your own file name prefix:


```python
stream_content key.json '#GER,#FRA,#ESP' -tag worldcup
```

This will create the filename 'worldcup.YYMMDD-hhmmss.json.gz'  

### Streaming based on geographic bounding box

 This allows you to filter the Streaming API by a rectangular bounding box (city, state, country, region).  If you need to find bounding boxes for specific countries, I recommend [country bounding boxes](https://gist.github.com/graydon/11198540).  

Let's say we want to stream data for New York City.  We could do this with the following command


```python
stream_geo key.json -74 40 -73 41
```

This CLI tool will create a new file every 20K tweets.  

This will create a file with the filename 'geo_-74.0_40.0_-73.0_41.0.YYMMDD-hhmmss.json.gz'.  In general it is nice to keep the bounding box in the file name for future reference.  If this is cumbersome, you can once again overwite this with the optional -tag flag:


```python
stream_geo key.json -74 40 -73 41 -tag nyc
```

which produces a file named 'nyc.YYMMDD-hhmmss.json.gz'



### Summary of functions

A list of functions is provided below:

| Tables   |      Are      |  Cool |
|----------|:-------------:|------:|
| col 1 is |  left-aligned | $1600 |
| col 2 is |    centered   |   $12 |
| col 3 is | right-aligned |    $1 |

| Function | Description |
|----------|-------------|
| `check_tweet`  | Takes user objects and reverses them to create status objects  |
| `combine_dedupe` | Combine and Dedupe list of Twitter JSON Files  |
| `convert_dates`  | This function converts twitter dates to python date objects  |
| `dedupe_twitter` |  This function dedupes a list of tweets based on tweet ID.  |
| `extract_coordinates` | Extracts Geo Coordinates to Pandas Dataframe  |
| `extract_emoji` | Creates  csv containing all emojis in a set of tweets  |
| `extract_gender` | This function will try to guess the gender of the Tweet user based on name. This function uses the gender_guesser package.  |
| `extract_hash_comention` |     Creates hashtag edgelist (either user to hashtag OR comention).  |
| `extract_hashtags` | Creates hashtag edgelist (either user to hashtag OR comention). |
| `extract_media`  |  Creates  csv containing all URLS in a set of tweets |
| `extract_mentions`  |  Creates mention edgelist.  Can return data.frame or write to csv. |
| `extract_reply_network`  |  Creates reply edgelist.  Can return data.frame or write to csv. |
| `extract_retweet_network`  |  Creates retweet edgelist.  Can return data.frame or write to csv. |
| `extract_urls`  |  Creates  csv containing all URLS in a set of tweets |
| `fetch_profiles`  |  A wrapper method around tweepy.API.lookup_users that handles the batch lookup of screen_names.  Returns list of users. |
| `fetch_profiles_file`  |  A wrapper method around tweepy.API.lookup_users that handles the batch lookup of screen_names (saving to file as it goes).  This is better for long lists with memory constraints. |
| `filter_tweets_by_date`  |  Filters tweets in a single file or list of files so that they fall within a given time window.   |
| `get_all_network_files`  |  This is a single command to get the hashtag network, comention network, and retweet network |
| `get_all_tweets`  |  Gets most recent 3240 tweets for a given user. Returns list of tweets. |
| `get_edgelist_file`  |  Builds an agent x agent edgelist of a Tweet json (normal or gzipped) |
| `get_edgelist_from_list`  |  Builds an agent x agent edgelist of a tweet list. |
| `get_emojis`  |  Returns list of emoji's for a tweet.  If no emoji's, returns empty list |
| `get_empty_status()``  |  This function returns an empty or Null status.  This is used to attach to the dictionary of any account that has never tweeted |
| `get_followers_for_id`  |  Gets ALL follower IDS for a given user Adapted from Mike K's code. |
| `get_friend_follower_edgelist`  |  This function loops through a directory and builds friend/follower network in an edgelist format. |
| `get_friends_for_id`  |  Gets ALL friend IDS for a given user Adapted from Mike K's code. |
| `get_hash`  |  Returns list of hashtags in a tweet.  If no hashtags, it returns an empty list. |
| `get_mention` |  Returns list of mentions in a tweet.  If no hashtags, it returns an empty list. |
| `get_reply_conversation`  |  Recursively extracts replies and replies to replies in order to pull all replies that are connected to a given status_id(s) |
| `get_sensitivity` | Returns sensitivity  |
| `get_urls`  |  Returns list of expanded urls in a tweet.  If no urls, returns an empty list. |
| `get_user_map`  |  Function provided by Tom Magelinski Creates mapping from old screen names to new screen names based on retweet data |
| `parse_only_ids`  |  This parses 'tweet' json to a pandas dataFrame, but only gets the text, user id, tweet id, and language settings. 'name' should be either 'id_str' or 'screen_name'. |
| `parse_only_text`  |  This parses 'tweet' json to a pandas dataFrame, but only gets the text, user id, tweet id, and language settings. |
| `parse_twitter_json`  |  This parses 'tweet' json to a pandas dataFrame. |
| `plot_time`  |  This is a quick plot function that will create the data density for tweets in a single file or list of tiles. Prints matplotlib to screen |
| `rehydrate`  |  A wrapper method around tweepy.API.statuses_lookup that handles the batch lookup of Tweet IDs. |
