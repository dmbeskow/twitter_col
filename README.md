## twitter_col: Twitter Collection, Analysis, and Visualization in Python

Still in development...

### Installation

```bash
pip install --user --upgrade git+git://github.com/dmbeskow/twitter_col.git
```

### Examples

Here's an example of how to get all network files related to a Twitter JSON

```python
import twitter_col
twitter_col.get_all_network_files('florida_schoolShooting_2018-02-17.json',
                      file_prefix='FLshooting', name='id_str')
```

The command above will generate individual network files (i.e. mention file, 
retweet file, and hashtag file).  If you just want a simple edgelist with some or 
all of these, use the command below:

```python
twitter_col.get_edgelist(file = 'sharkwheat.json', mentions = True,
 replies = True, retweets = True,)
```