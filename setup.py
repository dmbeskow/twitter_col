from setuptools import setup

setup(name='twitter_col',
      version='0.1',
      description='Functions to help collect and analyze twitter data',
      url='http://github.com/dbeskow/twitter_col',
      author='David Beskow',
      author_email='dnbeskow@gmail.com',
      license='MIT',
      packages=['twitter_col'],
      install_requires=[
              'tweepy',
              'pandas',
              'progressbar2',
              'textblob',
              'matplotlib'
              ],
      scripts=['bin/stream_content'],
      zip_safe=False)
