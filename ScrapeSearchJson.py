#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import json
import datetime
import logging as log
from time import sleep
from concurrent.futures import ThreadPoolExecutor
from DBUtils.PooledDB import PooledDB
import pymysql
try:
    from urllib.parse import urlparse, urlencode, urlunparse
except ImportError:
    from urlparse import urlparse, urlunparse
    from urllib import urlencode

import requests
from abc import ABCMeta
from abc import abstractmethod
from bs4 import BeautifulSoup
import DataClean

dbpool = PooledDB(creator=pymysql, mincached=10, maxcached=10, maxshared=20, maxconnections=100, host='localhost', user='root', passwd='123456', db='twitter', port=3306, charset='utf8mb4' )


class TwitterSearch(object):

    __meta__ = ABCMeta

    def __init__(self, rate_delay, error_delay=5):
        """
        :param rate_delay: How long to pause between calls to Twitter
        :param error_delay: How long to pause when an error occurs
        """
        self.rate_delay = rate_delay
        self.error_delay = error_delay

    def search(self, query, lang=0):
        self.perform_search(query, lang)

    def perform_search(self, query, lang=0):
        """
        Scrape items from twitter
        :param query:   Query to search Twitter with. Takes form of queries constructed with using Twitters
                        advanced search: https://twitter.com/search-advanced
        :param lang:   Choose a language to set get tweets's language
        """
        url = self.construct_url(query, lang=lang)
        continue_search = True
        min_tweet = None
        response = self.execute_search(url)
        while response is not None and continue_search and response['items_html'] is not None:
            tweets = self.parse_tweets(response['items_html'])

            # If we have no tweets, then we can break the loop early
            if len(tweets) == 0:
                break

            # If we haven't set our min tweet yet, set it now
            if min_tweet is None:
                min_tweet = tweets[0]

            continue_search = self.save_tweets(tweets)

            # Our max tweet is the last tweet in the list
            max_tweet = tweets[-1]
            if min_tweet['tweet_id'] is not max_tweet['tweet_id']:
                if "min_position" in response.keys():
                    max_position = response['min_position']
                else:
                    max_position = "TWEET-%s-%s" % (max_tweet['tweet_id'], min_tweet['tweet_id'])
                url = self.construct_url(query, lang=lang, max_position=max_position)
                # Sleep for our rate_delay
                sleep(self.rate_delay)
                response = self.execute_search(url)

    def execute_search(self, url):
        """
        Executes a search to Twitter for the given URL
        :param url: URL to search twitter with
        :return: A JSON object with data from Twitter
        """
        try:
            # Specify a user agent to prevent Twitter from returning a profile card
            headers = {
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.'
                              '86 Safari/537.36'
            }
            req = requests.get(url, headers=headers)
            # response = urllib2.urlopen(req)
            data = json.loads(req.text)
            return data

        # If we get a ValueError exception due to a request timing out, we sleep for our error delay, then make
        # another attempt
        except Exception as e:
            log.error(e)
            log.error("Sleeping for %i" % self.error_delay)
            sleep(self.error_delay)
            return self.execute_search(url)

    @staticmethod
    def parse_tweets(items_html):
        """
        Parses Tweets from the given HTML
        :param items_html: The HTML block with tweets
        :return: A JSON list of tweets
        """
        soup = BeautifulSoup(items_html, "html.parser")
        tweets = []
        for li in soup.find_all("li", class_='js-stream-item'):

            # If our li doesn't have a tweet-id, we skip it as it's not going to be a tweet.
            if 'data-item-id' not in li.attrs:
                continue

            tweet = {
                'tweet_id': li['data-item-id'],
                'text': None,
                'user_id': None,
                'user_screen_name': None,
                'user_name': None,
                'created_at': None,
                'retweets': 0,
                'favorites': 0
            }

            # Tweet Text
            text_p = li.find("p", class_="tweet-text")
            if text_p is not None:
                tweet['text'] = text_p.get_text()
                tweet['text'] = DataClean.dataclean(tweet['text'])

            # Tweet User ID, User Screen Name, User Name
            user_details_div = li.find("div", class_="tweet")
            if user_details_div is not None:
                tweet['user_id'] = user_details_div['data-user-id']
                tweet['user_screen_name'] = user_details_div['data-user-id']
                tweet['user_name'] = user_details_div['data-name']

            # Tweet date
            date_span = li.find("span", class_="_timestamp")
            if date_span is not None:
                tweet['created_at'] = float(date_span['data-time-ms'])

            # Tweet Retweets
            retweet_span = li.select("span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount")
            if retweet_span is not None and len(retweet_span) > 0:
                tweet['retweets'] = int(retweet_span[0]['data-tweet-stat-count'])

            # Tweet Favourites
            favorite_span = li.select("span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount")
            if favorite_span is not None and len(retweet_span) > 0:
                tweet['favorites'] = int(favorite_span[0]['data-tweet-stat-count'])

            reply_span = li.select("span.ProfileTweet-action--reply > span.ProfileTweet-actionCount")
            if reply_span is not None and len(reply_span) > 0:
                tweet['reply'] = int(reply_span[0]['data-tweet-stat-count'])

            tweets.append(tweet)
        return tweets

    @staticmethod
    def construct_url(query, lang=0, max_position=None):
        """
        For a given query, will construct a URL to search Twitter with
        :param query: The query term used to search twitter
        :param max_position: The max_position value to select the next pagination of tweets
        :return: A string URL
        """

        languages = {1: 'en', 2: 'it', 3: 'es', 4: 'fr', 5: 'de', 6: 'ru', 7: 'zh'}
        params = {
            # Type Param
            'f': 'tweets',
            # Query Param
            'q': query
        }

        # If our max_position param is not None, we add it to the parameters
        if max_position is not None:
            params['max_position'] = max_position

        url_tupple = ('https', 'twitter.com', '/i/search/timeline', '', urlencode(params), '')
        url = urlunparse(url_tupple)
        if lang != 0:
            url += "&l={}".format(languages[lang])
        url += "&src=typd"
        return url

    @abstractmethod
    def save_tweets(self, tweets):
        """
        An abstract method that's called with a list of tweets.
        When implementing this class, you can do whatever you want with these tweets.
        """


class TwitterSearchImpl(TwitterSearch):

    def __init__(self, rate_delay, error_delay, max_tweets):
        """
        :param rate_delay: How long to pause between calls to Twitter
        :param error_delay: How long to pause when an error occurs
        :param max_tweets: Maximum number of tweets to collect for this example
        """
        super(TwitterSearchImpl, self).__init__(rate_delay, error_delay)
        self.max_tweets = max_tweets
        self.counter = 0

    def save_tweets(self, tweets):
        """
        Save tweets into mysql database.
        :return: A Boolean to judge whether continue to get tweets
        """
        sql = "REPLACE INTO search_tweet(tweet_id, user_id, user_name, created_time, tweet_text)VALUES (%s, %s, %s, %s, %s)"
        conn = dbpool.connection()
        cur = conn.cursor()
        tweet_list = [] #used to save into mysql
        for tweet in tweets:
            # Lets add a counter so we only collect a max number of tweets
            self.counter += 1

            if tweet['created_at'] is not None:
                t = datetime.datetime.fromtimestamp((tweet['created_at']/1000))
                fmt = "%Y-%m-%d %H:%M:%S"
                t = t - datetime.timedelta(hours=8)
            log.info("%i [%s] -{%s}--[%s]-%s" % (self.counter, t.strftime(fmt), tweet['tweet_id'], tweet['user_name'], tweet['text']))
            tweet['user_name'] = tweet['user_name'].replace(u'\xa0', u' ')
            tweet['text'] = tweet['text'].replace(u'\xa0', u' ')
            tweet_list.append([tweet['tweet_id'], tweet['user_id'], tweet['user_name'], t.strftime(fmt), tweet['text']])
            # When we've reached our max limit, return False so collection stops
            if self.max_tweets is not None and self.counter >= self.max_tweets:
                cur.executemany(sql, tweet_list)
                conn.commit()
                conn.close()
                return False
        cur.executemany(sql, tweet_list)
        conn.commit()
        conn.close()
        return True


class TwitterSlicer(TwitterSearch):
    """
    The only additional parameters a user has to input, is a since date, and max_tweets.
    """
    def __init__(self, rate_delay, error_delay, since,  max_tweets):
        super(TwitterSlicer, self).__init__(rate_delay, error_delay)
        self.since = since
        self.max_tweets = max_tweets
        self.counter = 0

    def search(self, query, lang=0):
        since_query = self.since
        until_query = self.since + datetime.timedelta(days=1)
        day_query = "%s since:%s until:%s" % (query, since_query.strftime("%Y-%m-%d"),
                                              until_query.strftime("%Y-%m-%d"))
        self.perform_search(day_query, lang=lang)

    def save_tweets(self, tweets):
        """
        Save tweets into mysql database.
        """
        sql = "REPLACE INTO search_tweet(tweet_id, user_id, user_name, created_time, tweet_text, reply_count, retweet_count, like_count, reply_index)VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0)"
        conn =dbpool.connection()
        cur = conn.cursor()
        tweet_list = []  # used to save into mysql
        for tweet in tweets:
            # Lets add a counter so we only collect a max number of tweets
            self.counter += 1
            if tweet['created_at'] is not None:
                t = datetime.datetime.fromtimestamp((tweet['created_at']/1000))
                fmt = "%Y-%m-%d %H:%M:%S"
                t = t - datetime.timedelta(hours=8)
                log.info("%i [%s] -{%s}--[%s]-%s" % (self.counter, t.strftime(fmt), tweet['tweet_id'], tweet['user_name'], tweet['text']))
                tweet['user_name'] = tweet['user_name'].replace(u'\xa0', u' ')
                tweet['text'] = tweet['text'].replace(u'\xa0', u' ')
                tweet_list.append([tweet['tweet_id'], tweet['user_id'], tweet['user_name'], t.strftime(fmt), tweet['text'], tweet['retweets'], tweet['reply'], tweet['favorites']])
                # When we've reached our max limit, return False so collection stops
                if self.max_tweets is not None and self.counter >= self.max_tweets:
                    cur.executemany(sql, tweet_list)
                    conn.commit()
                    conn.close()
                    t = self.since.strftime("%Y-%m-%d")
                    log.info("The %s has been completed!Collected %i tweets!" % (t, self.counter))
                    return False
        cur.executemany(sql, tweet_list)
        conn.commit()
        conn.close()
        return True

class MultiThread:
    """
    Class of make get tweets by python threading
    """
    def __init__(self, rate_delay_seconds, error_delay_seconds, since, until,  max_tweets, n_threads=1):
        self.rate_delay_seconds = rate_delay_seconds
        self.error_delay_seconds = error_delay_seconds
        self.since = since
        self.until = until
        self.max_tweets = max_tweets
        self.n_threads = n_threads

    def search_thread(self, query, lang=0):
        """
        :param query:
        :param lang: Choose a language to set get tweets's language
        :return:
        """
        n_days = (self.until - self.since).days
        tp = ThreadPoolExecutor(max_workers=self.n_threads)
        for i in range(0, n_days):
            since_query = self.since + datetime.timedelta(days=i)
            twisi = TwitterSlicer(rate_delay_seconds, error_delay_seconds, since_query, self.max_tweets)
            tp.submit(twisi.search, query, lang=lang)
        tp.shutdown(wait=True)

if __name__ == '__main__':
    log.basicConfig(level=log.INFO)

    search_query = input("Enter the keywords you want to search:")
    rate_delay_seconds = 0
    error_delay_seconds = 5
    # twi = TwitterSearchImpl(rate_delay_seconds, error_delay_seconds, None)
    # twi.search(search_query)
    lang = int(input("0) All Languages 1) English | 2) Italian | 3) Spanish | 4) French | 5) German | 6) Russian | 7) Chinese\nEnter the language you want to use: "))

    select_tweets_since = input("Enter the start date in (Y-M-D): ")
    select_tweets_until = input("Enter the end date in (Y-M-D): ")
    select_tweets_since = datetime.datetime.strptime(select_tweets_since, '%Y-%m-%d')
    select_tweets_until = datetime.datetime.strptime(select_tweets_until, '%Y-%m-%d')
    threads = 10
    choose = int(input("input what kind of method you want to get:\n1).the day you want get some tweets   2).the day you want get enough tweets :"))
    if choose == 1:
        max_tweets = int(input("Enter the maximum tweets number that every day you will collect:"))
        mul = MultiThread(rate_delay_seconds, error_delay_seconds, select_tweets_since, select_tweets_until, max_tweets, threads)
        mul.search_thread(search_query, lang)
    elif choose == 2:
        mul = MultiThread(rate_delay_seconds, error_delay_seconds, select_tweets_since, select_tweets_until, None, threads)
        mul.search_thread(search_query, lang)