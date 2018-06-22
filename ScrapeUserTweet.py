#! usr/bin/python3
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import time
from csv import DictWriter
import pprint
import datetime
from datetime import date, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException,NoSuchElementException
import pymysql
import DataClean


def scroll(driver, start_date, end_date, user):
    tweet_selector = 'li.js-stream-item'
    url = 'https://twitter.com/search?f=tweets&vertical=default&q=from%3A'
    url += "{}%20".format(user)
    url += "since%3A{}%20until%3A{}".format(start_date, end_date)
    url += 'include%3Aretweets&src=typd'
    print(url)
    driver.get(url)
    try:
        found_tweets = driver.find_elements_by_css_selector(tweet_selector)
        increment = 10

        while len(found_tweets) >= increment:
            print('scrolling down to load more tweets')
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(1)
            found_tweets = driver.find_elements_by_css_selector(tweet_selector)
            increment += 10
    except NoSuchElementException:
        print('no tweets on this day')

def scrape_tweets(driver):
    try:
        tweet_divs = driver.page_source
        obj = BeautifulSoup(tweet_divs, "html.parser")
        content = obj.find_all("div", class_="content")
        dates = []
        names = []
        tweet_texts = []
        tid = []
        real_name = []
        reply_counts = []
        retweet_counts = []
        like_counts = []
        for i in content:
            date = i.find_all("span", class_="_timestamp")[0].get("data-time")
            #  GMT:0
            date = datetime.datetime.utcfromtimestamp(int(date)).strftime('%Y-%m-%d %H:%M:%S')
            try:
                nick_name = (i.find_all("strong", class_="fullname")[0].string).strip()
                user_name = i.find_all("a", class_="account-group")[0].get("href")
                user_name = user_name[1:]
            except AttributeError:
                user_name = "Anonymous"
                nick_name = "Anonymous"

            tweet_id = i.find_all("a", class_="tweet-timestamp")[0].get("data-conversation-id")

            count = i.find_all("span", class_="ProfileTweet-actionCount")
            reply_count = count[0].get("data-tweet-stat-count")
            retweet_count = count[1].get("data-tweet-stat-count")
            like_count = count[2].get("data-tweet-stat-count")

            tweets = i.find("p", class_="tweet-text").strings
            tweet_text = "".join(tweets)
            tweet_text = DataClean.dataclean(tweet_text)
            # hashtags = i.find_all("a", class_="twitter-hashtag")[0].string
            dates.append(date)
            names.append(nick_name)
            tweet_texts.append(tweet_text)
            tid.append(tweet_id)
            real_name.append(user_name)
            reply_counts.append(reply_count)
            retweet_counts.append(retweet_count)
            like_counts.append(like_count)

        data = {
            "date": dates,
            "nick_name": names,
            "tweet": tweet_texts,
            "tweet_id": tid,
            "real_name": real_name,
            "reply_count": reply_counts,
            "retweet_count": retweet_counts,
            "like_count": like_counts
        }
        # make_csv(data)
        save_into_sql(data)

    except Exception as e:
        print(e)
        print("Whoops! Something went wrong!")
        driver.quit()

def save_into_sql(data):
    l = len(data['date'])
    print("count: %d" % l)
    tweet_list = []
    for i in range(l):
        data['nick_name'][i] = data['nick_name'][i].replace(u'\xa0', u' ')
        data['tweet'][i] = data['tweet'][i].replace(u'\xa0', u' ')
        tweet_list.append([data['tweet_id'][i], data['real_name'][i], data['nick_name'][i], data['date'][i], data['tweet'][i], data['reply_count'][i], data['retweet_count'][i], data['like_count'][i]])
    cur.executemany(sql1, tweet_list)
    conn.commit()

def get_all_dates(start_date, end_date):
    dates = []
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    step = timedelta(days=1)
    while start_date <= end_date:
        dates.append(str(start_date.date()))
        start_date += step

    return dates

if __name__ == "__main__":
    sql1 = "REPLACE INTO user_tweet(tweet_id, real_name, nick_name, created_time, tweet_text, reply_count, retweet_count, like_count, reply_index)VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0)"
    conn = pymysql.connect(host='localhost', user='root', passwd='123456', db='twitter', port=3306, charset='utf8mb4')
    cur = conn.cursor()
    sql_user = "SELECT * FROM user_need WHERE user_index = 0"
    cur.execute(sql_user)
    users = cur.fetchall()

    for user in users:
        # user = input("Enter the user: ")
        start_date = input("Enter the start date in (Y-M-D): ")
        end_date = input("Enter the end date in (Y-M-D): ")
        all_dates = get_all_dates(start_date, end_date)
        driver = webdriver.Chrome()
        for i in range(len(all_dates) - 1):
            scroll(driver, str(all_dates[i]), str(all_dates[i + 1]), user[0])
            scrape_tweets(driver)
            time.sleep(2)
            print("The tweets for {} are ready!".format(all_dates[i]))
        driver.quit()
        sql_update = "UPDATE user_need SET user_index = 1 WHERE user_name = '%s'" % (user[0])
        cur.execute(sql_update)
        conn.commit()
    conn.close()

