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
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import pymysql


def scroll(driver, user, tid):
    tweet_selector = 'li.js-stream-item'
    # tweet_selector = 'ThreadedConversation--loneTweet'
    url = 'https://twitter.com/'
    url += "{}/status/{}".format(user, tid)
    print(url)
    driver.get(url)
    try:
        found_tweets = driver.find_elements_by_css_selector(tweet_selector)
        increment = 0
        while len(found_tweets) > increment:
            for i in range(4):
                ActionChains(driver).key_down(Keys.PAGE_DOWN).perform()
                time.sleep(0.5)
            time.sleep(1)
            increment = len(found_tweets)
            print('scrolling down to load more tweets %s' % (increment))
            found_tweets = driver.find_elements_by_css_selector(tweet_selector)
    except NoSuchElementException:
        print('no tweets on this day')

def scrape_tweets(driver, source_id):
    try:
        tweet_divs = driver.page_source
        obj = BeautifulSoup(tweet_divs, "html.parser")
        content = obj.find_all("li", class_="ThreadedConversation--loneTweet")
        dates = []
        names = []
        tweet_texts = []
        uid = []
        tid = []
        real_name = []
        for i in content:
            # print(i)
            date = i.find_all("span", class_="_timestamp")[0].get("data-time")
            date = datetime.datetime.utcfromtimestamp(int(date))
            try:
                nick_name = (i.find_all("strong", class_="fullname")[0].string).strip()
                user_name = i.find_all("a", class_="account-group")[0].get("href")
                user_name = user_name[1:]
            except AttributeError:
                user_name = "Anonymous"
                nick_name = "Anonymous"

            user_id = i.find_all("a", class_="account-group")[0].get("data-user-id")
            tweet_id = i.find_all("li", class_="js-stream-item stream-item stream-item ")[0].get("data-item-id")
            try:
                tweets = i.find("p", class_="tweet-text").strings

            except Exception as e:
                print(e)
                tweets = " "
            tweet_text = "".join(tweets)
            # hashtags = i.find_all("a", class_="twitter-hashtag")[0].string
            dates.append(date)
            names.append(nick_name)
            tweet_texts.append(tweet_text)
            uid.append(user_id)
            tid.append(tweet_id)
            real_name.append(user_name)

        data = {
            "date": dates,
            "nick_name": names,
            "tweet": tweet_texts,
            "user_id": uid,
            "tweet_id": tid,
            "real_name": real_name
        }
        # make_csv(data)
        save_into_sql(data, source_id)

    except Exception as e:
        print(e)
        print("Whoops! Something went wrong!")
        driver.quit()

def save_into_sql(data, source_id):
    l = len(data['date'])
    tweet_list = []
    for i in range(l):
        data['nick_name'][i] = data['nick_name'][i].replace(u'\xa0', u' ')
        data['tweet'][i] = data['tweet'][i].replace(u'\xa0', u' ')
        tweet_list.append([source_id, data['tweet_id'][i], data['real_name'][i], data['nick_name'][i], data['date'][i], data['tweet'][i]])
    cur.executemany(sql_insert, tweet_list)
    conn.commit()

def get_tweet_from_sql():
    sql_search = "SELECT real_name, tweet_id FROM user_tweet WHERE reply_index = 0"
    cur.execute(sql_search)
    results = cur.fetchall()
    return results


if __name__ == "__main__":
    sql_insert = "REPLACE INTO tweet_reply(tweet_id, reply_id, real_name, nick_name, created_time, tweet_text)VALUES (%s, %s, %s, %s, %s, %s)"

    conn = pymysql.connect(host='localhost', user='root', passwd='123456', db='twitter', port=3306, charset='utf8mb4')
    cur = conn.cursor()
    tweet_ids = get_tweet_from_sql()
    driver = webdriver.Chrome()
    for i in tweet_ids:
        scroll(driver, i[0], i[1])
        scrape_tweets(driver, i[1])
        sql_update = "UPDATE user_tweet SET reply_index = 1 WHERE tweet_id = '%s'" % (i[1])
        cur.execute(sql_update)
        conn.commit()
        print("The tweet(%s) has been down" % i[1])
    driver.quit()
    conn.close()
