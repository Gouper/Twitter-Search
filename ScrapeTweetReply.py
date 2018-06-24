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
import DataClean


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
            tweet_text = DataClean.dataclean(tweet_text)
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

def get_tweet_from_sql(sheet, index):
    sql_search = make_sql_search(sheet, index)
    cur.execute(sql_search)
    results = cur.fetchall()
    return results
def make_sql_search(sheet, index):
    if index == 1:
        sql = "SELECT tweet_id FROM %s WHERE reply_count != '0' AND reply_index = 0" % (sheet)
    else:
        start = input("Enter the start date: ")
        end = input("Enter the end date: ")
        sql = "SELECT tweet_id FROM %s WHERE created_time < '%s' AND created_time > '%s' AND reply_count != '0' AND reply_index = 0" % (sheet, end, start)
    return sql
def update_sql(sheet, tweet_id):
    sql ="UPDATE %s SET reply_index = 1 WHERE tweet_id = '%s'" % (sheet, tweet_id)
    return sql


if __name__ == "__main__":
    conn = pymysql.connect(host='localhost', user='root', passwd='123456', db='twitter', port=3306, charset='utf8mb4')
    cur = conn.cursor()
    sheet = input("Enter the sheet you want to get reply:")
    index = int(input("Enter the method you want to export:\n1. Export All     2. Export Some :"))
    tweet_ids = get_tweet_from_sql(sheet, index)

    sql_insert = "REPLACE INTO tweet_reply(primal_id, tweet_id, real_name, nick_name, created_time, tweet_text)VALUES (%s, %s, %s, %s, %s, %s)"
    driver = webdriver.Chrome()
    for i in tweet_ids:
        scroll(driver, "aa", i[0])
        scrape_tweets(driver, i[0])
        sql_update = update_sql(sheet, i[0])
        cur.execute(sql_update)
        conn.commit()
        print("The tweet %s has been down" % i[0])
    driver.quit()
    conn.close()
