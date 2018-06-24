# -*- coding:utf-8 -*-
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import tweepy
import settings
import pymysql
import time

auth = tweepy.OAuthHandler(settings.TWITTER_APP_KEY, settings.TWITTER_APP_SECRET)
auth.set_access_token(settings.TWITTER_KEY, settings.TWITTER_SECRET)
api = tweepy.API(auth)


def get_user_list(index):
    if index == 1:
        sql = "SELECT * FROM user_need"
    else:
        sql = "SELECT * FROM user_need WHERE user_index = 0"
    cur.execute(sql)
    results = cur.fetchall()
    return results

def scroll(driver, user, tid):
    tweet_selector = 'li.js-stream-item'
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
            print('scrolling down to get more info %s' % (increment))
            found_tweets = driver.find_elements_by_css_selector(tweet_selector)
    except NoSuchElementException:
        print('no info on this user')

if __name__ == "__main__":
    conn = pymysql.connect(host='localhost', user='root', passwd='123456', db='twitter', port=3306, charset='utf8mb4')
    cur = conn.cursor()
    sql_info = "REPLACE INTO user_information(user_id, real_name, nick_name, user_created_time," \
               " follower_count, verified, description, tweets_count, location)VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    index = int(input(
        "Enter the method you want to get user information:\n1.get all user information    2. get new added user information    :"))
    user_list = get_user_list(index)
    index = 1
    for user in user_list:
        user_info = api.get_user(user[0])
        print("%d. Begin to get %s information" % (index, user[0]))
        params = (user_info.id, user_info.screen_name, user_info.name, user_info.created_at, user_info.followers_count,
                  user_info.verified, user_info.description, user_info.statuses_count, user_info.location)
        cur.execute(sql_info, params)
        conn.commit()
        index += 1

    conn.close()