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
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pymysql


def scroll(driver, start_date, end_date, words, lang):
	languages = { 1: 'en', 2: 'it', 3: 'es', 4: 'fr', 5: 'de', 6: 'ru', 7: 'zh'}
	tweet_selector = 'li.js-stream-item'
	url = "https://twitter.com/search?f=tweets&q="
	url += "{}%20".format(words)
	url += "since%3A{}%20until%3A{}&".format(start_date, end_date)
	if lang != 0:
		url += "l={}&".format(languages[lang])
	url += "src=typd"
	print(url)
	driver.get(url)
	# start_time = time.time()  # remember when we started
	try:
		found_tweets = driver.find_elements_by_css_selector(tweet_selector)
		increment = 0
		while len(found_tweets) > increment:
			driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
			time.sleep(2)
			increment = len(found_tweets)
			print('scrolling down to load more tweets %s' % (increment))
			found_tweets = driver.find_elements_by_css_selector(tweet_selector)
	except NoSuchElementException:
		print('no tweets on this day')

def scroll2(driver, start_date, end_date, words, lang, max_tweets):
	languages = {1: 'en', 2: 'it', 3: 'es', 4: 'fr', 5: 'de', 6: 'ru', 7: 'zh'}
	tweet_selector = 'li.js-stream-item'
	url = "https://twitter.com/search?f=tweets&q="
	url += "{}%20".format(words)
	url += "since%3A{}%20until%3A{}&".format(start_date, end_date)
	if lang != 0:
		url += "l={}&".format(languages[lang])
	url += "src=typd"
	print(url)
	driver.get(url)
	# start_time = time.time()  # remember when we started
	try:
		found_tweets = driver.find_elements_by_css_selector(tweet_selector)
		increment = 0
		while len(found_tweets) < max_tweets:
			driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
			time.sleep(2)
			increment = len(found_tweets)
			print('scrolling down to load more tweets %s' % (increment))
			found_tweets = driver.find_elements_by_css_selector(tweet_selector)
			if increment == len(found_tweets):
				break
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
		uid = []
		tid = []
		real_name = []
		for i in content:
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
			tweet_id = i.find_all("a", class_="tweet-timestamp")[0].get("data-conversation-id")

			tweets = i.find("p", class_="tweet-text").strings
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
		tweet_list.append([data['tweet_id'][i], data['user_id'][i], data['real_name'][i], data['nick_name'][i], data['date'][i], data['tweet'][i]])
	cur.executemany(sql1, tweet_list)
	conn.commit()

def make_csv(data):
	l = len(data['date'])
	print("count: %d" % l)
	with open("twitterData.csv", "w+") as file:
		fieldnames = ['Date', 'Name', 'Tweets']
		writer = DictWriter(file, fieldnames=fieldnames)
		writer.writeheader()
		for i in range(l):
			data['name'][i] = data['name'][i].replace(u'\xa0', u' ')
			data['tweet'][i] = data['tweet'][i].replace(u'\xa0', u' ')
			writer.writerow({'Date': data['date'][i],
							'Name': data['name'][i],
							'Tweets': data['tweet'][i],
							})

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
	sql1 = "REPLACE INTO search_tweet(tweet_id, user_id, real_name, nick_name, created_time, tweet_text)VALUES (%s, %s, %s, %s, %s, %s)"
	conn = pymysql.connect(host='localhost', user='root', passwd='123456', db='twitter', port=3306, charset='utf8mb4')
	cur = conn.cursor()

	wordsToSearch = input("Enter the words: ")
	lang = int(input("0) All Languages 1) English | 2) Italian | 3) Spanish | 4) French | 5) German | 6) Russian | 7) Chinese\nEnter the language you want to use: "))
	start_date = input("Enter the start date in (Y-M-D): ")
	end_date = input("Enter the end date in (Y-M-D): ")
	choose = int(input("input what kind of method you want to get:\n1).the day you want get some tweets   2).the day you want get all tweets"))
	all_dates = get_all_dates(start_date, end_date)
	if choose == 1:
		max_tweets = int(input("Enter the maximum tweets number that every day you will collect:"))
		driver = webdriver.Chrome()
		for i in range(len(all_dates) - 1):
			scroll2(driver, str(all_dates[i]), str(all_dates[i + 1]), wordsToSearch, lang, max_tweets)
			scrape_tweets(driver)
			time.sleep(3)
			print("The tweets for {} are ready!".format(all_dates[i]))
		driver.quit()
	elif choose == 2:
		driver = webdriver.Chrome()
		for i in range(len(all_dates) - 1):
			scroll(driver, str(all_dates[i]), str(all_dates[i + 1]), wordsToSearch, lang)
			scrape_tweets(driver)
			time.sleep(3)
			print("The tweets for {} are ready!".format(all_dates[i]))
		driver.quit()

	conn.close()
