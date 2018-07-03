#! usr/bin/python3
# -*- coding: utf-8 -*-
import pymysql
import sys

conn = pymysql.connect(host='localhost', user='root', passwd='123456', db='twitter', port=3306, charset='utf8mb4')
cur = conn.cursor()
sql_replace = "REPLACE INTO remove_duplicate_reply(primal_id, tweet_id, real_name, nick_name, created_time, tweet_text)VALUES (%s, %s, %s, %s, %s, %s)"
index = int(input("Enter the method you want to remove duplicate:\n1. remove duplicate all     2. remove duplicate some :"))
if index == 1:
    sql = "SELECT * FROM tweet_reply"
elif index == 2:
    start = input("Enter the start date: ")
    end = input("Enter the end date: ")
    sql = "SELECT * FROM tweet_reply WHERE created_time < '%s' AND created_time > '%s'" % (end, start)
else:
    print("Enter error! Program exit!")
    sys.exit()
cur.execute(sql)
results = cur.fetchall()
for i in results:
    params = (i[0], i[1], i[2], i[3], i[4], i[5])
    cur.execute(sql_replace, params)
    conn.commit()
conn.close()

