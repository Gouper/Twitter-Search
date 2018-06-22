#! usr/bin/python3
# -*- coding: utf-8 -*-
import pymysql
import sys

conn = pymysql.connect(host='localhost', user='root', passwd='123456', db='twitter', port=3306, charset='utf8mb4')
cur = conn.cursor()
sql_replace = "REPLACE INTO remove_duplicate(created_time, message)VALUES (%s, %s)"
sheet = input("Enter the sheet you want to remove duplicate:")
index = int(input("Enter the method you want to remove duplicate:\n1. Export All     2. Export Some :"))
if index == 1:
    sql = "SELECT tweet_id, created_time, tweet_text FROM %s" % (sheet)
elif index == 2:
    start = input("Enter the start date: ")
    end = input("Enter the end date: ")
    sql = "SELECT tweet_id, created_time, tweet_text FROM %s WHERE created_time < '%s' AND created_time > '%s'" % (sheet, end, start)
else:
    print("Enter error! Program exit!")
    sys.exit()
cur.execute(sql)
results = cur.fetchall()
for i in results:
    params = (i[1], i[2])
    cur.execute(sql_replace ,params)
    conn.commit()
conn.close()

