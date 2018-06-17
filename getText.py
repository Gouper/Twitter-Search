#! usr/bin/python3
# -*- coding: utf-8 -*-
import pymysql
import sys

conn = pymysql.connect(host='localhost', user='root', passwd='123456', db='twitter', port=3306, charset='utf8mb4')
cur = conn.cursor()

sheet = input("Enter the sheet you want to export:")
index = int(input("Enter the method you want to export:\n1. Export All     2. Export Some :"))
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
    fname = i[1]
    fname = fname.replace(" ", "_")
    fname = fname.replace("-", "_")
    fname = fname.replace(":", "_")
    fname = fname + '_' + i[0] + '.txt'
    #change dir name
    fname = 'e:\\ddd\\' + fname
    fobj = open(fname, 'a', encoding='utf-8')
    fobj.write(i[2])