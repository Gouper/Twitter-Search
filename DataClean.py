# -*- coding: utf-8 -*-
import re


def dataclean(message):
    message = message.replace('…', '')
    pattern = re.compile(r'@\S*\s|@\S*|http://\S*\s|http://\S*|https://\S*|https://\S*\s|pic\.twitter\.com/\S*')
    c = pattern.search(message)
    while c:
        aa = c.span()
        message = message[:aa[0]] + message[aa[1]:]
        c = pattern.search(message)
    message = message.strip()
    return message