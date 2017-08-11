#!/usr/bin/env python
# -*- coding: utf-8

# 抓取用户的评分信息

import sys
import time
import urllib
import urllib2
import re
import json 
import MySQLdb
import random
import JProxy
import JDriver
import threading
import config

import faulthandler, signal
faulthandler.register(signal.SIGUSR1)

host = config.db['host']
user = config.db['user']
passwd = config.db['passwd']
tasks = []
lock = threading.Lock()

def getAllUserIds():
    db = MySQLdb.connect(host, user, passwd, "reSystem", charset="utf8")
    cursor = db.cursor()      

    cursor.execute("select uid from userIds order by count desc limit 3000")
    data = cursor.fetchall()
    db.close()
    return map(lambda x:x[0], data)

# 获取用户对节目的评分，并翻页
def getUserGradeAndNext(uid, content):
    # capture current page's movie and grade
    pattern = re.compile(r'<div class="title">(?:.|\n)*?<a href="https://movie.douban.com/subject/(\d+)/">(?:.|\n)*?</a>(?:.|\n)*?</div>(?:.|\n)*?<div class="date">\n\s*<span class="rating(\d)-t"></span>')
    rates = pattern.findall(content)
    # TODO write db
    for pid, rate in rates:
        writeToDB(uid, pid, rate)
    # if have next. goto next
    nextPagePattern = re.compile(r'rel="next" href="([^"]*)">')
    nextpage = nextPagePattern.findall(content)
    if len(nextpage) > 0:
        lock.acquire()
        tasks.append((uid, nextpage))
        lock.release()

# 貌似没用了
def getUserGrade(gradeUrl):
    print gradeUrl
    content = JProxy.anonymousRequest(gradeUrl)
    # capture current page's movie and grade
    pattern = re.compile(r'<div class="title">(?:.|\n)*?<a href="https://movie.douban.com/subject/(\d+)/">(?:.|\n)*?</a>(?:.|\n)*?</div>(?:.|\n)*?<div class="date">\n\s*<span class="rating(\d)-t"></span>')
    rates = pattern.findall(content)
    # 可能没有评分
    print rates
    # writeToDB(rates)
    # if have next. goto next
    nextPagePattern = re.compile(r'rel="next" href="([^"]*)">')
    nextpage = nextPagePattern.findall(content)
    if len(nextpage) > 0:
        getUserGrade(nextpage)

def writeToDB(uid, pid, rate):
    db = MySQLdb.connect(host, user, passwd, "reSystem", charset="utf8")
    cursor = db.cursor()      

    cursor.execute("SET NAMES 'utf8'")
    try:
        sql = 'insert into `userRates` values (\'%s\', %s, %s)' % (uid, pid, rate)
        #print sql
        cursor.execute(sql)
    except BaseException, e:
        print e
        db.rollback()

    db.commit()
    db.close()

def getCurrentTask():
    lock.acquire()
    task = len(tasks) > 0 and tasks.pop() or None
    lock.release()
    return task

if __name__ == "__main__":
    #request = urllib2.Request("https://www.douban.com")
    #print urllib2.urlopen(request).read()

    #print JProxy.anonymousRequest("https://www.douban.com")[:100]
#    count = 0
#    while count < 2:
#        print JProxy.anonymousRequest("http://www.baidu.com/")[:100]
#        #request = urllib2.Request("https://www.baidu.com/")
#        #print urllib2.urlopen(request).read()[:10]
#        count += 1
#        print "count: ", count
#    sys.exit(0)
    # get all uid
    userIds = getAllUserIds()
    workerPool = JDriver.initWorkerPool(30)

    for uid in userIds:
        gradeUrl = "https://movie.douban.com/people/%s/collect?sort=time&amp;start=0&amp;filter=all&amp;mode=list&amp;tags_sort=count" % uid
        tasks.append((uid, gradeUrl))

    JDriver.assigningDynamicTask(workerPool, getCurrentTask, getUserGradeAndNext)

    JDriver.finishPool(workerPool)
