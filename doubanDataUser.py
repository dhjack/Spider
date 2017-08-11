#!/usr/bin/env python
# -*- coding: utf-8

# 抓取对节目评论过的用户

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

lock = threading.Lock()

def getAllMovieIds():
    db = MySQLdb.connect(host, user, passwd, "reSystem", charset="utf8")
    cursor = db.cursor()      

    cursor.execute("select pid from movies")
    data = cursor.fetchall()
    db.close()
    return map(lambda x:x[0], data)

def checkUser(pid, context):
    pattern = re.compile(r'href="https://www.douban.com/people/([^/]*)/"')
    lock.acquire()
    userIds.update(set(pattern.findall(context)))
    lock.release()

# 获取节目的评论用户
def getCommitUsers(pid):
    commUrl = "https://movie.douban.com/subject/%d/comments" % pid
    print commUrl
    pattern = re.compile(r'href="https://www.douban.com/people/([^/]*)/"')
    return set(pattern.findall(JProxy.anonymousRequest(commUrl)))

def writeToDB(uid, count):
    db = MySQLdb.connect(host, user, passwd, "reSystem", charset="utf8")
    cursor = db.cursor()      

    cursor.execute("SET NAMES 'utf8'")
    try:
        sql = 'insert into `userIds` values (\'%s\', %d)' % (uid, count)
        #print sql
        cursor.execute(sql)
    except BaseException, e:
        print e
        db.rollback()

    db.commit()
    db.close()

# 只记录评论数量大于100的用户
def cbWriteToDBIfSatisfy(uid, context):
    pattern = re.compile(r'>(\d+)部<')
    matched = pattern.findall(context)
    if len(matched > 0):
        watched = int(matched[0])
        if watched > 100:
            writeToDB(uid, watched)

def writeToDBIfSatisfy(uid):
    userUrl = "https://movie.douban.com/people/%s/" % uid
    print userUrl
    pattern = re.compile(r'>(\d+)部<')
    watched = int(pattern.findall(JProxy.anonymousRequest(userUrl))[0])
    if watched > 100:
        writeToDB(uid, watched)

if __name__ == "__main__":
    # get all movie id
    movieIds = getAllMovieIds()
    totalMovieIds = len(movieIds)
    userIds = set()
    current = 0
    workerPool = JDriver.initWorkerPool(50)
    # 先获取已经抓取的节目id，然后提取评论用户
    tasks = []

    for pid in movieIds:
        commUrl = "https://movie.douban.com/subject/%d/comments" % pid
        tasks.append((pid, commUrl))

    JDriver.assigningTask(workerPool, tasks, checkUser)

    # 再获取每个用户的评论数量
    taskU = []
    for uid in userIds:
        userUrl = "https://movie.douban.com/people/%s/" % uid
        taskU.append((uid, userUrl))

    JDriver.assigningTask(workerPool, taskU, cbWriteToDBIfSatisfy)

    JDriver.finishPool(workerPool)

