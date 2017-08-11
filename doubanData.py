#!/usr/bin/env python
# -*- coding: utf-8

# 抓取节目信息

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
import config

import faulthandler, signal
faulthandler.register(signal.SIGUSR1)

host = config.db['host']
user = config.db['user']
passwd = config.db['passwd']

# 获取豆瓣所有的tag
def getAllTages():
	tagUrl = "https://movie.douban.com/tag/"
	pattern = re.compile(r'href="/tag/([^<]*)">')
	return pattern.findall(anonymousRequest(tagUrl))

# 获取某一类标签下，节目的id
def getOnePageMovieId(tag):
        print tag
	doubanUrlPrefix = "https://movie.douban.com/tag/"
	pattern = re.compile(r'href="https://movie.douban.com/subject/(\d*)/"')
        return pattern.findall(anonymousRequest(doubanUrlPrefix+urllib.quote(tag)))

# 根据节目id获取节目信息，并保存数据库
def getMovieInfo(mid):
        prefix = "http://api.douban.com/v2/movie/subject/"
        info = json.loads(anonymousRequest(prefix+urllib.quote(mid)))

        db = MySQLdb.connect(host, user, passwd, "reSystem", charset="utf8")
        cursor = db.cursor()      
        print mid
        print info['title']
        print info['images']['large']
        print info['summary']
        
        cursor.execute("SET NAMES 'utf8'")
        try:
            sql = 'insert into `movies` values (%s, \'%s\', \'%s\', \'%s\')' % (mid, info['title'], info['images']['large'], info['summary'])
            print sql
            cursor.execute(sql)
        except BaseException, e:
            print e
            db.rollback()

        db.commit()
        db.close()

# 测试读取数据库的数据。主要是编码问题。保存的时候，最好是原样unicode编码保存。不然倒腾几次后，各种乱码问题。还有引号，双引号
# 发现了这种编码和引号问题。但是不好重新去爬数据。耗时。所以做了进一步的处理。把双引号替换为单引号，然单引号替换为转移单引号
def testMovieInfo():

        db = MySQLdb.connect(host, user, passwd, "reSystem", charset="utf8")
        cursor = db.cursor()      
        cursor.execute("SET NAMES 'utf8'")

	cursor.execute("select * from moviesJsonInfo where pid = 4810419")
        data = cursor.fetchall()

        print "data len ", len(data)

        needFixIds = []
        pattern = re.compile(r'"summary":\s*"(.*)",\s*"subtype"',re.S)
        pattern2 = re.compile(r'"original_title":\s*"(.*)",\s*"summary"',re.S)
        for pid, info in data:
            try:
                pInfo = json.loads(info, strict=False)
                pInfo['title']
                pInfo['images']['large']
                pInfo['summary']
                print info
            except BaseException, e:
                needFixIds.append((pid, info))
                #sys.exit(0)
        print "len(needfix)", len(needFixIds)
        pattern3 = re.compile(r'"aka":\s*\[(.*)\]}')
        pattern4 = re.compile(r'"title":\s*"(.*)",\s*"do_count"')
        def akaReplace(c):
            c=c[1:-1]
            return '"' + c.replace('"', "'") + '"'
        for pid, info in needFixIds:
            try:
                m2 = pattern2.search(info)
                if m2:
                   newInfo = info[:m2.start()] + u'"original_title":"' + m2.group(1).replace('"', r"'") + u'","summary"' + info[m2.end():]

                m = pattern.search(newInfo)
                if m:
                   newInfo2 = newInfo[:m.start()] + u'"summary":"' + m.group(1).replace('"', r"'") + u'","subtype"' + newInfo[m.end():]

                m = pattern3.search(newInfo2)
                if m:
                    newInfo3 = newInfo2[:m.start()] + u'"aka":[' + ", ".join(map(akaReplace, m.group(1).split(", "))) + u']}' + newInfo2[m.end():]
                m = pattern4.search(newInfo3)
                if m:
                    newInfo4 = newInfo3[:m.start()] + u'"title":"' + m.group(1).replace('"', r"'") + u'","do_count"' + newInfo3[m.end():]
                pInfo = json.loads(info, strict=False)
                pInfo['title']
                pInfo['images']['large']
                pInfo['summary']
                sql = r'update `moviesJsonInfo` set info = %s where pid = %s' % ("'"+info.replace("'", r"\'")+"'", pid)
                print sql
                try:
                    cursor.execute(sql)
                    db.commit()
                except BaseException, e:
                    print "Error2:",e
                    db.rollback()
            except BaseException, e:
                print "Error:",e

        db.close()


        
# 一分钟更新一次
lastTime = 0
proxys = []
# 获取最新的代理ip
def getProxy():
    global lastTime, proxys
    if time.time() - lastTime > 1 * 60:
        print "upate proxys "
        lastTime = time.time()
        request = urllib2.Request('http://www.xicidaili.com/nn/')
        request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36')  
        response = urllib2.urlopen(request)
        pattern = re.compile(r'<td>([\d.]*)</td>\s*<td>(\d*)</td>')
        proxys = map(lambda x:x[0]+':'+x[1], pattern.findall(response.read()))
    return random.choice(proxys)

# 封装了一个请求url的函数。内部调用代理。
def anonymousRequest(url):
    needTry = True
    content = ""
    while needTry:
        proxy = getProxy()
        print  time.asctime( time.localtime(time.time()) ),
        print "proxy is ", proxy
        time.sleep(1)
        # 这里是个坑。如果是http代理，需要'http'字段
        # 如果是https代理，需要的是'https'字段
        # 开始不知道，弄的https代理一直没生效。一直纠结为什么加了代理还是被反爬了/(ㄒoㄒ)/~~
        proxy_handler = urllib2.ProxyHandler({'http': proxy}) 
        opener = urllib2.build_opener(proxy_handler)  
        opener.addheaders = [('User-Agent','Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'), ("Referer", "www.baidu.com")]
        try:
            # 还有各种超时，可能是一种反爬手段。就是一直卡住不放，也不断开连接。导致爬虫走不下去。所以要加超时
            f = opener.open(url, timeout=4)  
            content = f.read()
            needTry = False
        except BaseException, e:
            print "req error", e
    return content

def testS5():
	#tagUrl = "https://www.google.com"
	tagUrl = "http://api.douban.com/v2/movie/subject/6873143"
	proxy_handler = urllib2.ProxyHandler({'http': '106.46.136.172:808'})  
	opener = urllib2.build_opener(proxy_handler)  
        opener.addheaders = [('User-Agent','Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'), ("Referer", "www.baidu.com")]
	f = opener.open(tagUrl)  
	print f.read()
    
def getDBInfo():
	db = MySQLdb.connect(host, user, passwd, "reSystem", charset="utf8")
	cursor = db.cursor()      

	cursor.execute("select * from movies")
        data = cursor.fetchall()
        for i in data:
            for ii in i:
                print ii

        db.close()

# 提前已经获取的有效的节目id
def getAllMovieIds():
    db = MySQLdb.connect(host, user, passwd, "reSystem", charset="utf8")
    cursor = db.cursor()      

    cursor.execute("select distinct pid from userRates")
    data = cursor.fetchall()
    print "need movies info's count", len(data)

    cursor.execute("select pid from moviesJsonInfo")
    havedIds = cursor.fetchall()
    print "haved movies info's count", len(havedIds)

    cursor.execute("select pid from moviesInvalid")
    invalidIds = cursor.fetchall()
    print "invalid movies info's count", len(invalidIds)

    db.close()
    return set(map(lambda x:x[0], data)) - set(map(lambda x:x[0], havedIds)) - set(map(lambda x:x[0], invalidIds))

def saveToDB(pid, info):
    db = MySQLdb.connect(host, user, passwd, "reSystem", charset="utf8")
    cursor = db.cursor()      

    cursor.execute("SET NAMES 'utf8'")
    sql = ''
    try:
        if info == "":
            sql = 'insert into `moviesInvalid` values (%s)' % (pid)
        else:
            sql = 'insert into `moviesJsonInfo` values (%s, \'%s\')' % (pid, info.decode("unicode_escape").replace("'", r"\'"))
        cursor.execute(sql)
    except BaseException, e:
        print "save error sql :", sql
        print "save error pid:", pid
        print "save error len(info) :", len(info)
        print "save error:", e
        db.rollback()

    db.commit()
    db.close()

def maxRatings(movies):
    print "评分最高的"
    for i in sorted(movies.items(), key=lambda x:x[1][1], reverse=True)[:99]:
      print i[0]
      print i[1]

def maxYear(movies):
    print "最新的"
    for i in sorted(movies.items(), key=lambda x:x[1][0], reverse=True)[:9]:
      print i[0]
      print i[1]

def maxVies(movies):
    print "最多人看的"
    for i in sorted(movies.items(), key=lambda x:x[1][2], reverse=True)[:9]:
      print i[0]
      print i[1]

def testMovieInfo2():
        db = MySQLdb.connect(host, user, passwd, "reSystem", charset="utf8")
        cursor = db.cursor()      
        cursor.execute("SET NAMES 'utf8'")

	cursor.execute("select * from moviesJsonInfo")
        data = cursor.fetchall()

        print "data len ", len(data)

        movies = {}
        for pid, info in data:
            try:
                pInfo = json.loads(info, strict=False)
                #print pInfo['title']
                pInfo['summary']
                movies[pInfo['title']] = [pInfo["year"],pInfo['rating']['average'], pInfo['ratings_count']]
            except BaseException, e:
                print e
                sys.exit(0)
        db.close()
        maxRatings(movies)
        #maxYear(movies)
        #maxVies(movies)


if __name__ == "__main__":

    #testMovieInfo2()
    #sys.exit(0)
    #getMovieInfo("3285632")
    #anonymousRequest("https://www.baidu.com")
    #anonymousRequest("http://api.douban.com/v2/movie/subject/6873143")
    #getDBInfo()
    #sys.exit(0)
    # 1 get all tag
    # for each tag. get first page
#    mIds = set()
#    for tag in getAllTages():
#    #   match url and get id. save to Set
#         mIds.update(getOnePageMovieId(tag))
#
#    for mid in mIds:
#        getMovieInfo(mid)
    # get all movie id

    tasks = []
    # JDriver是封装的一个多线程的爬虫库。不断的取任务，然后通过回调函数反馈。
    workerPool = JDriver.initWorkerPool(1)
    movieIds = getAllMovieIds()
    print "actual need movies info's count", len(movieIds)
    for pid in movieIds:
        infoUrl = "http://api.douban.com/v2/movie/subject/%d" % pid
        tasks.append((pid, infoUrl))
    print tasks[:1]

    JDriver.assigningTask(workerPool, tasks, saveToDB)
    JDriver.finishPool(workerPool)
