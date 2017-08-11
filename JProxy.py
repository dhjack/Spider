#!/usr/bin/env python
# -*- coding: utf-8

import time
import random
import urllib2
import re
import threading
import requests
from requests.exceptions import HTTPError

lastTime = 0
proxys = set()
lock = threading.Lock()

def getProxy():
    global proxys
    with lock:
        if len(proxys) == 0:
            print  time.asctime( time.localtime(time.time()) ),
            print "upate proxys "
            request = urllib2.Request('http://www.xicidaili.com/nn/')
            request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36')  
            try :
                response = urllib2.urlopen(request)
                pattern = re.compile(r'<td>([\d.]*)</td>\s*<td>(\d*)</td>')
                proxys.update(map(lambda x:x[0]+':'+x[1], pattern.findall(response.read())))
                response.close()
            except BaseException, e:
                print "get proxys", e
        p = random.choice(list(proxys))
        return p

def getProxyDaxiang():
    global proxys
    with lock:
        if len(proxys) == 0:
            print  time.asctime( time.localtime(time.time()) ),
            print "upate proxys "
            #request = urllib2.Request('http://tvp.daxiangdaili.com/ip/?tid=559493766107456&num=1000&operator=1&category=2&protocol=https')
            #request = urllib2.Request('http://tvp.daxiangdaili.com/ip/?tid=559493766107456&num=1&operator=1&delay=5&category=2&protocol=https')
            request = urllib2.Request('http://tvp.daxiangdaili.com/ip/?tid=559493766107456&num=1&operator=1&delay=5&category=2&protocol=https')
            request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36')  
            try :
                response = urllib2.urlopen(request)
                proxys.update(response.read().split("\r\n"))
                response.close()
            except BaseException, e:
                print "daxiang proxy", e
        p = random.choice(list(proxys))
        return p

def removeProxy(proxy):
    global proxys
    with lock:
        if proxy in proxys:
            proxys.remove(proxy)

def anonymousRequest(url):
    needTry = True
    content = ""
    while needTry:
        #curProxy = getProxy()
        curProxy = ""
        #curProxy = getProxyDaxiang()
        #print  time.asctime( time.localtime(time.time()) ),
        #print "proxy is ", curProxy
        time.sleep(10)
        #proxy_handler = urllib2.ProxyHandler({'https': "60.3.178.128:8998"}) 
        #proxy_handler = urllib2.ProxyHandler({'https': "119.254.84.90:80"}) 
        #proxy_handler = urllib2.ProxyHandler({'https': curProxy}) 
        #opener = urllib2.build_opener(proxy_handler)  
        #opener.addheaders = [('User-Agent','Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'), ("Referer", "www.baidu.com")]
        pretentHeader = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}
        try:
            print "jack in"
            #f = opener.open(url, timeout=4) 
            #content = f.read()
            #f.close()
            #r = requests.get(url, timeout=(3, 6), proxies={'https': curProxy}, headers=pretentHeaders)
            #r = requests.get(url, timeout=(6, 9), proxies={'http': curProxy}, headers=pretentHeader)
            r = requests.get(url, timeout=(6, 9), headers=pretentHeader)
            r.raise_for_status()
            print "jack", r.status_code, url, curProxy
            if r.status_code != 404:
                if r.text.find("<h1>Unauthorized ...</h1>") != -1 or r.text.find("<h1>Web site unauthorized...</h1>") != -1:
                    raise "proxy maybe invalid", curProxy
                content = r.text
            needTry = False
        except HTTPError, e:
            if r.status_code == 404:
                content = ""
                needTry = False
            elif r.status_code == 400:
                time.sleep(1800)
            else:
                print "Thread", threading.currentThread().getName(), e
        except BaseException, e:
            print "Thread", threading.currentThread().getName(), e
            pass
            #removeProxy(curProxy)
    return content
