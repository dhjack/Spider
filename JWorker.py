#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading
import JProxy

'''
Woker类
获取任务，也就是url
设置反馈函数
开始处理

'''

class WorkerStatus:
    # 空闲
    Idle = 0
    # 处理任务中
    Busy = 1
    # 结束
    Done = 2

class Woker(threading.Thread):
    '''
    封装了一个线程。处理实际的任务。
    '''

    def __init__(self, name):
        threading.Thread.__init__(self)
        self.status = WorkerStatus.Idle
        self.url = None
        self.callback = None
        self.lock = threading.Lock()
        self.condition = threading.Condition()
        self.name = name
        self.param = None
        self.start()
        print "init Worker", self.name

    def setStatus(self, status):
        self.lock.acquire()
        self.status = status
        self.lock.release()

    def startJob(self):
        content = JProxy.anonymousRequest(self.url)
        try:
            self.callback(self.param, content)
        except BaseException, e:
            print "bac content:", content
            import pdb
            pdb.set_trace()
            print e

    def finish(self):
        with self.condition:
            self.setStatus(WorkerStatus.Done)
            self.condition.notify()

    def checkStatus(self, status):
        self.lock.acquire()
        flag = status == self.status
        self.lock.release()
        return flag

    def isIdle(self):
        return self.checkStatus(WorkerStatus.Idle)

    def isBusy(self):
        return self.checkStatus(WorkerStatus.Busy)

    def isDone(self):
        return self.checkStatus(WorkerStatus.Done)

    def schedule(self, task, callback):
        print "set url ", task, "for", self.name
        self.param, self.url = task
        self.callback = callback
        with self.condition:
            self.setStatus(WorkerStatus.Busy)
            self.condition.notify()

    def run(self):
        while True:
            with self.condition:
                self.condition.wait()
                if self.isDone():
                    break
                else:
                    self.startJob()
                    self.setStatus(WorkerStatus.Idle)
