#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Driver.
生成任务队列。
轮休worker池，如果有空闲的woker。那么设置任务
如果没有，等待。

设置任务：
设置url。设置反馈，开始。

锁:
询问，设置，通知
'''

from JWorker import Woker, WorkerStatus
import time

def showUrl(param, content):
    print "content is ", content[:30]

# 处理任务。这里的任务可能动态变化
def assigningDynamicTask(workerPool, getTask, callback):
    while True:
        task = getTask()
        # 如果任务没有了，等待所有worker处理完毕
        if task == None:
            isAllIdle = True
            for w in workerPool:
                if w.isBusy():
                    isAllIdle = False
                    break
            if isAllIdle:
                break
            else:
                time.sleep(1)
                continue

        # 找一个空闲的worker处理task。如果没有，循环等待
        notFindIdleWorker = True
        while notFindIdleWorker:
            for w in workerPool:
                if w.isIdle():
                    w.schedule(task, callback)
                    notFindIdleWorker = False
                    break

            if notFindIdleWorker:
                time.sleep(1)

    print "wait take finish"
    # 上面已经判断是否处理完毕了。这还继续判断？多余否？
    haveFinish = False
    while not haveFinish:
        haveFinish = True
        for w in workerPool:
            if w.isBusy():
                haveFinish = False
                time.sleep(1)

    print "finished this take"

# 处理任务。任务不会变化
def assigningTask(workerPool, tasks, callback):

    total = len(tasks)
    count = 0
    for i in tasks:
        count += 1
        print "total Tasks", total, "now", count
        notFindIdleWorker = True
        while notFindIdleWorker:
            for w in workerPool:
                if w.isIdle():
                    w.schedule(i, callback)
                    notFindIdleWorker = False
                    break

            if notFindIdleWorker:
                time.sleep(1)

    print "wait take finish"
    haveFinish = False
    while not haveFinish:
        haveFinish = True
        for w in workerPool:
            if w.isBusy():
                haveFinish = False
                time.sleep(1)

    print "finished this take"


def initWorkerPool(number):
    workerPool = []
    for i in range(number):
        w = Woker(str(i))
        workerPool.append(w)

    time.sleep(1)
    return workerPool

def finishPool(workerPool):
    print "Finish"
    for w in workerPool:
        w.finish()

if __name__ == "__main__":

    workerPool = initWorkerPool(2)

    task = [(1, "https://www.douban.com"), (2, "https://www.baidu.com"), (3, "https://www.zhihu.com")]
    #task = [(1, "https://www.baidu.com"), (2, "https://www.baidu.com"), (3, "https://www.baidu.com")]


    assigningTask(workerPool, task, showUrl)

    finishPool(workerPool)
