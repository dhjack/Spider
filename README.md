# 抓取豆瓣数据的爬虫
开始做一个推荐系统，是基于节目内容推荐的。所以抓取了豆瓣的电影数据。
1. 获取豆瓣的所有的标签
    * 豆瓣标签url：`https://movie.douban.com/tag/`
    * 提取标签的正则表达式：`r'href="/tag/([^<]*)">'`
2. 进入每一个标签页，提取节目的id，保存数据库
    * 具体的标签页面；`https://movie.douban.com/tag/{?}`
    * 正则表达式：`r'href="https://movie.douban.com/subject/(\d*)/"')`

后来增加基于行为推荐。需要继续抓取用户行为，这里主要是评分。
1. 基于已经抓取的节目id。获取节目的评论用户
    * 每个节目的评论页：`"https://movie.douban.com/subject/%d/comments" % pid`
    * 提取用户的正则表达式：`r'href="https://www.douban.com/people/([^/]*)/"'`
2. 抓取用户的评论个数
    * 用户的评论信息页面：`"https://movie.douban.com/people/%s/" % uid`
    * 获取评论个数的正则表达式：`r'>(\d+)部<'`
3. 只保留评论个数大于100的用户
4. 对剩下的“高质量”的用户，抓取评分信息
    * 用户的评分信息页面：`"https://movie.douban.com/people/%s/collect?sort=time&amp;start=0&amp;filter=all&amp;mode=list&amp;tags_sort=count" % uid`
    * 提取评分的正则表达式：`r'<div class="title">(?:.|\n)*?<a href="https://movie.douban.com/subject/(\d+)/">(?:.|\n)*?</a>(?:.|\n)*?</div>(?:.|\n)*?<div class="date">\n\s*<span class="rating(\d)-t"></span>'`
    * 提取下一页的正则表达式：`r'rel="next" href="([^"]*)">'`

