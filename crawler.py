#!/usr/bin/python3
#coding=utf-8

from urllib import request
import pymysql
from bs4 import *
import datetime
import time
import jieba
import random

class DAO:
    def __init__(self):
        self.db = pymysql.connect(host="127.0.0.1",
                                  port=3306,
                                  user="root",
                                  password="920526",
                                  db="cnvcms",charset='utf8')
        self.con = self.db.cursor()
    def __del__(self):
        self.db.close()

    def getConnection(self):
        return self.con

    def selectOne(self,sql):
        self.con.execute(sql)
        return self.con.fetchone()

    def selectAll(self, sql):
        self.con.execute(sql)
        return self.con.fetchall()
    def insert(self,sql,args):
        self.con.execute(sql,args)
        self.db.commit()
    def execute(self,sql,args):
        rs=self.con.execute(sql,args)
        self.db.commit()
        return rs

colums_key = {
    "新闻":["新闻","实事","资讯"],
    "体育":["体育","NBA","CBA","中超","足球","篮球","乒乓球","欧冠","欧洲杯","亚冠","世界杯"],
    "娱乐":["娱乐","八卦","明星","花边","综艺",""],
    "":["","","","","",""],
    "":["","","","","",""]
}
class Crawler:
    def __init__(self):
        self.dao=DAO()
        self.count=0
        self.queue=[]
    def __init__(self,url,num):
        self.dao=DAO()
        self.count=0
        self.queue=[]
        self.queue.append(url)
        self.targetCount = num
        self.users = self.dao.selectAll("select * from t_user")
        channels = self.dao.selectAll("select * from t_channel")
        self.columns={}
        for ch in channels:
            self.columns[ch[1]]=ch[0]

        self.addsql = "insert into t_article (title,content,keywords,userId,channelId,createDate) values (%s,%s,%s,%s,%s,%s)"
    def getConnection(self):
        return self.dao.con
    def appendUrl(self,url):
        self.queue.append(url)
    def appendUrls(self, urls):
        self.queue.extend(urls)
    def nextUrl(self):
        if len(self.queue)==0:
            return None
        index = random.randint(0,len(self.queue)-1)
        return self.queue.pop(index)
    def parseLinks(self, page):
        links = page.find_all("a")
        for link in links:
            if "href" in link.attrs:
                url = link["href"]
                if url.endswith(".shtml"):
                    #print("enq:",url)
                    self.appendUrl(url)
    def getContent(self, page,url="none"):
        title = None
        texts = None
        #找到标题
        titdiv = page.find("div", attrs={"class": "yc_tit"})
        if (titdiv != None):
            title = titdiv.h1.string
            texts = page.select('div[class="yc_con_txt"] > p')
        #换一种方式解析标题
        else:
            tith1 = page.find("h1", attrs={"id": "artical_topic"})
            if (tith1 != None):
                title = tith1.string
                textdiv = page.find("div", attrs={"id": "main_content"})
                texts = textdiv("p")
        #如果标题仍为空
        if title == None or texts == None:
            #print("无法解析:", nexturl)
            return None

        # 删除最后一个<p>中的span
        pn = len(texts)
        if pn==0:
            print("解析错误:",url)
            print(texts)
            return  None

        text = texts[pn - 1]
        lastspan = text.span
        if lastspan != None:
            lastspan.extract()
        content=""
        for para in texts:
            content+=str(para)

        news={"title":title,"content":content}
        # print("title:", title)
        # print("Content: ", texts)
        return news

    def crawl(self):
        while self.count<self.targetCount:
            nexturl = self.nextUrl()
            if nexturl==None:
                break
            try:
                urlobj = request.urlopen(nexturl)
                page = BeautifulSoup(urlobj, "html.parser")
                news=self.getContent(page,nexturl)
            except:
                print("解析错误:", nexturl)
                continue
            self.parseLinks(page)
            if news!=None:

                cname = self.getChannelId(news)
                if cname == None:
                    cname="新闻"
                    #continue
                rs=self.saveArticle(news['title'], news['content'], news['channelId'])
                if rs:
                    self.count += 1
                    print("爬取 %s : %s\t: %s : %s" % (self.count, nexturl, cname, news['title']))
                #print(news)


    def saveArticle(self,title,content,channelId):

        #当前时间
        tnow = time.strftime('%Y-%m-%d %H:%M:%S')
        #随机用户id
        userlen=len(self.users)
        usel = random.randint(0,userlen-1)
        userId = self.users[usel][0]
        #标题分词作为keywords
        keywords_list = jieba.cut(title)
        #是否已存在
        rs=self.dao.execute("select * from t_article where title=%s",(title))
        if(rs>0):
            print("已存在,跳过")
            return False
        #插入
        # self.dao.insert(self.addsql,(title, content, "|".join(keywords_list),userId, channelId, tnow))
        return True
    def getChannelId(self,news):
        for colname in self.columns.keys():
            if news['title'].find(colname)>0 or news['content'].find(colname)>0:
                news["channelId"]=self.columns[colname]
                #print(colname)
                return  colname
        news["channelId"] = 1
        return  None

def test1(nexturl):
    #nexturl ="http://news.ifeng.com/a/20170617/51270614_0.shtml"
    #nexturl ="http://news.ifeng.com/a/20170618/51272367_0.shtml"
    urlobj = request.urlopen(nexturl)
    page = BeautifulSoup(urlobj.read().decode('UTF-8'), "html.parser")

    titdiv=page.find("div",attrs={"class":"yc_tit"})
    if(titdiv != None):
        title = titdiv.h1.string
        texts=page.select('div[class="yc_con_txt"] > p')

    tith1=page.find("h1",attrs={"id":"artical_topic"})
    if(tith1 != None):
        title=tith1.string
        textdiv=page.find("div",attrs={"id":"main_content"})
        texts=textdiv("p")
    if title==None or texts==None:
        print("无法解析:",nexturl)
    print("title:",title)
    #删除最后一个<p>中的span
    pn=len(texts)
    text=texts[pn-1]
    lastspan=text.span
    if lastspan!=None:
        lastspan.extract()
    t1 = str(texts)
    print("Content: ",t1)



url1="http://www.ifeng.com"
url2 ="http://news.ifeng.com/a/20170617/51270614_0.shtml"
url3="http://news.ifeng.com/a/20170619/51275097_0.shtml"
url4="http://finance.ifeng.com/"
url5="http://news.ifeng.com/mil/"
urls=["http://ent.ifeng.com/","http://sports.ifeng.com/","http://tech.ifeng.com/","http://finance.ifeng.com/","http://news.ifeng.com/mil/"]

crawl=Crawler(url1,500)
crawl.appendUrls(urls)
crawl.crawl()

# users=crawl.dao.selectAll("select * from t_user")
# columns=crawl.dao.selectAll("select * from t_channel")
# comments=crawl.dao.selectAll("select * from t_comment")

