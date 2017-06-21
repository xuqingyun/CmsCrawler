#!/usr/bin/python3

import pymysql
class DAO:
    def __init__(self):
        self.db = pymysql.connect(host="121.248.54.247",
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

def getDao():
    return DAO()

#print("main test")
# dao = DAO()
# sql = "select * from t_user"
# data = dao.selectAll(sql)
# #for d in data:
# #   print("data ",d[0],": ",d)
#
# con = dao.getConnection()
# con.execute(sql)
# d1=con.fetchone()
# d2=con.fetchone()
# print(d1,d2)
