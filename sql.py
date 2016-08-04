#coding=utf-8
import pdb
import MySQLdb
import json
import requests

class SQL:
    def __init__(self, host='127.0.0.1', user='root', passwd='', db='EVAL', charset='utf8'):
        self._host = host
        self._user = user
        self._passwd = passwd
        self._db = db
        self._charset = charset

    def execute_query(self, sqltext):
        conn = None
        try:
            conn = MySQLdb.Connect(host=self._host, user= self._user, passwd= self._passwd, db= self._db, charset=self._charset)
            cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            cur.execute(sqltext)
            result = cur.fetchall()
            return result
        finally:
            if conn:
                pdb.set_trace()
                conn.close()

    
    def execute_update(self, sqltext):
        conn = None
        try:
            conn = MySQLdb.Connect(host=self._host, user=self._user, passwd=self._passwd, db=self._db, charset=self._charset)
            cur = conn.cursor()
            cur.execute(sqltext)
            return conn.insert_id()
        finally:
            if conn:
                conn.close()

    def dump2file(self, sqltext, filename, delimiter='!@#$'):
        results = self.execute_query(sqltext)
        import codecs
        if results:
            with codecs.open(filename, 'w', encoding='utf8') as f:
                for res in results:
                    out_str = delimiter.join(['%s'%(value) for value in res.values()]) + '\n'
                    f.write(out_str)

if __name__ == '__main__':

    mysql = SQL()
    sqltext = "select * from df_abstract"
    filename = "out"
    mysql.dump2file(sqltext, filename)
