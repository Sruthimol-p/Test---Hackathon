import configparser
import os
import psycopg2


class databaseutils:

    conn = ""
    cur = ""

    def config_parser(self):
        try:
            config = configparser.ConfigParser()
            config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
            config.read(config_file)
        except Exception as error:
            print("Exception occured during config_parser!",error)
        return config

    def connecttodatabase(self):
        try:
            config = self.config_parser()
            database = config['DATABASE']
            self.host = database['host']
            self.port = database['port']
            self.dbname = database['dbname']
            self.user = database['user']
            self.password = database['password']
            self.conn = psycopg2.connect(host=self.host,port=self.port,dbname=self.dbname,user=self.user,password=self.password)
        except Exception as error:
            print("Exception occured during connecttodatabase!",error)
        return self.conn

    def getcursor(self, conn):
        try:
            self.cur = self.conn.cursor()
        except Exception as error:
            print("Exception occured during getcursor!",error)
        return self.cur

    def executeselectquery(self,conn,query):
        try:
            cur= self.getcursor(conn)
            cur.execute(query)
            rows = cur.fetchall()
        except Exception as error:
            print("Exception occured during executeselectquery!",error)
        return rows

    def executeinsertquery(self,cur,query):
        try:
            cur.execute(query)
        except Exception as error:
            print("Exception occured during executeinsertquery!",error)

    def executeupdatequery(self,cur,query):
        try:
            cur.execute(query)
        except Exception as error:
            print("Exception occured during executeupdatequery!",error)

    def executecreatequery(self,cur,query):
        try:
            cur.execute(query)
        except Exception as error:
            print("Exception occured during executecreatequery!",error)

    def droptable(self,cur,tablename):
        try:
            query = "DROP TABLE IF EXISTS "+ tablename
            cur.execute(query)
        except Exception as error:
            print("Exception occured during droptable!",error)

    def closedatabase(self, conn,cur):
        try:
            self.conn.commit()
            cur.close()
            conn.close()
        except Exception as error:
            print("Exception occured during closedatabase!",error)
