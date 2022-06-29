import requests
#import psycopg2
from bs4 import BeautifulSoup
#import databaseutils
import TextAndKeywordsProcess


class webscrapping:

    def get_soupdata(self, url):
        try:
            """
            get the soup data from webscraping from a url
            param: url string
            return: return <class 'bs4.BeautifulSoup'>
            """
            page = requests.get(url)
            soupdata = BeautifulSoup(page.content, 'html.parser')
        except Exception as error:
            print("Exception occured during get_soupdata!",error)
        return soupdata

    def get_scrapedlinks(self, url):
        try:
            """
            gets soup data and then scrape all links/<a> tags from soup data
            param: url
            return: list of links from the page scrapping
            """
            soupdata = self.get_soupdata(url)
            #print("URL in scraped links----",url)
            #print("soupdata in scraped links----", soupdata)
            links_list = soupdata.find_all('a')
            scrapedlinks = [url]
            for link in links_list:
                if 'href' in link.attrs:
                    if link.attrs['href'].startswith("/") and link.attrs['href'] != "/":
                        scrapedlink = url + str(link.attrs['href'])
                        if scrapedlink not in scrapedlinks:
                            scrapedlinks.append(scrapedlink)
        except Exception as error:
            print("Exception occured during get_scrapedlinks!",error)
        return scrapedlinks

    '''
    def getdunstablecount(self,conn):
        """
            connect to DB and get duns table count
            return: connecion
        """
        cur = conn.cursor()
        cur.execute("select count(*) from duns")
        dunsrecords_count = cur.fetchone()[0]
        return dunsrecords_count
    '''

    def get_dunsdata_from_db(self, cur):
        try:
            """
                connect to DB and get dunsnum, get dunsname and urls from duns table and remove / from the url if it ends with it
                return duns data into a dictionary with keys as autoincrement and value as list of id,name,url
            """
            cur.execute("select dunsnum, dunsname , dunsurl from duns")
            rows = cur.fetchall()
            dunsnums = []
            dunsnames = []
            dunsurls = []
            newurl = ""
            for data in rows:
                dunsnums = [data[0] for data in rows]
                dunsnames = [data[1] for data in rows]
                dunsurls = [data[2] for data in rows]

            new_duns_urls = []
            for url in dunsurls:
                if url.endswith("/"):
                    newurl = url[:len(url) - 1:]
                else:
                    newurl = url
            new_duns_urls.append(newurl)

            dunsdata = dict()
            for i in range(1, len(dunsnums) + 1):
                dunsdata[i] = [dunsnums[i - 1], dunsnames[i - 1], dunsurls[i - 1]]
        except Exception as error:
            print("Exception occured during get_dunsdata_from_db!",error)
        return dunsdata

    def removetags(self, soupdata):
        try:
            """
                removes script ans style tags from the soup data
                param: soupdata
                return: cleaned soupdata without tags
            """
            for data in soupdata(['style', 'script']):
                data.decompose()
            soupdata = ' '.join(soupdata.stripped_strings)
        except Exception as error:
            print("Exception occured during removetags!",error)
        return soupdata

    def createtemptable_webdata(self, cur):
        try:
            # DROP webdata TABLE IF EXISTS
            cur.execute("DROP TABLE IF EXISTS webdata")
            # CREATE webdata TABLE
            cur.execute("CREATE TABLE IF NOT EXISTS webdata (\
                id int NOT NULL GENERATED ALWAYS AS IDENTITY,\
                dunsnum text not NULL,\
                web_url text not null,\
                webdatatext text NOT NULL,\
                CONSTRAINT webdata_pkey PRIMARY KEY (id));")
        except Exception as error:
            print("Exception occured during createtemptable_webdata!",error)

    def inserttemptable_webdata(self, dunsnum, web_url, webdatatext, cur):
        try:
            cur.execute("INSERT INTO webdata (dunsnum,web_url,webdatatext) VALUES (%s, %s, %s)",
                        (dunsnum, web_url, webdatatext))
        except Exception as error:
            print("Exception occured during inserttemptable_webdata!",error)

    '''
    def selecttemptable_webdata(self,conn):
        cur = conn.cursor()
        cur.execute("select * from webdata")
        rows = cur.fetchall()
        return rows
    '''

    def performscrape(self, cur):
        try:
            #print("7.1")
            ws = webscrapping()
            #print("7.2")
            ws.createtemptable_webdata(cur)
            #print("7.3")
            dunsdata = ws.get_dunsdata_from_db(cur)
            #print("7.4")
            #print("len(dunsdata)-------",len(dunsdata))
            for i in range(1, len(dunsdata) + 1):
                url = dunsdata[i][2]
                #print("url--------",url)
                link_list = ws.get_scrapedlinks(url)
                #print("link_list------",link_list)
                for link in link_list:
                    # get soup data from all links
                    soupdata = ws.get_soupdata(link)
                    # clean soup data
                    soupdata = ws.removetags(soupdata)
                    #print(dunsdata[i][0],"-------",link,"-------",soupdata)
                    ws.inserttemptable_webdata(dunsdata[i][0], link, soupdata, cur)
                tkp = TextAndKeywordsProcess.TextAndKeywords()
                tkp.TextAndKeywordsProcessor(cur, dunsdata[i][0])
            #print("7.5")
        except Exception as error:
            print("Exception occured during perform!",error)