import webscrapping
import databaseutils

class mainrun:

    global conn
    global cur

    def process(self):
        try:
            dbu = databaseutils.databaseutils()
            ws = webscrapping.webscrapping()
            conn = dbu.connecttodatabase()
            cur = conn.cursor()
            ws.performscrape(cur)
        except Exception as error:
            print("Exception occurred during main run init!", error)
        finally:
            dbu.closedatabase(conn, cur)
            #print("Script Execution Completed! ")
