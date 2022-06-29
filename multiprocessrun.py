import multiprocessing
import databaseutils
import json

from mainrun import mainrun

if __name__ == "__main__":
   m = mainrun()
   dbu = databaseutils.databaseutils()
   p = multiprocessing.Process(target=m.process)
   p.start()
   p.join()
   result = {'response': "You can view result now!"}
   print(json.dumps(result))