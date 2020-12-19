from __future__ import absolute_import, print_function
from CountryHolder import CountryHolder
import threading
import time
from TwitterStream import TwitterStream
import json
import csv
from datetime import date

#save methoden virker ikke

def worker(arg):
    while arg["run"] is True:
        time.sleep(10)
        cash = arg["cash"]
        print("---export---")  # todo testing
        l = ['country_code','count']
        today = date.today()
        with open(str(today)+"ting.csv", "a") as outfile:
            for c in cash.country_list:
                writer = csv.writer(outfile)
                writer.writerow([str(c.name),str(len(c.tweets))])
                print(str(c.name)+" - "+str(len(c.tweets)))
            cash.clear()

if __name__ == '__main__':
    cash = CountryHolder()
    ts = TwitterStream()
    # create export thread
    info = {"cash": cash, "run": True}
    thread = threading.Thread(target=worker, args=(info,))
    thread.start()

    ts.start_streaming(cash)
