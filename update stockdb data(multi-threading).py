# -*- coding:utf-8 -*-
#/usr/bin/env python

"update all data in stockdb"
"using multi-thread to accelerating download speed"
import tushare as ts
import pandas as pd
import sqlite3 as sql
import datetime as dt
import threading as td
from Queue import Queue

threads_num = 30
updateStockList = False

conn = sql.connect(r'D:\OneDrive\stock\data\stock.db',check_same_thread=False)
cl = pd.DataFrame()
codeList = pd.DataFrame()

if updateStockList == True:
    cl = ts.get_today_all()
    codeList = cl.iloc[0:len(cl),0:1]
    codeList.to_sql('CodeList', conn,if_exists = "replace", index_label = 'index')
else:      
    codeList=pd.read_csv(r'D:\OneDrive\stock\data\allcode.csv', dtype={'code':str})

#df = ts.get_hist_data('399001')
#lastDate = df.index[0]

def sql_writer(database, dataqueue, listqueue):
    stockcnt=0
    while listqueue.empty() != True:
        atuple = dataqueue.get(1)
        tableName = "s"+atuple[0]
        atuple[1].to_sql(tableName, database, if_exists = "replace" ,index_label = 'date')
        stockcnt = stockcnt+1
        print "%d/%d" %(stockcnt, len(codeList))

def update_one_stock(code, dataqueue):   
    df = ts.get_hist_data(code)
    dataqueue.put((code,df),1)

def thread_manager(listqueue,dataqueue):
    while listqueue.empty() != True:
        code = listqueue.get(1)
        #print "%d/%d" %(listqueue.qsize(), len(codeList))
        update_one_stock(code, dataqueue)      

def main():
    listq = Queue(len(codeList))
    for i in range(len(codeList)):
        listq.put(codeList.loc[i]['code'],1)

    dataq = Queue(threads_num)

    threads = []
    t = td.Thread(target = sql_writer, args = (conn, dataq,listq))
    threads.append(t)
                  
    for i in range(threads_num):
        t = td.Thread(target = thread_manager,args = (listq,dataq))
        threads.append(t)     

    for i in range(len(threads)):
        threads[i].start()

    for i in range(len(threads)):
        threads[i].join()
    
    
if __name__ == "__main__":
    main()
