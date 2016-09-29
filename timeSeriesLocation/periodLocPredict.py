
# coding: utf-8

# In[1]:

from pyspark import SparkContext
from pyspark.sql import HiveContext, DataFrameWriter
from datetime import datetime
import time
import re


# In[6]:

#Put all variables here
iHiveTable = "vconsolsession"
oHiveTable = "vprdloc"

startDate = 20160601
endDate = 20160602

iHiveQuery = "SELECT CONCAT(uid, ','," + " loc_ts_dur) as ev " + "from " + iHiveTable + " where data_dt>=" + str(startDate) + " and data_dt <=" + str(endDate)


# In[7]:

#iHiveQuery


# In[8]:

sc = SparkContext( 'local', 'pyspark')
hiveContext = HiveContext(sc)


# In[9]:

tdf = hiveContext.sql(iHiveQuery)


# In[10]:

#tdf.collect()


# In[11]:

def predictPeriodDuration(x):
    uidI = 0
    locTsDur = 1
    
    locPrdDur = []
    RX = re.compile('[[()]')
    locTsDurL = RX.sub('', x[locTsDur]).replace(']','').split('|')
    for locTsDur in locTsDurL:
        loc,ts,dur = locTsDur.split(':')
        #Convert ts to period
        t1 = time.gmtime(int(ts))
        prd = ((t1.tm_wday + 1)%7)*24 + t1.tm_hour
        locPrdDur.append((loc,prd,int(dur), int(ts)))

    #Pick the location with max duration in each period
    locPrdDur = sorted(locPrdDur, key=lambda x: (x[1],x[2]), reverse=True)
    sortedPrdL = []
    prevprd = -1
    for loc,prd,dur,ts in locPrdDur:
        if prevprd != prd:
            sortedPrdL.append((x[uidI],loc,prd,dur,ts))
        prevprd = prd

    sortedPrdL = sorted(sortedPrdL, key=lambda x: x[2])
    return sortedPrdL


# In[12]:

rdd2 = tdf.select("ev").rdd.map(lambda x: tuple(x.ev.split(',')))                             .reduceByKey(lambda a,b: a+b)                             .flatMap(predictPeriodDuration)


# In[13]:

#rdd2.take(10)


# In[14]:

tdf2 = hiveContext.createDataFrame(rdd2, ['uid','loc','prd','dur', 'ts'])


# In[15]:

#tdf2.collect()


# In[16]:

df_writer = DataFrameWriter(tdf2)
df_writer.insertInto(oHiveTable,overwrite=True)


# In[17]:

sc.stop()


# In[19]:

#predictPeriodDuration((u'101',
#   u'[(202:1464764702:90)|(201:1464764642:60)|(201:1464764792:50)|(203:1464782682:50)|(202:1464782642:40)]'))

