
# coding: utf-8

# In[45]:

from pyspark import SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.sql import HiveContext, DataFrameWriter
from datetime import datetime
import os


# In[46]:

#Put all variables here
iHiveTable = "rawlocationtime"
oHiveTable = "vlocations"

iHiveQuery = "SELECT CONCAT(uid, ',', start_time,',',dur_loc_seq) as ev from " +  iHiveTable


# In[47]:

#Must do this if running py files independently
sc = SparkContext( 'local', 'pyspark')
hiveContext = HiveContext(sc)


# In[48]:

tdf = hiveContext.sql(iHiveQuery)


# In[51]:

#tdf.collect()


# In[52]:

DATETIME_FMT="%Y-%m-%d@%H:%M:%S"
def toLocDurTuples(line):
    values = line.split(',')
    uid = values[0]
    startTime = int(datetime.strptime(values[1],DATETIME_FMT).strftime("%s"))
    
    durLocStrL = values[2].split('][')
    durList = map(int, durLocStrL[0][1:].split(':'))
    loclist = map(int, durLocStrL[1][0:-1].split(':'))
    
    locTimeL = []
    dur = 0;
    i = 0
    for d in durList:
        locTimeL.append((loclist[i], startTime+dur, d))
        dur += d
        i += 1
    return (uid, locTimeL)

def tfin(x):
    data = x[1]
    #Sort by timestamp
    data = sorted(data, key=lambda tup: tup[1])
    
    #Remove redundant locations
    data2 = []
    maxlen = len(data) - 1
    i = 0
    while(1):
        if (i >= maxlen):
            if (i == maxlen):
                data2.append((x[0], data[i][0], data[i][1], data[i][2]))
            break;
        if data[i][0] != data[i+1][0]:
            #locations are different
            data2.append((x[0], data[i][0], data[i][1], data[i][2]))
            i += 1
        else:
            #locations are same!
            if (data[i][1] + data[i][2] == data[i+1][1]):
                #back to back on same location
                data2.append((x[0], data[i][0],                               data[i][1],                               data[i][2] + data[i+1][2]))
                #skip the next entry
                i += 2
            else:
                i += 1
    return(sorted(data2, key=lambda x: x[3], reverse=True))


# In[53]:

rdd2 = tdf.select("ev").rdd.map(lambda x: toLocDurTuples(x.ev))                        .reduceByKey(lambda a,b: a+b)                        .flatMap(lambda x: tfin(x))


# In[56]:

#rdd2.take(10)


# In[57]:

tdf2 = hiveContext.createDataFrame(rdd2, ['uid', 'loc','ts','dur'])


# In[60]:

#tdf2.collect()


# In[61]:

df_writer = DataFrameWriter(tdf2)
df_writer.insertInto(oHiveTable,overwrite=True)


# In[62]:

sc.stop()


# In[2]:

#toLocDurTuples('101,2016-06-01@12:04:02,[40:50][202:203]')


# In[3]:

'''
tfin((u'101',
  [(202, 1464782642, 40),
   (203, 1464782682, 50),
   (201, 1464851042, 60),
   (202, 1464851102, 50),
   (202, 1464851152, 40),
   (201, 1464851192, 50)]))
'''


# In[ ]:



