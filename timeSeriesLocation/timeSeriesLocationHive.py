
# coding: utf-8

# In[8]:

from pyspark import SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.sql import HiveContext
from datetime import datetime
from pyspark.sql.functions import udf
import os


# In[9]:

#Must do this if running py files independently
sc = SparkContext( 'local', 'pyspark')
sqlContext = HiveContext(sc)


# In[10]:

tdf = sqlContext.sql("SELECT CONCAT(uid, ',', start_time,',',dur_loc_seq) as ev from rawlocationtime")


# In[11]:

tdf.collect()


# In[12]:

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
                data2.append(data[i])
            break;
        if data[i][0] != data[i+1][0]:
            #locations are different
            data2.append(data[i])
            i += 1
        else:
            #locations are same!
            if (data[i][1] + data[i][2] == data[i+1][1]):
                #back to back on same location
                data2.append((data[i][0],                               data[i][1],                               data[i][2] + data[i+1][2]))
                #skip the next entry
                i += 2
            else:
                i += 1
    return(x[0],sorted(data2, key=lambda x: x[2], reverse=True))


# In[13]:

rdd2 = tdf.select("ev").rdd.map(lambda x: toLocDurTuples(x.ev))                        .reduceByKey(lambda a,b: a+b)                        .map(lambda x: tfin(x))


# In[14]:

for x in rdd2.take(3):
    print x


# In[15]:

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

