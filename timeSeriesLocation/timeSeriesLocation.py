from pyspark import SparkContext
from pyspark.sql import Row, SQLContext
from datetime import datetime
import os

sc = SparkContext( 'local', 'pyspark')
sqlContext = SQLContext(sc)

LabeledDocument = Row('uid',
                     'startTime',
                     'locTimeL')
DATETIME_FMT="%Y-%m-%d@%H:%M:%S"
def parseDocument(line):
    values = [str(x) for x in line.split(',')]
    uid = values[0]
    startTime = int(datetime.strptime(values[1],DATETIME_FMT).strftime("%s"))
    durList = map(int, values[2][1:-1].split(':'))
    loclist = values[3][1:-1].split(':')
    
    locTimeL = []
    dur = 0;
    i = 0
    for d in durList:
        locTimeL.append((loclist[i], startTime+dur, d))
        dur += d
        i += 1
    return LabeledDocument(uid,startTime,locTimeL)

fileurl = fileurl = 'file:///'+os.path.abspath('.')+'/timeSeriesLocation.csv'

df = sc.textFile(fileurl) \
    .filter(lambda s: "uid" not in s) \
    .map(parseDocument) \
    .toDF()

for r in df.collect():
    print r.uid + ' ' + str(r.locTimeL)

sc.stop()
