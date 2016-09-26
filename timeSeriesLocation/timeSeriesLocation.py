from pyspark import SparkContext
from pyspark.sql import Row, SQLContext
from datetime import datetime
import os

sc = SparkContext( 'local', 'pyspark')
sqlContext = SQLContext(sc)

LabeledDocument = Row('uid',
                     'start_time',
                     'locTime')
DATETIME_FMT="%Y-%m-%d@%H:%M:%S"
def parseDocument(line):
    values = [str(x) for x in line.split(',')]
    uid = values[0]
    st = datetime.strptime(values[1],DATETIME_FMT)
    tlist = values[2][1:-1].split(':')
    loclist = values[3][1:-1].split(':')
    locTimeList = zip(loclist,tlist)
    return LabeledDocument(uid,st,locTimeList)

fileurl = fileurl = 'file:///'+os.path.abspath('.')+'/timeSeriesLocation.csv'

df = sc.textFile(fileurl) \
    .filter(lambda s: "uid" not in s) \
    .map(parseDocument) \
    .toDF()

print df.show(2)

sc.stop()
