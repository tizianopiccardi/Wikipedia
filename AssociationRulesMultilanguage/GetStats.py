from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import tzinfo, datetime
import pytz
import re
from pyspark import SparkContext, SQLContext

sc = SparkContext()

sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")


wk = sc.textFile('hdfs:///user/piccardi/all_sections_merged.txt')

counts_rdd = wk.flatMap(lambda x: x.split("\t")).map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b).map(lambda x: Row(title=x[0], count=x[1]))


counts =  sqlContext.createDataFrame(counts_rdd)

sorted_counts = counts.sort(desc("count"))
