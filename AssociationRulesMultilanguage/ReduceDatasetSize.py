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




###############################################


sections_dataframe = sqlContext.createDataFrame(sections_titles.flatMap(lambda r: [Row(id=r.id, title=t) for t in r.sections]))

counts_rdd = sections_dataframe.map(lambda x: (x.title, 1)).reduceByKey(lambda a,b: a+b).map(lambda x: Row(sections_title=x[0], count=x[1]))
counts_dataframe =  sqlContext.createDataFrame(counts_rdd).filter("count > 10")

joined = sections_dataframe.join(counts_dataframe, sections_dataframe.title == counts_dataframe.sections_title)


filtered = joined.map(lambda r: (r.id, [r.title])).reduceByKey(lambda a,b: a+b)

def remove_single_lang(r):
    langs = set()
    sections = []
    for s in r[1]:
        langs.add(s[0:2])
        sections.append(s)
    if len(langs)<2:
        sections=None
    return Row(id=r[0], sections=sections)

filtered_dataframe = sqlContext.createDataFrame(filtered.map(remove_single_lang))

"""
wk = sc.textFile('hdfs:///user/piccardi/all_sections_merged.txt')

counts_rdd = wk.flatMap(lambda x: x.split("\t")).map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b).map(lambda x: Row(title=x[0], count=x[1]))


counts =  sqlContext.createDataFrame(counts_rdd)

merged = sqlContext.read.parquet('hdfs:///user/piccardi/parquet/merged_sections.parquet')

sections_dataframe = sqlContext.createDataFrame(merged.flatMap(lambda r: [Row(id=r.id, title=r.origin+":"+t) for t in r.sections]))

counts_rdd = sections_dataframe.map(lambda r: (r.title, 1)).reduceByKey(lambda a,b: a+b).map(lambda x: Row(sections_title=x[0], count=x[1]))
counts_dataframe = sqlContext.createDataFrame(counts_rdd).filter("count > 10")

joined = sections_dataframe.join(counts_dataframe, sections_dataframe.title == counts_dataframe.sections_title)

def as_row(e):


a = joined.map(lambda r : (r.id, [r.title])).reduceByKey(lambda a,b:a+b)
"""

fi = sqlContext.read.json("hdfs:///user/piccardi/all_sections_merged_freqItemsets.txt")
sort = fi.sort(desc("freq"))

def get_diff_lang(row):
    mapping = None
    if row.items[0][0:2] != row.items[1][0:2]:
        mapping = row.items
    return Row(freq=row.freq, items=mapping)

tuples = sqlContext.createDataFrame(sort.filter(size(col("items"))==2).map(get_diff_lang)).filter("items is not null")
def as_string(r):
    return "\t".join(r.items)+"\t"+str(r.freq)
