import re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SQLContext
from datetime import tzinfo, datetime
from pyspark import SparkContext, SQLContext
from pyspark.mllib.fpm import FPGrowth
import json

sc = SparkContext()

sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")



last_revisions = sqlContext.read.parquet("hdfs:///user/piccardi/last_revisions.parquet")


# create the list of excluded sections
exclusion_list = set(["REFERENCES", "EXTERNAL LINKS", "SEE ALSO"])
# exclude alphabetic lists
for i in range(ord('A'), ord('Z')+1):
    exclusion_list.add(chr(i))
# exclude years
for i in range(1900, 2018):
    exclusion_list.add(str(i))


# extract and clean the sections' titles
def get_sections(row):
    sections = row['sections'].strip().split("\t")
    output = []
    for title in sections:
        title = title.replace("[[","").replace("]]","").replace("''", "").replace("{{", "").replace("}}", "").upper()
        if title not in exclusion_list and title not in output:
            output.append(title)
    return output


transactions = last_revisions.map(get_sections)
transactions.map(lambda r: "\t".join(r)).saveAsTextFile('hdfs:///user/piccardi/sections.txt')
