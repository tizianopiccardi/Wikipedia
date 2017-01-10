"""
STEP 3
"""
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

white_list = ["itwiki", "enwiki", "dewiki", "fawiki", "nlwiki", "frwiki", "eswiki"]
parquet_path = 'hdfs:///user/piccardi/parquet/'

articles = None
for v in white_list:
    path = parquet_path + v + ".parquet"
    df = sqlContext.read.parquet(path)
    df = df.withColumn("origin", lit(v))
    if articles is None:
        articles = df
    else:
        articles = articles.unionAll(df)

sections_regex = re.compile(r'(^|[^=])==([^=\n\r]+)==([^=]|$)')

def get_sections(row):
    sections = []
    try:
        sections = [s[1].strip().upper() for s in sections_regex.findall(row.text)]
    except Exception as e:
        pass
    if len(sections)< 1:
        sections = None
    return Row(id=row.id, title=row.title,
            origin=row.origin, sections=sections)

sections_dataframe = sqlContext.createDataFrame(articles.map(get_sections))
sections_dataframe = sections_dataframe.filter("sections is not null")

sections_dataframe.write.parquet('hdfs:///user/piccardi/parquet/merged_sections.parquet')
