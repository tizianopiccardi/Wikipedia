"""
STEP 1
"""
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import tzinfo, datetime
import pytz
from pyspark import SparkContext, SQLContext


input_dump = 'hdfs:///user/piccardi/dumps/itwiki-20161220-pages-articles-multistream.xml.bz2'
output_parquet = 'hdfs:///user/piccardi/parquet/itwiki.parquet'

sc = SparkContext()

sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

wikipedia = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='page').load(input_dump)
articles = wikipedia.where("ns = 0").where("redirect is null")

def get_as_row(line):
    return Row(text=line.revision.text._VALUE, id=line.id,
    timestamp=line.revision.timestamp, revision_id=line.revision.id,
    title=line.title)

wk_dataframe = sqlContext.createDataFrame(articles.map(get_as_row))

wk_dataframe.write.parquet(output_parquet)
