"""
STEP 4
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

df = sqlContext.read.parquet('hdfs:///user/piccardi/parquet/merged_sections.parquet')
