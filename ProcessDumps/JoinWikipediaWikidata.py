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

white_list = ["itwiki", "enwiki", "dewiki", "fawiki", "nlwiki", "frwiki", "eswiki"]

links = sqlContext.read.parquet('hdfs:///user/piccardi/parquet/wikidata_links.parquet')

def pivot_row(row):
    result = []
    for l in white_list:
        result.append(Row(id=row.id, lang=l, title=row[l]))
    return result

# DataFrame[id: string, lang: string, title: string]
titles = sqlContext.createDataFrame(links.flatMap(pivot_row))
titles.registerTempTable("titles")
###############################################à

# merged_sections => DataFrame[id: bigint, origin: string, sections: array<string>, title: string]
sections_join = sqlContext.sql("""
    SELECT all_sections.*, titles.id as wikidata_id
    FROM parquet.`hdfs:///user/piccardi/parquet/merged_sections.parquet` as all_sections
    JOIN titles
    ON all_sections.origin = titles.lang AND all_sections.title = titles.title
""")

"""
[Row(id=1277775, origin='dewiki', sections=['SIEHE AUCH', 'EINZELNACHWEISE'],
title='(1016) Anitra', wikidata_id='Q11553'), Row(id=7952160, origin='dewiki',
sections=['WEBLINKS', 'EINZELNACHWEISE'], title='(11335) Santiago', wikidata_id='Q370943')
...]
"""

# group by wikidata id
def map_sections(row):
    labeled_titles = [row.origin+":"+t for t in row.sections]
    return (row.wikidata_id, labeled_titles)

def merge_sections(a, b):
    return a+b

merged_sections = sections_join.map(map_sections).reduceByKey(merge_sections)

def as_string(tpl):
    return "\t".join(tpl[1])

merged_sections.map(as_string).saveAsTextFile('hdfs:///user/piccardi/all_sections_merged.txt')
