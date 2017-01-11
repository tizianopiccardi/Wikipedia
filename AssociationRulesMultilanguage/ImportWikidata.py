"""
STEP 2
"""
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import tzinfo, datetime
import pytz
from pyspark import SparkContext, SQLContext

sc = SparkContext()

sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

lines = sc.textFile("hdfs:///user/piccardi/dumps/wikidata-20161219-all.json.bz2")

white_list = ["itwiki", "enwiki", "dewiki", "fawiki", "nlwiki", "frwiki", "eswiki"]

def get_as_row(line):
    if line[-1]==',':
        line = line[0:-1]
    entity = {}
    try:
        # required to fix self-contained row (i.e. first and last)
        entity = json.loads(line)
    except Exception as e:
        pass

    entity_id = entity.get("id", None)
    links = entity.get("sitelinks", None)
    if links is None:
        links = {}
    # at least 2 languages version of the article
    count = 0
    for l in white_list:
        if l in links:
            count+=1
    if count<2:
        entity_id = None # mark the entity as bad
    row_params = {
        'id': entity_id
    }
    for l in white_list:
        site_title = links.get(l, None)
        if site_title is not None:
            site_title = site_title.get("title")
        row_params[l] = site_title
    return Row(**row_params)

wikidata_links = sqlContext.createDataFrame(lines.map(get_as_row))

wikidata_links.filter("id is not null").write.parquet("hdfs:///user/piccardi/parquet/wikidata_links.parquet")
