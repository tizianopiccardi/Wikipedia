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

exclusion_list = """enwiki:REFERENCES
enwiki:EXTERNAL LINKS
dewiki:WEBLINKS
eswiki:REFERENCIAS
frwiki:NOTES ET RÉFÉRENCES
eswiki:ENLACES EXTERNOS
enwiki:SEE ALSO
dewiki:EINZELNACHWEISE
itwiki:COLLEGAMENTI ESTERNI
itwiki:NOTE
fawiki:منابع
itwiki:ALTRI PROGETTI
frwiki:LIENS EXTERNES
frwiki:VOIR AUSSI
eswiki:VÉASE TAMBIÉN
frwiki:RÉFÉRENCES
itwiki:VOCI CORRELATE
fawiki:جستارهای وابسته
enwiki:NOTES
dewiki:SIEHE AUCH
nlwiki:EXTERNE LINKS
nlwiki:EXTERNE LINK
enwiki:FURTHER READING
nlwiki:ZIE OOK
frwiki:LIEN EXTERNE
frwiki:ANNEXES
frwiki:ARTICLES CONNEXES
eswiki:NOTAS"""

exclusion_list = set(exclusion_list.replace("\r", "").split("\n"))

# group by wikidata id
def map_sections(row):
    labeled_titles = []
    for s in row.sections:
        localized_token = row.origin+":"+s
        if localized_token not in exclusion_list:
            labeled_titles.append( (row.origin,s) )
    return (row.wikidata_id, labeled_titles)

def merge_sections(a, b):
    return a+b

merged_sections = sections_join.map(map_sections).reduceByKey(merge_sections)

def as_row(tpl):
    lang_index = set()
    for e in tpl[1]:
        lang_index.add(e[0])
    token_list = []
    for e in tpl[1]:
        token_list.append(e[0]+":"+e[1])
    if len(token_list)<1 or len(lang_index)<2 :
        token_list = None
    return Row(id=tpl[0], sections=token_list)

sections_titles = sqlContext.createDataFrame(merged_sections.map(as_row)).filter("sections is not null")

def as_string(section):
    return "\t".join(section.sections)

sections_titles.map(as_string).saveAsTextFile('hdfs:///user/piccardi/all_sections_merged.txt')
