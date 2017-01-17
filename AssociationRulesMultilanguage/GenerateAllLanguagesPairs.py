from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import tzinfo, datetime
import pytz
import re
from pyspark import SparkContext, SQLContext
from pyspark.mllib.fpm import FPGrowth

sc = SparkContext()

sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

languages_list = ["enwiki", "itwiki", "dewiki", "fawiki", "nlwiki", "frwiki", "eswiki"]


# DataFrame[dewiki: string, enwiki: string, eswiki: string, fawiki: string,
# frwiki: string, id: string, itwiki: string, nlwiki: string]
links = sqlContext.read.parquet('hdfs:///user/piccardi/parquet/wikidata_links.parquet')

# translate the elements from columns to rows format
def pivot_row(row):
    result = []
    for l in languages_list:
        result.append(Row(id=row.id, lang=l, title=row[l]))
    return result

# DataFrame[id: string, lang: string, title: string]
# removing null entry removes ~12M rows
titles = sqlContext.createDataFrame(links.flatMap(pivot_row)).filter("title is not null")
titles.registerTempTable("titles")

"""
titles (9,815,534):
[Row(id='Q31', lang='enwiki', title='Belgium'),
Row(id='Q31', lang='itwiki', title='Belgio'),
Row(id='Q31', lang='dewiki', title='Belgien'),
...
"""

# merged_sections => DataFrame[id: bigint, origin: string, sections: array<string>, title: string]
sections_join = sqlContext.sql("""
    SELECT all_sections.*, titles.id as wikidata_id
    FROM parquet.`hdfs:///user/piccardi/parquet/merged_sections.parquet` as all_sections
    JOIN titles
    ON all_sections.origin = titles.lang AND all_sections.title = titles.title
""")

"""
sections_join:
[Row(id=1277775, origin='dewiki', sections=['SIEHE AUCH', 'EINZELNACHWEISE'],
title='(1016) Anitra', wikidata_id='Q11553'), Row(id=7952160, origin='dewiki',
sections=['WEBLINKS', 'EINZELNACHWEISE'], title='(11335) Santiago', wikidata_id='Q370943')
...]
"""

# NOTE: START OF CLEANING STEP

# Needs manual review:
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
enwiki:OTHER
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
dewiki:ANMERKUNGEN
enwiki:FURTHER READING
nlwiki:ZIE OOK
frwiki:LIEN EXTERNE
frwiki:ANNEXES
frwiki:ARTICLES CONNEXES
nlwiki:REFERENTIES
eswiki:NOTAS"""

exclusion_list = set(exclusion_list.replace("\r", "").split("\n"))

# filters common words
# out:
def map_sections(row):
    labeled_titles = []
    for s in row.sections:
        localized_token = row.origin+":"+s
        if localized_token not in exclusion_list:
            labeled_titles.append( (row.origin,s) )
    return (row.wikidata_id, labeled_titles)

def merge_sections(a, b):
    return a+b

# filtering out the common words removes a lot of articles from the list (most of the are empty)
merged_sections = sections_join.map(map_sections).reduceByKey(merge_sections)
"""
merged_sections:
[('Q1761610', [('dewiki', 'SITATAPATRA APARAJITA'),
('dewiki', 'USHNISHA SITATAPATRA'), ('dewiki', 'WIRKUNG'),
('dewiki', 'DARSTELLUNG'), ('enwiki', 'NAMES'), ('enwiki', 'MANTRAS'),
('enwiki', 'SYMBOLISM')]), ('Q1146513', [('enwiki', 'RELATED ECLIPSES'),
('nlwiki', 'LENGTE')])]

"""


# I need to filter the articles that has at least 2 languages
# if len(lang_index)<2 the sections list is None
def as_row(tpl):
    lang_index = set()
    for e in tpl[1]:
        lang_index.add(e[0])
    # lang_index: {dewiki, itwiki, ...}
    token_list = []
    for e in tpl[1]:
        token_list.append({"lang": e[0], "title": e[1]})
    if len(token_list)<1 or len(lang_index)<2 :
        token_list = None
    return Row(id=tpl[0], sections=token_list)

# this is the list of articles that have at least 2 languages (any combination)
sections_titles = sqlContext.createDataFrame(merged_sections.map(as_row)).filter("sections is not null")
"""
sections_titles (1,674,844):
Row(id='Q218792', sections=[{'title': 'DISTRIBUCIÓN', 'lang': 'eswiki'},
{'title': 'MERKMALE', 'lang': 'dewiki'}, {'title': 'VERBREITUNG', 'lang': 'dewiki'}
...
{'title': 'RÉPARTITION GÉOGRAPHIQUE', 'lang': 'frwiki'},
{'title': "L'ESPÈCE ET L'HOMME", 'lang': 'frwiki'}, ...
{'title': 'REFERENTIES', 'lang': 'nlwiki'}])
"""
# NOTE: END OF CLEANING STEP

sections_expanded_rdd = sections_titles.flatMap(lambda r: [Row(id=r.id, title=e['title'], lang=e['lang']) for e in r.sections])
# DataFrame[id: string, lang: string, title: string]
sections_expanded = sqlContext.createDataFrame(sections_expanded_rdd)

# NOTE: from here the dataset is clean and ready

pairs = []
for i in range(0, len(languages_list)-1):
    for j in range(i+1, len(languages_list)):
        pairs.append((languages_list[i], languages_list[j]))
"""
pairs:
[('itwiki', 'enwiki'), ('itwiki', 'dewiki'), ('itwiki', 'fawiki'),
...  ('nlwiki', 'frwiki'), ('nlwiki', 'eswiki'), ('frwiki', 'eswiki')]
"""


def as_string(sections):
    return "\t".join(sections.items)+"\t"+str(sections.freq)

def map_couples_different_lang(row):
    items = None
    if len(row.items)==2 and row.items[0][0:2] is not row.items[1][0:2]:
        items = row.items
    return Row(items=items, freq=row.freq)


for p in pairs:
    languages_mapping = sections_expanded.where((col("lang") == p[0]) | (col("lang") == p[1]))
    # IMPORTANT: here several articles may have only 1 languages
    # It's time to reduce by id and remove these articles
    language_pairs_rdd = languages_mapping.map(lambda r: (r.id, [(r.lang, r.title)])).reduceByKey(merge_sections)
    valid_pairs = sqlContext.createDataFrame(language_pairs_rdd.map(as_row)).filter("sections is not null")
    # transactions(FPGrowth formal name): list of terms
    # IMPORTANT: as set because there are repetitions
    transactions = valid_pairs.map(lambda r: set([e['lang']+":"+e['title'] for e in r.sections]))
    model = FPGrowth.train(transactions, minSupport=0.0002, numPartitions=150)

    # REQUIREMENTS: size of the items == 2 AND from two different languages
    result = sqlContext.createDataFrame(model.freqItemsets().map(map_couples_different_lang)).filter("items is not null")
    sorted_result = result.rdd.sortBy(lambda r: -r.freq)
    sorted_result.map(as_string).saveAsTextFile('hdfs:///user/piccardi/matching/'+p[0]+p[1]+'.txt')
