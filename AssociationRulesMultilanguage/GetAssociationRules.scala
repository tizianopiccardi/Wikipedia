
import org.apache.spark.mllib.fpm._
import org.apache.spark.mllib.fpm.FPGrowth._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import sqlContext.implicits._
import org.apache.spark.sql.types._

val MIN_SUPPORT = 0.0002
val NUM_PARTITIONS = 300
val CONFIDENCE = 0.005

// load list of sections tab separated
val data = sc.textFile("hdfs:///user/piccardi/all_sections_merged3.txt")

// to array
val transactions: RDD[Array[String]] = data.map(s => s.trim.split("\t").toSet.toArray)

// create the tree
val fpg = new FPGrowth().setMinSupport(MIN_SUPPORT).setNumPartitions(NUM_PARTITIONS)
val model = fpg.run(transactions)


//val fiRows = model.freqItemsets.map(fi=>Row(fi.items, fi.freq))
//val schema = StructType("items freq".split(" ").map(fieldName => StructField(fieldName, StringType, true)))
//val fiDataFrame = sqlContext.createDataFrame(fiRows, schema)


// store partial result as JSON (with counts)
val textFormat = model.freqItemsets.map(fi => s"""{"items": ${fi.items.map {"\"%s\"".format(_)}.mkString("[",", ","]")}, "freq": ${fi.freq}}""")
textFormat.saveAsTextFile("hdfs:///user/piccardi/all_sections_merged_freqItemsets4.txt")



// Create association rules
val ar = model.generateAssociationRules(CONFIDENCE)

val arJson = ar.map(rule => s"""{"antecedent": ${rule.antecedent.mkString("[", ",", "]")}, "consequent": "${rule.consequent.mkString("[", ",", "]")}", "confidence": ${rule.confidence}}""")
arJson.saveAsTextFile("hdfs:///user/piccardi/all_sections_merged_AR4.txt")
