
import org.apache.spark.mllib.fpm._
import org.apache.spark.mllib.fpm.FPGrowth._
import org.apache.spark.rdd.RDD

val MIN_SUPPORT = 0.00001
val NUM_PARTITIONS = 300
val CONFIDENCE = 0.001

// load list of sections tab separated
val data = sc.textFile("hdfs:///user/piccardi/sections.txt")

// to array
val transactions: RDD[Array[String]] = data.map(s => s.trim.split("\t"))

// create the tree
val fpg = new FPGrowth().setMinSupport(MIN_SUPPORT).setNumPartitions(NUM_PARTITIONS)
val model = fpg.run(transactions)


// store partial result as JSON (with counts)
val textFormat = model.freqItemsets.map { fi => s"""{"items": ${fi.items.map {"\"%s\"".format(_)}.mkString("[",", ","]")}, "freq": ${fi.freq}}""" }
textFormat.saveAsTextFile("hdfs:///user/piccardi/freqItemsets.txt")



// Create association rules
val ar = model.generateAssociationRules(CONFIDENCE)

val arJson = ar.map(rule => s"""{"antecedent": ${rule.antecedent.mkString("[", ",", "]")}, "consequent": "${rule.consequent}", "confidence": ${rule.confidence}}""")
arJson.saveAsTextFile("hdfs:///user/piccardi/ar_json.txt")
