
import org.apache.spark.mllib.fpm._
import org.apache.spark.mllib.fpm.FPGrowth._
import org.apache.spark.rdd.RDD

val data = sc.textFile("hdfs:///user/piccardi/sections.txt")

val transactions: RDD[Array[String]] = data.map(s => s.trim.split("\t"))

val fpg = new FPGrowth().setMinSupport(0.00001).setNumPartitions(300)
val model = fpg.run(transactions)



val textFormat = model.freqItemsets.map { fi => s"""{"items": ${fi.items.map {"\"%s\"".format(_)}.mkString("[",", ","]")}, "freq": ${fi.freq}}""" }

textFormat.saveAsTextFile("hdfs:///user/piccardi/freqItemsets.txt")






val ar = model.generateAssociationRules(0.001)

val arText = ar.map(rule => s"""{"antecedent": ${rule.antecedent.mkString("[", ",", "]")}, "consequent": "${rule.consequent}", "confidence": ${rule.confidence}}""")

arText.saveAsTextFile("hdfs:///user/piccardi/ar_json.txt")
