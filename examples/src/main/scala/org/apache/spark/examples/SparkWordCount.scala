
package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object SparkWordCount {



  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: SparkWordCount <input> <output>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val sc = new SparkContext(sparkConf)
    val textFile = sc.textFile(args(0))

    val counts = textFile.flatMap(line => line.split(",")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))

    sc.stop()
  }
}
