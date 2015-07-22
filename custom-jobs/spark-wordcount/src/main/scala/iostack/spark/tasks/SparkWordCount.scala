package iostack.spark.tasks

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object SparkWordCount {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: SparkWordCount [<delimiter>] <input> <output>\nBy default delimiter is a space")
      System.exit(1)
    }

    var delimiter = " "
    var argIndex = 0
    if (args.length == 3) {
      delimiter = args(argIndex).toString
    }
    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val sc = new SparkContext(sparkConf)
    val textFile = sc.textFile(args(argIndex))
    argIndex = argIndex+1
    val counts = textFile.flatMap(line => line.split(delimiter)).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile(args(argIndex))

    sc.stop()
  }
}
