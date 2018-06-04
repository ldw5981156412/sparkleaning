import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(args(0))
    val wordcount = rdd.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    val wordsort = wordcount.map(x => (x._1, x._2)).sortByKey(false).map(x => (x._2, x._1))
    wordsort.saveAsTextFile(args(1))
    sc.stop()
    //    println("Hello world")
  }
}
