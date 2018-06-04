import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "D:\\spark-2.3.0-bin-hadoop2.7\\README.md"
    val spark = SparkSession.builder().appName("Simplle Application").master("local").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b:$numBs")
    spark.stop()
  }
}
