package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.stat.Statistics

object TaxiCluster {
  val conf = new SparkConf().setAppName("taxi")
  val sc = new SparkContext(conf)
  val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

  def main(args: Array[String]): Unit = {
    import preprocess._
    import learn._
    val file = "data/taxi201601.csv"
    val data = new TaxiData(getData(sc, file))
    val dataSanitized = data.sanitize
    expCluster(data, 2)
    expCluster(dataSanitized, 2)
    expCluster(dataSanitized, 10)
    expCluster(dataSanitized, 10, true)

  }
}
