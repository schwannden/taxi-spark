package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TaxiCluster {
  val conf = new SparkConf().setAppName("taxi")
  val sc = new SparkContext(conf)
  val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

  def main(args: Array[String]) = {
    import preprocess._
    import learn._
    val file = "data/taxi201601.csv"
    val data = new TaxiData(getData(sc, file))
    runGmm(data)
    runGmm(data.sanitize)
  }
}
