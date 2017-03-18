import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD


package object preprocess {

  class TaxiData (val _raw: RDD[Vector], val sanitized: Boolean) {

    def this(data: RDD[String]) =
      this(data.mapPartitionsWithIndex((i, lines) => {
        if (i == 0)
          lines.drop(1)
        lines
          .map(_.split(','))
          .map(line => {
            Vectors.dense(
              line(1).split(' ')(1).split(':')(0).toDouble,
              line(2).split(' ')(1).split(':')(0).toDouble,
              line(3).toDouble, line(4).toDouble, line(5).toDouble, line(6).toDouble,
              line(9).toDouble, line(10).toDouble, line(14).toDouble)
          })
      }), false)

    def pickHour: RDD[Vector] = _raw map (line => Vectors.dense(line(0).toInt))
    def dropHour: RDD[Vector] = _raw map (line => Vectors.dense(line(1).toInt))
    def passengerCount: RDD[Vector] = _raw map (line => Vectors.dense(line(2).toInt))
    def distance: RDD[Vector] = _raw map (line => Vectors.dense(line(3)))
    def pickLocation: RDD[Vector] = _raw map (line => Vectors.dense(line(4), line(5)))
    def dropLocation: RDD[Vector] = _raw map (line => Vectors.dense(line(6), line(7)))
    def tip: RDD[Vector] = _raw map (line => Vectors.dense(line(8)))

    def sanitize: TaxiData = new TaxiData(
      _raw.filter(v => {
        v(4) != 0 && v(5) != 0 && v(6) != 0 && v(7) != 0
      }), true
    )

    def filter(p: Vector => Boolean): TaxiData = new TaxiData(
      _raw.filter(p), true
    )

    class Summary {
      lazy val summary = Statistics.colStats(_raw)
      lazy val means = summary.mean.toArray
      lazy val vars = summary.variance.toArray
      override def toString = {
        "pickHour dropHour pasCount distance pickLoct dropLoct tip\n" +
        means.map(m => (math.floor(m * 1000) / 1000).toString).mkString(",  ") + "\n" +
        vars.map(m => (math.floor(m * 1000) / 1000).toString).mkString(",  ")
      }
    }

    lazy val summary = new Summary
  }

  def getData(sc: SparkContext, file: String) = {
    sc.textFile(file)
  }

  def normalize(data: RDD[Vector], mean: Array[Double], factor: Double): RDD[Vector] = {
    data.map(line => Vectors.dense(
      (line.toArray zip mean) map (pair => (pair._1 - pair._2)*factor))
    )
  }

  def deNormalize(data: RDD[Vector], mean: Array[Double], factor: Double): RDD[Vector] = {
    data.map(line => Vectors.dense(
      (line.toArray zip mean) map (pair => pair._1/factor + pair._2))
    )
  }

  def getDistance(v: Vector): Double = {
    v(3)
  }
}
