import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import preprocess._

package object learn {
  def gmm(data: RDD[Vector], k: Int = 2): GaussianMixtureModel = {
    new GaussianMixture().setK(k).run(data)
  }

  def printGmm(gmmResult: GaussianMixtureModel, means: Array[Double] = Array(0, 0), factor: Double = 1.0): Unit = {
    val sortedW = gmmResult.weights.sorted.reverse
    for (w <- sortedW) {
      val i = gmmResult.weights.indexOf(w)
      println("weight=%f\nlong=%s,lag=%s\nsigma=\n%s\n" format
        (w,
          gmmResult.gaussians(i).mu(0) / factor + means(0),
          gmmResult.gaussians(i).mu(0) / factor + means(1),
          gmmResult.gaussians(i).sigma))
    }
  }

  def runGmm(locations: RDD[Vector], k: Int = 2, normalized: Boolean = false): GaussianMixtureModel = {
    val t1 = System.currentTimeMillis
    val res = if (normalized) {
      val factor = 1000
      val mean = Statistics.colStats(locations).mean.toArray
      val locationsNormalized = normalize(locations, mean, factor)
      gmm(locationsNormalized, k)
    } else {
      gmm(locations, k)
    }
    val t2 = System.currentTimeMillis
    println("Running Time: " + (t2 - t1)/60000.0 + " minutes")
    res
  }

  def distanceAnalysis(data: TaxiData): Unit = {
    val ds = data.distance
    val summary = Statistics.colStats(ds)
    print(summary.mean)
  }

  def expCluster(data: TaxiData, k: Int, normalized: Boolean = false): Unit = {
    val res1 = runGmm(data.pickLocation, k = k, normalized = normalized)
    printGmm(res1)
    val res2 = runGmm(data.dropLocation, k = k, normalized = normalized)
    printGmm(res2)
  }

  def expHourCount(data: TaxiData): Unit = {
    val pickHour = data.pickHour.groupBy(v => v(0)).mapValues(_.size)
    print(pickHour.collect.sortWith((x, y) => x._2 > y._2))

    val dropHour = data.dropHour.groupBy(v => v(0)).mapValues(_.size)
    print(dropHour.collect.sortWith((x, y) => x._2 > y._2))
  }

}
