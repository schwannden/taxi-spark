import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import preprocess._

package object learn {
  def gmm(data: RDD[Vector], k: Int, nIter: Int): GaussianMixtureModel = {
    new GaussianMixture().setK(k).setMaxIterations(nIter).run(data)
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

  def runGmm(data: TaxiData, sampleRatio: Double = 0.01): Unit = {
    val pickRaw = data.pickLocation.sample(false, sampleRatio).cache
    val dropRaw = data.dropLocation.sample(false, sampleRatio).cache
    val factor = 1000
    val pickMean = Statistics.colStats(pickRaw).mean.toArray
    val dropMean = Statistics.colStats(dropRaw).mean.toArray
    val pick = normalize(pickRaw, pickMean, factor)
    val drop = normalize(dropRaw, dropMean, factor)
    val t1 = System.currentTimeMillis
    val (k, nIter) = (10, 90)
    val gmmPickup = gmm(pick, k, nIter)
    val t2 = System.currentTimeMillis
    val gmmDropoff = gmm(drop, k, nIter)
    val t3 = System.currentTimeMillis
    printGmm(gmmPickup, pickMean, factor)
    printGmm(gmmDropoff, pickMean, factor)
    println(t2 - t1)
    println(t3 - t2)
  }

  def peakHour(hours: RDD[Int]) = {
    hours.groupBy(identity).mapValues(_.size)
  }

  def distanceAnalysis(data: TaxiData): Unit = {
    val ds = data.distance
    val summary = Statistics.colStats(ds)
    print(summary.mean)
  }

}
