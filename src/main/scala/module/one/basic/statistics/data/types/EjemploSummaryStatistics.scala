package module.one.basic.statistics.data.types

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object EjemploSummaryStatistics extends App{
  val conf = new SparkConf().setAppName("EjemploSummaryStatistics").setMaster("local")
  val sc = new SparkContext(conf)

  val observations: RDD[Vector] = sc.parallelize(Array(
    Vectors.dense(1,2),
    Vectors.dense(4,5),
    Vectors.dense(7,8)))

  val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)

  println("mean: " + summary.mean)
  println("variance: " + summary.variance)
  println("numNonzeros: " + summary.numNonzeros)
  println("normL1: " + summary.normL1)
  println("normL2: " + summary.normL2)
}
