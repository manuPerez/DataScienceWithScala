package module.one.basic.statistics.data.types

import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.{SparkConf, SparkContext}

object EjemploRandomDataGeneration extends App{
  /*
    - RandomRDDs generate either random double RDDs or vector RDDs
    - Supported distributions:
      - uniform
      - normal
      - lognormal
      - poisson
      - exponential
      - gamma
    - Useful for randomized algorithms, prototyping and performance testing


    All Vector RDDs are dimensionality of 2 or greater.
   */

  //Example of poissonRDD
  val conf = new SparkConf().setAppName("EjemploPairwiseCorrelations").setMaster("local")
  val sc = new SparkContext(conf)

  val million = poissonRDD(sc, mean=1, size=1000000L, numPartitions = 10)

  println("million.mean: " + million.mean())
  println("million.variance: " + million.variance())

  //Example of Multivariate normal
  val data = normalVectorRDD(sc, numRows = 100000L, numCols = 3, numPartitions = 10)

  val stats: MultivariateStatisticalSummary = Statistics.colStats(data)

  println("stats.mean: " + stats.mean)
  println("stats.variance: " + stats.variance)
}
