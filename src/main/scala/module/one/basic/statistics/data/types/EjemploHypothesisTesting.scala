package module.one.basic.statistics.data.types

import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.random.RandomRDDs.normalRDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.stat.{KernelDensity, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object EjemploHypothesisTesting extends App{

  val conf = new SparkConf().setAppName("EjemploPairwiseCorrelations").setMaster("local")
  val sc = new SparkContext(conf)

  /*
    Hypothesis Testing:

      - Used to determine whether a result is statistically significant, that is, whether it occurred by chance or not
      - Supported tests:
        - Pearson's Chi-Squiared test for goodness of fit
        - Pearson's Chi-Squiared test for independence
        - Kolmogorov-Smirnov test for equality of distribution
      - Inputs of type RDD[LabeledPoint] are also supported, enabling feature selection
   */

  /*
    Pearson's Chi-Squiared test for goodness of fit

      - Determines whether an observed frequency distribution differs from a given distribution or not
      - Requires an input of type Vector containing the frequencies of the events
      - It runs against a uniform distribution, if a secon dvector to test against is not suppliesd
      - Available as chiSqTest() function in Statistics
   */

  val vec: Vector = Vectors.dense(0.3, 0.3, 0.15, 0.1, 0.1, 0.1, 0.05)

  val goodnessOfFitTestResult = Statistics.chiSqTest(vec)

  println(goodnessOfFitTestResult)

  /*
    Pearson's Chi-Squiared test for independence

      - Determines whether unpaired observations on two variables are independent of each other
      - Requires an input of type Matrix, representing a contingency table, or an RDD[LabeledPoint]
      - Available as chiSqTest() function in Statistics
      - May be used for feature selection
   */

  //Example based in a matrix
  val mat: Matrix = Matrices.dense(3,2, Array(13, 47, 40, 80, 11, 9))

  println("\n" + mat + "\n")

  val independenceTestResult = Statistics.chiSqTest(mat)

  println(independenceTestResult)

  //Example based in a RDD of Labeled Points
  val obs: RDD[LabeledPoint] = sc.parallelize(Array(
    LabeledPoint(0, Vectors.dense(1,2)),
    LabeledPoint(0, Vectors.dense(0.5,1.5)),
    LabeledPoint(1, Vectors.dense(1,8))))

  val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(obs)

  var cadena: String = ""
  for(i <- 0 until featureTestResults.length)
    cadena += featureTestResults(i) + "\n"

  println(cadena)

  /*
    Kolmogorov-Smirnov test for equality of distribution

      - Determines whether or not two probability distributions are equal
      - One sample, two sided test
      - Supported distributions to test against:
        - normal distribution (distName='norm')
        - customized cumulative density function (CDF)
      - Available as kolmogorovSmirnovTest() function in Statistics
   */

  val data: RDD[Double] = normalRDD(sc, size=100, numPartitions=1, seed=13L)

  val testResult = Statistics.kolmogorovSmirnovTest(data, "norm", 0, 1)

  println("\n" + testResult)

  /*
    Kernel Density Estimation

      - Computes an estimate of the probability density function of a random variable,
        evaluated at a given set of points
      - Does not require assumptions about the particular distribution that the observed samples are drawn from
      - Requires an RDD of samples
      - Available as estimate() function in KernelDensity
      - In Spark, only Gaussian kernel is supported
   */

  val dataK: RDD[Double] = normalRDD(sc, size = 1000, numPartitions = 1, seed = 17L)

  val kd = new KernelDensity().setSample(dataK).setBandwidth(0.1)

  val densities = kd.estimate(Array(-1.5, -1, -0.5, 0, 0.5, 1, 1.5))

  cadena = ""
  for(i <- 0 until densities.length)
    cadena += densities(i) + "\n"

  println(cadena)
}
