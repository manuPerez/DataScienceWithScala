import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}

/*
  Correlación: indica la fuerza y dirección de una relación lineal y proporcionalidad entre dos variables estadísticas.
  Se dice que dos variables cuantitativas están correlacionadas cuando los valores de una de ellas varían
  sistemáticamente con respecto a los valores homónimos de la otra. Si tenemos dos variables A y B, existe correlación
  si al aumentar los valores de A lo hacen también los de B y viceversa.

   Coeficientes de correlación:

      - Pearson: se obtiene dividiendo la covarianza de dos variables entre el producto de sus desviaciones estándar.
      - Spearman: Este coeficiente se emplea cuando una o ambas escalas de medidas de las variables son ordinales,
        es decir, cuando una o ambas escalas de medida son posiciones. Ejemplo: Orden de llegada en una carrera y peso
        de los atletas.
 */
object EjemploPairwiseCorrelations extends App{
  val conf = new SparkConf().setAppName("EjemploPairwiseCorrelations").setMaster("local")
  val sc = new SparkContext(conf)

  //Pearson Correlation Between Two Series
  val x: RDD[Double] = sc.parallelize(Array(2,9,-7))
  val y: RDD[Double] = sc.parallelize(Array(1,3,5))
  val correlation: Double = Statistics.corr(x, y, "pearson")

  println("correlation:")
  println(correlation)

  //Pearson Correlation among Series
  val data: RDD[Vector] = sc.parallelize(Array(
    Vectors.dense(2,9,-7),
    Vectors.dense(1,-3,5),
    Vectors.dense(4,0,-5)))

  val correlMatrix: Matrix = Statistics.corr(data, "pearson")

  println("correlMatrix:")
  println(correlMatrix)

  //Pearson vs Spearman Correlation among Series
  val ranks: RDD[Vector] = sc.parallelize(Array(
    Vectors.dense(1,2,3),
    Vectors.dense(5,6,4),
    Vectors.dense(7,8,9)))

  val corrPearsonMatrix: Matrix = Statistics.corr(ranks, "pearson")

  val corrSpearmanMatrix: Matrix = Statistics.corr(ranks, "spearman")

  println("corrPearsonMatrix:")
  println(corrPearsonMatrix)
  println("corrSpearmanMatrix:")
  println(corrSpearmanMatrix)
}
