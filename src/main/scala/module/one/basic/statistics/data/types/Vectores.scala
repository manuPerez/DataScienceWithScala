package module.one.basic.statistics.data.types

import org.apache.spark.mllib.linalg.Vectors

object Vectores extends App {
  /*
    DENSE VECTOR: is backed by a double array containing its values
   */
  val dv = Vectors.dense(44.0, 0.0, 55.0)

  println(dv)

  /*
    SPARSE VECTOR: is backed by two arrays:
      - an integer array representing the indexes
      - a double array containing its values
   */

  //by two separated arrays
  val sv1 = Vectors.sparse(3, Array(0, 2), Array(44.0, 55.0))

  //by a sequence of tuples
  val sv2 = Vectors.sparse(3, Seq((0, 44.0), (2, 55.0)))

  println(sv1)

  println(sv2)
}
