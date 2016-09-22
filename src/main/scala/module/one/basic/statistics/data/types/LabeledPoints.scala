package module.one.basic.statistics.data.types

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object LabeledPoints extends App{
  /*
    LABELED POINT:
      - The association of a vector with a corresponding label/response
      - Used in Supervised Machine Learning Algorithms.
        - Supervised learning: a machine is told the "correct" answers so
          it can look for similar patterns
        - Unsupervised learning: where the machine has to make intelligent
          guesses
      - Are stored as doubles so they can be used in both regression and
        classification problems
      - In classification problems, labels must be:
        - 0 (negative) or 1 (positive) for binary classification
        - class indices starting from zero (0, 1, 2...) for multiclass
   */

  val lbp1 = LabeledPoint(1.0, Vectors.dense(44.0, 0.0, 55.0))

  println(lbp1)

  val lbp2 = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(44.0, 55.0)))

  println(lbp2)
}
