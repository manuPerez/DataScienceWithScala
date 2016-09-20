import org.apache.spark.mllib.linalg.{Matrix, Matrices}

object EjemploMatrices extends App {
  /*
    LOCAL MATRICES:
      - Natural extension of Vectors
      - Stored in a single machine
   */

  /*
    DENSE MATRICES:
      - A "reshaped" dense vector
      - First two arguments specify dimensions of the matrix
      - Entries are stored in a single double array
   */

  val dm: Matrix = Matrices.dense(3, 2, Array(1, 3, 5, 2, 4, 6))

  println("dense matrix")
  println(dm)

  /*
    SPARSE MATRICES:
      - MLlib uses CSC format: Compressed Sparse Column

   */

  val sm =
    Matrices.sparse(5,                     //rows
                    4,                     //columns
                    Array(0, 0, 1, 2, 2),  //column pointers
                    Array(1, 3),          //row indices
                    Array(34, 55))         //non-zero values

  println("sparse matrix")
  println(sm)
}
