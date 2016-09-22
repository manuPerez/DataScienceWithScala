package module.one.basic.statistics.data.types

import org.apache.spark.mllib.linalg.{Matrices, Matrix}

object EjemploLocalMatrices extends App {
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
    Matrices.sparse(5,                              //rows
                    4,                              //columns
                    Array(0, 1, 3, 4, 6),           //column pointers
                                                    // primer valor (0): ???
                                                    // segundo valor (1): indica que en la primera columna habrá un valor non-zero
                                                    // tercer valor (3): indica que en la segunda columna se añaden dos valores más non-zero (más lo anterior)
                                                    // cuarto valor (4): indica que en la tercera columna se añade un valor más non-zero (más lo anterior)
                                                    // quinto valor (6): indica que en la cuarta columna se añaden dos valores más non-zero (más lo anterior)
                    Array(0, 1, 2, 3, 2, 4),        //row indices
                    Array(34, 22, 11, 44, 33, 55))  //non-zero values

  println("sparse matrix")
  println(sm)
}
