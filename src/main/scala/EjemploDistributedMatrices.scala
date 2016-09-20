import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object EjemploDistributedMatrices extends App{
  /*
    - Distributed matrices are where Spark starts to deliver significant value
    - They are stored in one or more RDD's
    - Three types have been implemented:
      - RowMatrix
      - IndexedRowMatrix
      - CoordinateMatrix
    - Conversions may require an expensive global shuffle
   */

  /*
    RowMatrix
      - The most basic type of distributed matrix
      - It has no meaningful row indices, being only a collection of feature vectors
      - Backed by an RDD of its rows, where each row is a local vector
      - Assumes the number of columns is small enough to be stored in a local vector
      - Can be easily created from an instance of RDD[Vector]
   */
  val conf = new SparkConf().setAppName("EjemploDistributedMatrices").setMaster("local")
  val sc = new SparkContext(conf)

  //First, I create an RDD of vectors called rows by parallelizing three dense vectors with two elements in each.
  val rowsRowMatrix: RDD[Vector] = sc.parallelize(Array(
    Vectors.dense(1,2),
    Vectors.dense(4,5),
    Vectors.dense(7,8)))

  //Now I create a new instance of RowMatrix, named mat, from the previous RDD
  val mat: RowMatrix = new RowMatrix(rowsRowMatrix)

  val m = mat.numRows()
  println("numRows: " + m)

  val n = mat.numCols()
  println("numCols: " + n)

  /*
    IndexedRowMatrix
      - Similar to RowMatrix, but it has meaningful row indices, which can be used for identifying rows
        and executing joins
      - Backed by an RDD of indexed rows, where each row is a tuple containing an index (long-typed) and a local vector
      - Easily created from an instance of RDD[IndexedRow]
      - Can be converted to a RowMatrix by calling toRowMatrix()
   */

  //First, I create an RDD of IndexedRows called rows by parallelizing three IndexedRows,
  // each one being a tuple of a long and dense vector.
  val rowsIndexedRowMatrix: RDD[IndexedRow] = sc.parallelize(Array(
    IndexedRow(0, Vectors.dense(1,2)),
    IndexedRow(1, Vectors.dense(4,5)),
    IndexedRow(2, Vectors.dense(7,8))))

  //Then, I create a new instance of IndexedRowMatrix named idxMat, from the previous RDD.
  val idxMat: IndexedRowMatrix = new IndexedRowMatrix(rowsIndexedRowMatrix)
  println("nCols: " + idxMat.numCols())
  println("nRows: " + idxMat.numRows())

  /*
    CoordinateMatrix
      - Should be used only when both dimensions are huge and the matrix is very sparse
      - Backed by an RDD of matrix entries, where each entry is a tuple (i: Long, j: Long, value: Double) where:
        - i is the row index
        - j is the column index
        - value is the entry value
      - Can be easily created from an instance of RDD[MatrixEntry]
      - Can be converted to an IndexedRowMatrix with sparse rows by calling toIndexedRowMatrix()
   */

  //I create an RDD of Matrix Entries called entries by parallelizing 3 Matrix Entries containing the coordinates
  // for the values I want to insert in the CoordinateMatrix.
  val entries: RDD[MatrixEntry] = sc.parallelize(Array(
    MatrixEntry(0,0,9),
    MatrixEntry(1,1,8),
    MatrixEntry(2,1,6)))

  val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
}
