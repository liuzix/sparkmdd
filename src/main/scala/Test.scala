/**
  * Created by zixiong on 5/8/17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/* This is not defined and does not compile
object TestComputation extends MDDComputation[Int] {
  def kernel (input: UnsafeMDD[Int]) : UnsafeMDD[Int] = {
    input.map(_ * 10)
    input
  }
}*/


object TestingParameters {
  val numPartitions = 1
  val arrayLength = 1000
  val multRatio = 1000
}

object WorkerLocal extends ThreadLocal[UnsafeMDD[Integer]]{
  override protected def initialValue = new UnsafeMDD[Integer](
    TestingParameters.arrayLength / TestingParameters.numPartitions)
}


object Test {

  /* I set up a Spark*/

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MDD Testing")
    val sc = new SparkContext(conf)

    println("^^^^^^^^^^^^^^^^^^^^^ Entering TESTING ROUTINE")

    val rdd = sc.parallelize(0 until TestingParameters.arrayLength, TestingParameters.numPartitions)
    rdd.foreachPartition(iter =>  WorkerLocal.get().FromIter(iter))
    sc.parallelize(0 until TestingParameters.numPartitions, 
                   TestingParameters.numPartitions)
      .foreach(_ => WorkerLocal.get().map(_ * TestingParameters.multRatio))
    val res = sc.parallelize(0 until TestingParameters.numPartitions, TestingParameters.numPartitions)
                .mapPartitions(_ => WorkerLocal.get().getIter(), true)
                .collect()
    println("^^^^^^^^^^^^^^^^^^^^^ START:::::::::::::::::::::Printing")
    for (i <- res) {
      println(i)
    }
    println("^^^^^^^^^^^^^^^^^^^^^ END:::::::::::::::::::::Printing")
  }
}
