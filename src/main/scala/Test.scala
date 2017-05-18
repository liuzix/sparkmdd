/**
  * Created by zixiong on 5/8/17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TestComputation extends MDDComputation[Int] {
  def kernel (input: MutableSet[Int]) : MutableSet[Int] = {
    input.map(_ * 10)
    input
  }
}

object WorkerLocal extends ThreadLocal[MutableSet[Int]]{
  override protected def initialValue = new MutableSet[Int](100)
}


object Test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MDD Testing")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(0 until 1000, 10)
    rdd.foreachPartition(iter =>  WorkerLocal.get().FromIter(iter))
    sc.parallelize(0 until 10, 10).foreach(_ => WorkerLocal.get().map(_ * 1000))
    val res = sc.parallelize(0 until 10, 10).mapPartitions(_ => WorkerLocal.get().getIter(), true).collect()
    for (i <- res) {
      println(i)
    }
  }
}
