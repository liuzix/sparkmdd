/**
  * Created by zixiong on 5/9/17.
  */

import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet

trait MDD {
  type V
  def map (f: V => V) : Unit
  def getIter () : Iterator[V]
  def FromIter (iter: Iterator[V]) : Unit
}

/*
class SimpleMDDList[V] extends MDDList {
  type T = V
  private def container : HashSet[V]
  def map (f: T => T) : Unit = {

  }
}
*/
object MDD {
  /*
  def fromRDD[T] (rdd : RDD[T]) : MDD[T] = {

  }*/
}
