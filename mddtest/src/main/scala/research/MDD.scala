package research

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


/**
  * Created by zixiong on 6/4/17.
  */
abstract class MDD[T : ClassTag] {
  var rDD : RDD[UnsafeGenericHandle] = null
  protected def toHandle (v : T) : UnsafeGenericHandle
  protected def toValue (handle : UnsafeGenericHandle) : T

  def copyIn (input : RDD[T]) : Unit = {
    if (rDD != null) {
      throw new RuntimeException("MDD already occupied")
    }
    rDD = input.mapPartitions(iter => {
      iter.map(toHandle)
    }, true)
  }

  def inPlace (f : UnsafeGenericHandle => Unit) : Unit = {
    rDD.foreachPartition(_.map(f))
  }

  def copyOut () : RDD[T] = {
    if (rDD == null) {
      throw new RuntimeException("MDD not occupied")
    }
    rDD.mapPartitions(iter => {
      iter.map(toValue)
    }, true)
  }
}
