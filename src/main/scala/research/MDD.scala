package research

import org.apache.spark.rdd.RDD
import com.google.common.reflect.TypeToken
import java.beans._
import scala.reflect.ClassTag


/**
  * Created by zixiong on 6/4/17.
  */
abstract class MDD[T : ClassTag] extends java.io.Serializable {
  var rDD : RDD[UnsafeGenericHandle] = null
  val className = implicitly[ClassTag[T]].runtimeClass.getName


  protected def toHandle(src: Iterator[T], className: String): Iterator[UnsafeGenericHandle]
  protected def toValue(src: Iterator[UnsafeGenericHandle], className: String): Iterator[T]

  def copyIn (input : RDD[T]) : Unit = {
    if (rDD != null) {
      throw new RuntimeException("MDD already occupied")
    }
    val className = implicitly[ClassTag[T]].runtimeClass.getName

    rDD = input.mapPartitions(iter => {
      toHandle(iter, className)
    }, true)
  }

  def copyOut(): RDD[T] = {
    if (rDD == null) {
      throw new RuntimeException("MDD not occupied")
    }
    val outRDD = rDD.mapPartitions(iter => {
      toValue(iter, className)
    }, true)
    rDD = null
    outRDD
  }


  def inPlace (f : UnsafeGenericHandle => UnsafeGenericHandle) : Unit = {
    if (rDD == null) {
      throw new RuntimeException("MDD not occupied")
    }
    rDD = rDD.map(f)
  }

}

