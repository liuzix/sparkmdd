package research

import org.apache.spark.rdd.RDD

import research.UnsafeGenericHandle

import com.google.common.reflect.TypeToken

import java.beans._

import scala.reflect.ClassTag

object MDD {
  def initHandle(handle: UnsafeGenericHandle, index: Int, 
               fieldType: TypeToken[_], target: Any): Unit = {
    fieldType.getRawType match
    {
      case c: Class[_] if c == java.lang.Double.TYPE => {
        handle.setDouble(index, target.asInstanceOf[Double])
      }
      case c: Class[_] if c == java.lang.Integer.TYPE => {
        handle.setInt(index, target.asInstanceOf[Int])
      }
        case c: Class[_] if c == java.lang.Short.TYPE => {
        handle.setShort(index, target.asInstanceOf[Short])
      }
        case c: Class[_] if c == java.lang.Long.TYPE => {
        handle.setLong(index, target.asInstanceOf[Long])
      }
        case c: Class[_] if c == java.lang.Byte.TYPE => {
        handle.setByte(index, target.asInstanceOf[Byte])
      }
        case c: Class[_] if c == java.lang.Float.TYPE => {
        handle.setFloat(index, target.asInstanceOf[Float])
      }
        case c: Class[_] if c == java.lang.Boolean.TYPE => {
        handle.setBoolean(index, target.asInstanceOf[Boolean])
      }

      // let us separate the primitives from Boxed objects

        case c: Class[_] if c == classOf[java.lang.Short] => {
        handle.setShort(index, target.asInstanceOf[Short])
      }
        case c: Class[_] if c == classOf[java.lang.Integer] => {
        handle.setInt(index, target.asInstanceOf[Int])
      }
        case c: Class[_] if c == classOf[java.lang.Long] => {
        handle.setLong(index, target.asInstanceOf[Long])
      }
        case c: Class[_] if c == classOf[java.lang.Double] => {
        handle.setDouble(index, target.asInstanceOf[Double])
      }
        case c: Class[_] if c == classOf[java.lang.Byte] => {
        handle.setByte(index, target.asInstanceOf[Byte])
      }
        case c: Class[_] if c == classOf[java.lang.Float] => {
        handle.setFloat(index, target.asInstanceOf[Float])
      }
        case c: Class[_] if c == classOf[java.lang.Boolean] => {
        handle.setBoolean(index, target.asInstanceOf[Boolean])
      }

        case c: Class[_] if c == classOf[java.lang.String] => {
          println("Support String later")
        }
      case _ if fieldType.isArray => {
        println("Support array later")
      }
        case _ => println("Match missed")
    }
  }

  def toMDD(src: Iterator[_], className: String): Iterator[UnsafeGenericHandle] = {
    val target = Class.forName(className, true, Option(Thread.currentThread().getContextClassLoader)
                          .getOrElse(getClass.getClassLoader))      
    //val target: Class[_] = classOf[Player]
    val typetoken = TypeToken.of(target)
    val beanInfo = Introspector.getBeanInfo(typetoken.getRawType())
    val properties = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
    val conversions = (0 until properties.length).zip(properties)
    src.map{elem =>
      //println("Inside partition conversion")
      val unsafe = new UnsafeGenericHandle(properties.length)
      conversions.map{ case (index, p) => MDD.initHandle(unsafe, index, 
                                                              typetoken.method(p.getReadMethod).getReturnType,
                                                              p.getReadMethod.invoke(elem).asInstanceOf[Any]) }
      /*println("Age is %d".format(unsafe.getInt(0)))
      println("Height is %f".format(unsafe.getDouble(1)))
      println("Weight is %f".format(unsafe.getDouble(2)))*/
      unsafe
    }
  }
}


/**
  * Created by zixiong on 6/4/17.
  */
abstract class MDD[T : ClassTag] extends java.io.Serializable {
  var rDD : RDD[UnsafeGenericHandle] = null
  val className = implicitly[ClassTag[T]].runtimeClass.getName

  
  protected def toHandle(src: Iterator[T], className: String): Iterator[UnsafeGenericHandle]
  //protected def toHandle (v : T) : UnsafeGenericHandle
  def copyIn (input : RDD[T]) : Unit = {
    if (rDD != null) {
      throw new RuntimeException("MDD already occupied")
    }

    rDD = input.mapPartitions(iter => {
      toHandle(iter, className)
    }, true)
  }


  protected def toValue(src: Iterator[UnsafeGenericHandle], className: String): Iterator[T]

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

/*  protected def toValue (handle : UnsafeGenericHandle) : T
  def copyOut () : RDD[T] = {
    if (rDD == null) {
      throw new RuntimeException("MDD not occupied")
    }
    rDD.mapPartitions(iter => {
      iter.map(toValue)
    }, true)
  }*/



  def inPlace (f : UnsafeGenericHandle => UnsafeGenericHandle) : Unit = {
    if (rDD == null) {
      throw new RuntimeException("MDD not occupied")
    }
    rDD = rDD.map(elem => f(elem))
  }

/*  def inPlace (f : UnsafeGenericHandle => Unit) : Unit = {
    rDD.foreachPartition(_.map(f))
  }*/
}

