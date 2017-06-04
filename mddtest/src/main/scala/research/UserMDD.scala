package research

import java.beans.Introspector

import com.google.common.reflect.TypeToken

import scala.reflect.ClassTag
import java.lang.reflect.Method

import org.apache.spark.rdd.RDD

import java.beans._


class UserMDD[T](implicit tag: ClassTag[T]) extends MDD[T] {

  def toHandle(src: Iterator[T], className: String): Iterator[UnsafeGenericHandle] = {
    /* the classloading info needs to be discovered in each partition */
    val target = Class.forName(className, true, Option(Thread.currentThread().getContextClassLoader)
                      .getOrElse(getClass.getClassLoader))      
    //val target: Class[_] = classOf[Player]
    val typetoken = TypeToken.of(target)
    val beanInfo = Introspector.getBeanInfo(typetoken.getRawType)
    val properties = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
    val conversions = properties.indices.zip(properties)
    src.map{elem =>
      //println("Inside partition conversion")
      val unsafe = new UnsafeGenericHandle(properties.length)
      conversions.foreach{ case (index, p) => initHandle(unsafe, index,
                                                         typetoken.method(p.getReadMethod).getReturnType,
                                                         p.getReadMethod.invoke(elem).asInstanceOf[Any]) }
      unsafe
    }
  }


  def toValue(src: Iterator[UnsafeGenericHandle], className: String): Iterator[T] = {
    /* the classloading info needs to be discovered in each partition */
    val target = Class.forName(className, true, Option(Thread.currentThread().getContextClassLoader)
                      .getOrElse(getClass.getClassLoader))      
    //val target: Class[_] = classOf[Player]
    val typetoken = TypeToken.of(target)
    val beanInfo = Introspector.getBeanInfo(typetoken.getRawType())
    val properties = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
    val conversions = (0 until properties.length).zip(properties)
    src.map { unsafeHandle =>
      //println("Inside partition conversion")
      val value = target.getConstructor().newInstance().asInstanceOf[T]
      //val value = target.newInstance()
      conversions.map{ case (index, p) => initValue(unsafeHandle, value, index, 
                                                     typetoken.method(p.getReadMethod).getReturnType, 
                                                     p.getWriteMethod)}
      value
    }
  }

  private def initValue(handle: UnsafeGenericHandle, value: T, index: Int,
                        fieldType: TypeToken[_], method: Method) 
  {
    fieldType.getRawType match {
      case c: Class[_] if c == java.lang.Double.TYPE =>
        method.invoke(value, handle.getDouble(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == java.lang.Integer.TYPE =>
        method.invoke(value, handle.getInt(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == java.lang.Short.TYPE =>
        method.invoke(value, handle.getShort(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == java.lang.Long.TYPE =>
        method.invoke(value, handle.getLong(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == java.lang.Byte.TYPE =>
        method.invoke(value, handle.getByte(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == java.lang.Float.TYPE =>
        method.invoke(value, handle.getFloat(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == java.lang.Boolean.TYPE =>
        method.invoke(value, handle.getBoolean(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == classOf[java.lang.Short] =>
        method.invoke(value, handle.getShort(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == classOf[java.lang.Integer] =>
        method.invoke(value, handle.getInt(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == classOf[java.lang.Long] =>
        method.invoke(value, handle.getLong(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == classOf[java.lang.Double] =>
        method.invoke(value, handle.getDouble(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == classOf[java.lang.Byte] =>
        method.invoke(value, handle.getByte(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == classOf[java.lang.Float] =>
        method.invoke(value, handle.getFloat(index).asInstanceOf[AnyRef])

      case c: Class[_] if c == classOf[java.lang.Boolean] =>
        method.invoke(value, handle.getBoolean(index).asInstanceOf[AnyRef])

      case _ => println("Match missed")
    }
  }

  private def initHandle(handle: UnsafeGenericHandle, index: Int,
                 fieldType: TypeToken[_], target: Any): Unit = {
    fieldType.getRawType match {
      case c: Class[_] if c == java.lang.Double.TYPE =>
        handle.setDouble(index, target.asInstanceOf[Double])

      case c: Class[_] if c == java.lang.Integer.TYPE =>
        handle.setInt(index, target.asInstanceOf[Int])

      case c: Class[_] if c == java.lang.Short.TYPE =>
        handle.setShort(index, target.asInstanceOf[Short])

      case c: Class[_] if c == java.lang.Long.TYPE =>
        handle.setLong(index, target.asInstanceOf[Long])

      case c: Class[_] if c == java.lang.Byte.TYPE =>
        handle.setByte(index, target.asInstanceOf[Byte])

      case c: Class[_] if c == java.lang.Float.TYPE =>
        handle.setFloat(index, target.asInstanceOf[Float])

      case c: Class[_] if c == java.lang.Boolean.TYPE =>
        handle.setBoolean(index, target.asInstanceOf[Boolean])


      // let us separate the primitives from Boxed objects

      case c: Class[_] if c == classOf[java.lang.Short] =>
        handle.setShort(index, target.asInstanceOf[Short])

      case c: Class[_] if c == classOf[java.lang.Integer] =>
        handle.setInt(index, target.asInstanceOf[Int])

      case c: Class[_] if c == classOf[java.lang.Long] =>
        handle.setLong(index, target.asInstanceOf[Long])

      case c: Class[_] if c == classOf[java.lang.Double] =>
        handle.setDouble(index, target.asInstanceOf[Double])

      case c: Class[_] if c == classOf[java.lang.Byte] =>
        handle.setByte(index, target.asInstanceOf[Byte])

      case c: Class[_] if c == classOf[java.lang.Float] =>
        handle.setFloat(index, target.asInstanceOf[Float])

      case c: Class[_] if c == classOf[java.lang.Boolean] =>
        handle.setBoolean(index, target.asInstanceOf[Boolean])

      case _ => println("Match missed")
    }
  }

/*  def copyOut () : RDD[T] = {
    if (rDD == null) {
      throw new RuntimeException("MDD not occupied")
    }
    rDD.mapPartitions(iter => {
      iter.map(toValue)
    }, true)
  }*/


}
