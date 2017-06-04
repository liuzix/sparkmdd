package research

import java.beans.Introspector

import com.google.common.reflect.TypeToken

import scala.reflect.ClassTag

/**
  * Created by zixiong on 6/4/17.
  */
class UserMDD[T](implicit tag: ClassTag[T]) extends MDD[T] {
  private val target: Class[_] = tag.runtimeClass
  private val typetoken = TypeToken.of(target)
  private val beanInfo = Introspector.getBeanInfo(typetoken.getRawType)
  private val properties = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
  private val conversions = (0 until properties.length).zip(properties)

  def toHandle(v: T): UnsafeGenericHandle = {
    val unsafe = new UnsafeGenericHandle(properties.length)
    conversions.foreach { case (index, p) => initHandle(unsafe, index,
      typetoken.method(p.getReadMethod).getReturnType,
      p.getReadMethod.invoke(v).asInstanceOf[Any])
    }
    unsafe
  }

  def toValue(handle: UnsafeGenericHandle): T = {

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



}
