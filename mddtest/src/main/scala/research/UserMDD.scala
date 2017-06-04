package research

import java.beans.Introspector

import com.google.common.reflect.TypeToken

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD



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
      conversions.foreach{ case (index, p) => MDD.initHandle(unsafe, index,
                                                     typetoken.method(p.getReadMethod).getReturnType,
                                                     p.getReadMethod.invoke(elem).asInstanceOf[Any]) }
      unsafe
    }
  }

 def toValue(handle: UnsafeGenericHandle): T =  ???


}

