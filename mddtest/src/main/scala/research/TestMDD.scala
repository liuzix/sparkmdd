package research

import research.UnsafeGenericHandle
import com.google.common.reflect.TypeToken
import java.beans._
import scala.beans.BeanProperty


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



// this is a scala bean object. Getter and Setter automatically generated for all cons fields
class User(@BeanProperty var firstName: String, 
		   @BeanProperty var lastName: String, 
		   @BeanProperty var age: Int) {
  override def toString: String = return "%s %s, age %d".format(firstName, lastName, age)
}

class Player(@BeanProperty var age: Int, 
		   @BeanProperty var height: Double, 
		   @BeanProperty var weight: Double) extends java.io.Serializable {
  override def toString: String = return "Play %d %f %f".format(age, height, weight)
}


object TestMDD {
	def main(args: Array[String]): Unit = {
	    val conf = new SparkConf().setAppName("MDD Testing")
	    val sc = new SparkContext(conf)
	    println("^^^^^^^^^^^^^^^^^^^^^ Entering TESTING ROUTINE")
	    val players = (0 until 100).map(index => new Player(index, index * 1.0, index * 7.0))
	    val rdd = sc.parallelize(players)


	    val source: Class[_] = classOf[Player]
	    val className = source.getName

	    val mutableUnsafeRDD = rdd.mapPartitions { iter =>
	    	//val localBeanInfo = Introspector.getBeanInfo(Utils.classForName(className))
			val target = Class.forName(className, true, Option(Thread.currentThread().getContextClassLoader)
														.getOrElse(getClass.getClassLoader))			
			//val target: Class[_] = classOf[Player]
			val typetoken = TypeToken.of(target)
			val beanInfo = Introspector.getBeanInfo(typetoken.getRawType())
			val properties = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
			val conversions = (0 until properties.length).zip(properties)

			iter.map{elem =>
				val unsafe = new UnsafeGenericHandle(properties.length)
				conversions.map{ case (index, p) => initHandle(unsafe, index, 
					                                           typetoken.method(p.getReadMethod).getReturnType,
					                                           p.getReadMethod.invoke(elem).asInstanceOf[Any]) }
				unsafe
			}
	    }

	    val num = mutableUnsafeRDD.count()

	    // use only unsafeRDD to verify 

/*	    mutableUnsafeRDD.mapPartitions{iter => 
	    	iter.map{elem =>
	    		elem.setInt(0, 1000)
	    		elem
	    	}
	    }

	    val normalRDD = mutableUnsafeRDD.mapPartitions{iter => 
	    	iter.map{elem =>
	    		val first: Int = elem.getInt(0)
	    		first
	    	}
	    }

	    val taken = normalRDD.take(10)
	    taken.map(elem => println("expect 1000: %d".format(elem)))*/
		
		
		
/*		val target = Class.forName(className, true, Option(Thread.currentThread().getContextClassLoader)
													.getOrElse(getClass.getClassLoader))
		val wang = new Player(5, 2.0, 3.0) 
		// all should happen locally within the convert
		val typetoken = TypeToken.of(target)
		val beanInfo = Introspector.getBeanInfo(typetoken.getRawType())
		val properties = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")

		val unsafe = new UnsafeGenericHandle(properties.length)

		val conversions = (0 until properties.length).zip(properties)

		//conversions.map{case (index, p) => p.getReadMethod.invoke()}

		conversions.map{case (index, p) => initHandle(unsafe,
			                                          index, 
			                                          typetoken.method(p.getReadMethod).getReturnType,
			                                          p.getReadMethod.invoke(wang).asInstanceOf[Any]) }

		println("Age is %d".format(unsafe.getInt(0)))
		println("Height is %f".format(unsafe.getDouble(1)))
		println("Weight is %f".format(unsafe.getDouble(2)))*/

		println("End of mock")
	}


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

}


/*object Utility  {
	match 
}*/


/*def initHandle(handle: UnsafeGenericHandle, index: Int, 
	           fieldType: TypeToken[_], target: Any): Unit = {
	fieldType.getRawType match
	{
      case c: Class[_] if c == java.lang.Short.TYPE => println("Match type: short type")
      case c: Class[_] if c == java.lang.Integer.TYPE => println("Match type: int type")
      case c: Class[_] if c == java.lang.Long.TYPE => println("Match type: long type")
      case c: Class[_] if c == java.lang.Double.TYPE => println("Match type: double type")
      case c: Class[_] if c == java.lang.Byte.TYPE => println("Match type: byte type")
      case c: Class[_] if c == java.lang.Float.TYPE => println("Match type: float type")
      case c: Class[_] if c == java.lang.Boolean.TYPE => println("Match type: bool type")

      case c: Class[_] if c == classOf[java.lang.String] => println("Match type: string")
      case c: Class[_] if c == classOf[java.lang.Short] => println("Match type: short")
      case c: Class[_] if c == classOf[java.lang.Integer] => println("Match type: int ")
      case c: Class[_] if c == classOf[java.lang.Long] => println("Match type: long")
      case c: Class[_] if c == classOf[java.lang.Double] => println("Match type: double")
      case c: Class[_] if c == classOf[java.lang.Byte] => println("Match type: byte ")
      case c: Class[_] if c == classOf[java.lang.Float] => println("Match type: float")
      case c: Class[_] if c == classOf[java.lang.Boolean] => println("Match type: bool ")
      case _ => println("Match missed")
	}
	println("Exit init")
}*/