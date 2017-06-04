package research


import research.MDD._


import scala.beans.BeanProperty

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import org.apache.spark.rdd



// this is a scala bean object. Getter and Setter automatically generated for all cons fields
class User(@BeanProperty var firstName: String, 
		   @BeanProperty var lastName: String, 
		   @BeanProperty var age: Int) {
  override def toString: String = return "%s %s, age %d".format(firstName, lastName, age)
}

class Player(@BeanProperty var age: Int, 
	         @BeanProperty var height: Double, 
	         @BeanProperty var weight: Double) extends java.io.Serializable 
{
	def this(other: Player) = {
		this(0, 0.0, 0.0)
		this.age = other.age
		this.height = other.height
		this.weight = other.weight
		println("Copy cons")
	}

	override def toString: String = return "Play %d %f %f".format(age, height, weight)
}






object MDDTestSuite {

	/* we have 3 varibles: 
	 * numFields: number of fields in the target data structure.
	              This determines how costly it is to re-allocate the data across map
	 * numLoops: number of loops to iterate over. The longer the costlier?
	 * time: time taken to complete the job, disregarding starup
	 */


	/* test 1: time vs numLoops. numFields constant */
	def test1(dataSize : Int = 1000, numLoops : Int = 200) = {
		/* note too many loops could cause stackOverflow */
		val conf = new SparkConf().setAppName("MDD Test 1")
		var sc = new SparkContext(conf)
		println("~~~~~~~~~~~~~~~~~ Entering TESTING ROUTINE")

		/*val players = (0 until dataSize).map{index => 
			new Player(index, index * 1.0, index * 7.0)
		}
		var rdd = sc.parallelize(players)
		*/

		var rdd = sc.parallelize((0 until dataSize).map{index => 
			new Player(index, index * 1.0, index * 7.0)
		})

	    for (i <- 0 until numLoops) {
	    	rdd = rdd.map{ elem =>
	    		val newElem = new Player(elem)
	    		newElem.setAge(newElem.getAge() + 1)
	    		newElem
	    	}
	    }
	    /* force execution */
	    /* start timer */
	    val num = rdd.count()
	    /* end timer */

	    sc.stop()
	    sc = new SparkContext(conf)
		rdd = sc.parallelize((0 until dataSize).map{index => 
			new Player(index, index * 1.0, index * 7.0)
		})

	    val source: Class[_] = classOf[Player]
	    val className = source.getName
	    val mutableUnsafeRDD = rdd.mapPartitions { iter =>
	    	MDD.toMDD(iter, className)
	    }
	}
}


object TestMDD {


	def main(args: Array[String]): Unit = {
/*		val p1 = new Player(5, 1.0, 2.0)
		val p2 = new Player(p1)
		p2.setAge(-100)
		println("p1 age is %d, while p2 age is %d".format(p1.getAge, p2.getAge))*/


		val conf = new SparkConf().setAppName("MDD Testing")
		val sc = new SparkContext(conf)
	    
	    println("^^^^^^^^^^^^^^^^^^^^^ Entering TESTING ROUTINE")
	    val players = (0 until 1000).map(index => new Player(index, index * 1.0, index * 7.0))
	    

	    var rdd = sc.parallelize(players)

/*	    for (i <- 0 until 200) {
	    	rdd = rdd.map{ elem =>
	    		val newElem = new Player(elem)
	    		newElem.setAge(newElem.getAge() + 1)
	    		newElem
	    	}
	    }
		val taken = rdd.take(1000)
		taken.map(elem => println("Age is %d".format(elem.getAge)))*/

/*	    val rdd2 = rdd.map{elem => 
	    	val newElem = new Player(elem)
	    	newElem.setAge(-100)
	    	newElem
	    }

		val taken = rdd.take(10)
		taken.map(elem => println("Age is %d".format(elem.getAge)))

		val taken2 = rdd2.take(10)
		taken2.map(elem => println("Age is %d".format(elem.getAge)))*/

		/* result both print -10 */
		/* This is a dangerous discovery. References are carried forward without re-allocation. 
		 * Thus, if I modify a downstream rdd, an upstream cached RDD will be affected
		 */


	    val source: Class[_] = classOf[Player]
	    val className = source.getName

	    /* note we map by partition to avoid repeated lookup of class meta info */

	    val mutableUnsafeRDD = rdd.mapPartitions { iter =>
	    	MDD.toMDD(iter, className)
	    }

	    var num = mutableUnsafeRDD.count()
	    println("there are %d".format(num))


	    val intermediary = mutableUnsafeRDD.map{elem => 
    		elem.setInt(0, 1000)
    		elem
	    }
	    val normalRDD = intermediary.map{elem => 
    		val first: Int = elem.getInt(0)
    		first
	    }

	    val taken2 = mutableUnsafeRDD.take(10)
	    println("~~~~~~~~~~~~~~~~~~~~~~~~~~  try to take")
	    val taken = normalRDD.take(10)
	    println("normal one")


	    taken.map(elem => println("expect 1000: %d".format(elem)))
	    println("special one")
	    println("length is %d".format(taken2.length))
	    for (i <- taken2) {
	    	println("what is this %d", i.getInt(0))
	    }

		println("End of mock")
	}
}





