package research


import research.MDD._
import research.UserMDD


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
	    val source: Class[_] = classOf[Player]
	    val className = source.getName

	    /* note we map by partition to avoid repeated lookup of class meta info */

	    val mdd1 = new UserMDD[Player]
	    mdd1.copyIn(rdd)

	    for (i <- 0 until 7) {
		    mdd1.inPlace{ elem => 
	    		elem.setInt(0, elem.getInt(0) + 1)
	    		elem
		    }
	    }

	    val taken = mdd1.rDD.take(10)
	    taken.map(elem => println(elem.getInt(0)))

		println("End of mock")
	}
}





