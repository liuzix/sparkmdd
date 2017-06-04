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
	/* WARNING: MUST MUST create a no-param default constructor */
	def this() = {
		this(0, 0.0, 0.0)
	}
	def this(other: Player) = {
		this()
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
	    var num = rdd.count()
	    /* end timer */

	    sc.stop()
	    sc = new SparkContext(conf)
		rdd = sc.parallelize((0 until dataSize).map{index => 
			new Player(index, index * 1.0, index * 7.0)
		})

		val mdd = new UserMDD[Player]
		mdd.copyIn(rdd)
	    for (i <- 0 until numLoops) {
		    mdd.inPlace{ elem => 
	    		elem.setInt(0, elem.getInt(0) + 1)
	    		elem
		    }
	    }
	    /* start timer */
	    num = mdd.rDD.count()
	    /* end timer */
	    println("~~~~~~~~~~~~~~~~~ TEST FINISHED")
	}
}


object TestMDD {
	def main(args: Array[String]): Unit = {
		MDDTestSuite.test1()
	}
}





