package research

import research.UserMDD
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
	    sc.stop()
	}


	/* test 2: time vs numFields, Loops Constant */
	/* How to vary the numFields? We have not implemented array method yet */
	def test2() = {
		/* note too many loops could cause stackOverflow */
		val conf = new SparkConf().setAppName("MDD Test 1")
		var sc = new SparkContext(conf)
		println("~~~~~~~~~~~~~~~~~ Entering TESTING ROUTINE")
		
	    println("~~~~~~~~~~~~~~~~~ TEST FINISHED")
	    sc.stop()
	}

	/* For MDD: Time taken to update 1 field vs numFields
	 * Expected outcome: should be roughly constant since we are in-place
	 */
	def test3() = {
		val conf = new SparkConf().setAppName("MDD Test 1")
		var sc = new SparkContext(conf)
		println("~~~~~~~~~~~~~~~~~ Entering TESTING ROUTINE")
	    println("~~~~~~~~~~~~~~~~~ TEST FINISHED")
	    sc.stop()
	}


	/* MDD only, operating under In-place mode vs Safe Mode */
	/* Compare Time vs NumLoops, numFields constant. numFields must be large to see difference */
	def test4(dataSize : Int = 1000, numLoops : Int = 200) = {
		/* note too many loops could cause stackOverflow */
		val conf = new SparkConf().setAppName("MDD Test 1")
		var sc = new SparkContext(conf)
		println("~~~~~~~~~~~~~~~~~ Entering TESTING ROUTINE")

		var rdd = sc.parallelize((0 until dataSize).map{index => 
			new Player(index, index * 1.0, index * 7.0)
		})

		/* first operate under in-place mode */
		val mdd1 = new UserMDD[Player]
		mdd1.copyIn(rdd)
	    for (i <- 0 until numLoops) {
		    mdd1.inPlace{ elem => 
	    		elem.setInt(0, elem.getInt(0) + 1)
	    		elem
		    }
	    }

	    /* start timer */
	    var num = mdd1.rDD.count()
	    /* end timer */

	    sc.stop()
	    sc = new SparkContext(conf)
		rdd = sc.parallelize((0 until dataSize).map{index => 
			new Player(index, index * 1.0, index * 7.0)
		})

		/* then operate under safe mode */
		val mdd2 = new UserMDD[Player]
		mdd2.copyIn(rdd)
	    for (i <- 0 until numLoops) {
		    mdd2.inPlace{ elem => 
		    	val dup = elem.duplicate()
	    		dup.setInt(0, dup.getInt(0) + 1)
	    		dup
		    }
	    }
	    /* start timer */
	    num = mdd2.rDD.count()
	    /* end timer */
	    println("~~~~~~~~~~~~~~~~~ TEST FINISHED")
	    sc.stop()
	}
}


object TestMDD {
	def main(args: Array[String]): Unit = {
		MDDTestSuite.test1()
		MDDTestSuite.test4()
	}
}





