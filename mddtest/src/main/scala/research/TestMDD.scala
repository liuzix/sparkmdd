package research

import research.UserMDD

import scala.beans.BeanProperty
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.math.pow
import scala.reflect.ClassTag

// this is a scala bean object. Getter and Setter automatically generated for all cons fields
class User(@BeanProperty var firstName: String,
           @BeanProperty var lastName: String,
           @BeanProperty var age: Int) {
  override def toString: String = return "%s %s, age %d".format(firstName, lastName, age)
}

class Player(@BeanProperty var age: Int,
             @BeanProperty var height: Double,
             @BeanProperty var weight: Double) extends java.io.Serializable {
  /* WARNING: MUST MUST create a no-param default constructor */
  def this() = {
    this(0, 0.0, 0.0)
  }

  def this(other: Player) = {
    this()
    this.age = other.age
    this.height = other.height
    this.weight = other.weight
    //println("Copy cons")
  }

  override def toString: String = return "Play %d %f %f".format(age, height, weight)
}

object TestClass {
  def facN() : Narrow = new Narrow
  def facM() : Medium = new Medium
  def facW() : Wide = new Wide
}

class Narrow() extends java.io.Serializable {
  var field1: Double = 0
  var field2: Double = 0
  var field3: Double = 0
}

class Medium() extends java.io.Serializable {
  var field1: Double = 0
  var field2: Double = 0
  var field3: Double = 0
  var field4: Double = 0
  var field5: Double = 0
  var field6: Double = 0
  var field7: Double = 0
  var field8: Double = 0
  var field9: Double = 0
}

class Wide() extends java.io.Serializable {
  var field1: Double = 0
  var field2: Double = 0
  var field3: Double = 0
  var field4: Double = 0
  var field5: Double = 0
  var field6: Double = 0
  var field7: Double = 0
  var field8: Double = 0
  var field9: Double = 0
  var field10: Double = 0
  var field11: Double = 0
  var field12: Double = 0
  var field13: Double = 0
  var field14: Double = 0
  var field15: Double = 0
  var field16: Double = 0
  var field17: Double = 0
  var field18: Double = 0
  var field19: Double = 0
  var field20: Double = 0
  var field21: Double = 0
  var field22: Double = 0
  var field23: Double = 0
  var field24: Double = 0
  var field25: Double = 0
  var field26: Double = 0
  var field27: Double = 0
}

class ArrPlayer() extends java.io.Serializable {
  @BeanProperty var height: Double = 0.0
  @BeanProperty var vec: Array[Double] = (0.0 until 1000.0 by 1.0).toArray
}



object MDDTestSuite {

  def test0(dataSize: Int = 1000, numLoops: Int = 100) = {
    val conf = new SparkConf().setAppName("MDD Test 1")
    var sc = new SparkContext(conf)
    var rdd = sc.parallelize( List.fill(dataSize)(new ArrPlayer) )
    /*var rdd = sc.parallelize((0 until 100).map { index =>
      new ArrPlayer()
    })*/

    val mdd = new UserMDD[ArrPlayer]
    mdd.copyIn(rdd)
    for (i <- 0 until numLoops) {
      mdd.inPlace { elem =>
        elem.setDouble(0, elem.getInt(0) + 1)
        elem
      }
    }

    println("MDD:")
    StopWatch.start()
    var num = mdd.rDD.count()
    StopWatch.stop()

    sc.stop()
    sc = new SparkContext(conf)
    rdd = sc.parallelize( List.fill(dataSize)(new ArrPlayer) )

    for (i <- 0 until numLoops) {
      rdd = rdd.map { elem =>
        val newElem = new ArrPlayer()
        newElem
      }
    }


    println("Stock:")
    StopWatch.start()
    num = rdd.count()
    StopWatch.stop()

    
    sc.stop()

  }

  /* we have 3 varibles:
   * numFields: number of fields in the target data structure.
                This determines how costly it is to re-allocate the data across map
   * numLoops: number of loops to iterate over. The longer the costlier?
   * time: time taken to complete the job, disregarding starup
   */


  /* test 1: time vs numLoops. numFields constant */
  def test1[T <: AnyRef](fac: ()=> T, dataSize: Int = 100000, numLoops: Int = 200)(implicit tag : ClassTag[T]) = {
    /* note too many loops could cause stackOverflow */
    val conf = new SparkConf().setAppName("MDD Test 1")
    var sc = new SparkContext(conf)
    println("~~~~~~~~~~~~~~~~~ Entering TESTING ROUTINE 1 stock RDD vs MDD vs Safe Mode")
    println("Datasize = " + dataSize + " numLoops = " + numLoops)

    /*val players = (0 until dataSize).map{index =>
      new Player(index, index * 1.0, index * 7.0)
    }
    var rdd = sc.parallelize(players)
    */

    var rdd = sc.parallelize((0 until dataSize).map { index =>
      fac()
    })

    for (i <- 0 until numLoops) {
      rdd = rdd.map { elem =>
        val newElem = fac()
        newElem
      }
    }
    /* force execution */
    println("Stock:")
    StopWatch.start()
    var num = rdd.count()
    StopWatch.stop()

    sc.stop()
    sc = new SparkContext(conf)
    rdd = sc.parallelize((0 until dataSize).map { index =>
      fac()
    })

    val mdd = new UserMDD[T]
    mdd.copyIn(rdd)
    for (i <- 0 until numLoops) {
      mdd.inPlace { elem =>
        elem.setDouble(0, elem.getInt(0) + 1)
        elem
      }
    }
    println("MDD:")
    StopWatch.start()
    num = mdd.rDD.count()
    StopWatch.stop()


    sc.stop()
    sc = new SparkContext(conf)
    rdd = sc.parallelize((0 until dataSize).map { index =>
      fac()
    })
/*
    /* then operate under safe mode */
    val mdd2 = new UserMDD[T]
    mdd2.copyIn(rdd)
    for (i <- 0 until numLoops) {
      mdd2.inPlace { elem =>
        val dup = elem.duplicate()
        dup.setDouble(0, dup.getInt(0) + 1)
        dup
      }
    }
    println("Safe")
    StopWatch.start()
    num = mdd2.rDD.count()
    StopWatch.stop()
    */
    println("~~~~~~~~~~~~~~~~~ TEST  FINISHED")
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
  /* Compare Time vs NumLoops, numFields constant. numFields must be large to see difference
  def test4[T <: AnyRef](dataSize: Int = 100000, numLoops: Int = 200)(implicit tag : ClassTag[T]) = {
    /* note too many loops could cause stackOverflow */
    val conf = new SparkConf().setAppName("MDD Test 1")
    var sc = new SparkContext(conf)
    println("~~~~~~~~~~~~~~~~~ Entering TESTING ROUTINE 4 MDD vs Safe mode")
    println("Datasize = " + dataSize + " numLoops = " + numLoops)
    var rdd = sc.parallelize((0 until dataSize).map { index =>
      new T()
    })

    /* first operate under in-place mode */
    val mdd1 = new UserMDD[T]
    mdd1.copyIn(rdd)
    for (i <- 0 until numLoops) {
      mdd1.inPlace { elem =>
        elem.setInt(0, elem.getInt(0) + 1)
        elem
      }
    }
    StopWatch.start()
    var num = mdd1.rDD.count()
    StopWatch.stop()

    sc.stop()
    sc = new SparkContext(conf)
    rdd = sc.parallelize((0 until dataSize).map { index =>
      new T
    })

    /* then operate under safe mode */
    val mdd2 = new UserMDD[T]
    mdd2.copyIn(rdd)
    for (i <- 0 until numLoops) {
      mdd2.inPlace { elem =>
        val dup = elem.duplicate()
        dup.setInt(0, dup.getInt(0) + 1)
        dup
      }
    }
    StopWatch.start()
    num = mdd2.rDD.count()
    StopWatch.stop()
    println("~~~~~~~~~~~~~~~~~ TEST 4 FINISHED")
    sc.stop()
  }
  */
}


object TestMDD {
  def main(args: Array[String]): Unit = {
    /*for (i <- 0 until 7) {
      println("Type: Narrow")
      (0 to 4).foreach {j => MDDTestSuite.test1(TestClass.facN,10000 * pow(2, i).toInt, 50 * j) }
    }

    for (i <- 0 until 7) {
      println("Type: Medium")
      (0 to 4).foreach {j => MDDTestSuite.test1(TestClass.facM,10000 * pow(2, i).toInt, 50 * j) }
    }

    for (i <- 0 until 7) {
      println("Type: Wide")
      (0 to 4).foreach {j => MDDTestSuite.test1(TestClass.facW,10000 * pow(2, i).toInt, 50 * j) }
    }*/
    MDDTestSuite.test0()
  }

}

object StopWatch {
  private var time : Long = 0
  def start () : Unit = {
    time = System.nanoTime()
  }

  def stop () : Unit = {
    val elapsed = System.nanoTime() - time
    printf("Time passed: %s ns\n", elapsed)
  }
}




