import scala.reflect.ClassTag
import UnsafeWrapper._
import scala.collection.JavaConversions._

/*
 * The code is tested on a Spark Standalone Cluster with 1 worker node.
 * Interoperating with Java Integer and Scala Int is extremely bug prone
 * The current implementation is limited to 1 type, i.e. Int. I am trying to work around this
 * 
 */


trait IntegerMDD {
  type V
  def map (f: Int => Int) : Unit
  def getIter () : Iterator[Integer]
  def FromIter (iter: Iterator[Int]) : Unit
}

class UnsafeMDD[T] (size: Long) (implicit m: ClassTag[T]) extends Serializable with  IntegerMDD {
  type V = T
  private var init = false

  // assumption (danger), object must be int
  private val arrayData = new DirectIntArray(size);

  // copy all the data given the iterator
  def FromIter (iter: Iterator[Int]) : Unit = {
    var index: Long = 0
    for (v <- iter) {
      arrayData.setValue(index, v.asInstanceOf[Integer])
      //arrayData(index) = v
      index += 1
    }
    if (init) throw new RuntimeException("MDD already initialized")
    init = true
  }

  // a mock map. This is not possible to implement.
  def map (f: Int => Int) : Unit =  {
    if (!init) {
      throw new RuntimeException("MDD not initialized")
    }
    for (i <- 0 until size.asInstanceOf[Int]) {
      arrayData.setValue(i, f(arrayData.getValue(i)))
      // arrayData(i) = f(arrayData(i))
    }
  }

  // well this iterator interface is a bit troublesome
  def getIter () = {
    asScalaIterator(arrayData.iterator)
  }
}
