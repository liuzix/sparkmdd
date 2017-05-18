import scala.reflect.ClassTag


class MutableSet[T] (size: Int) (implicit m: ClassTag[T]) extends Serializable with  MDD {
  type V = T
  private val arrayData = new Array[T](size)
  private var init = false
  def FromIter (iter: Iterator[T]) : Unit = {
    var index: Int = 0
    for (v <- iter) {
      arrayData(index) = v
      index += 1
    }
    if (init) throw new RuntimeException("MDD already initialized")
    init = true
  }

  def map (f: T => T) : Unit =  {
    if (!init) {
      throw new RuntimeException("MDD not initialized")
    }
    for (i <- 0 until size) {
      arrayData(i) = f(arrayData(i))
    }
  }

  def getIter () = {
    arrayData.iterator
  }
}
