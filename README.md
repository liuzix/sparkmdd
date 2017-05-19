# sparkmdd
This is a proof-of-concept implementation of a mutable dataset for Spark. We aim to achieve in-place update of data with low memory management overhead. 

Currently the dataset is stored locally on each worker. We are able to persist the data in memory across operations by utilizing a thread-local data structure per JVM. 

# Thread-local Storage
Since we need the MDD's to persist across operations (e.g., when we need to calculate something else between two updates of the MDD), we cannot pass the MDD object with the closure, which will be destroyed after each operation. Then it becomes necessary to use a static storage object that is automatically created by the classloader. However, since each JVM may run multiple threads at the same time, a simple global object will face data races. The solution we adopt is to extend the ThreadLocal class to hold our custom MDD data. 

The detailed implementation is still under development, but conceptually it should look like this:

```scala
object WorkerLocalMap extends ThreadLocal[HashMap[String, MDD]]{
  override protected def initialValue = new HashMap[String, MDD]();
  def registerMDD (mdd: MDD, name: String) : Unit = {
    this.get().update(name, mdd)
  }
  
  def retrieveMDD (name: String) : MDD = {
    this.get().getOrElse(name, _ => throw new RuntimeException("No Such MDD"))
  }
}

```
