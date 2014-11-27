package eu.fakod.streamprocessing.util

import com.clearspring.analytics.stream.StreamSummary
import com.clearspring.analytics.stream.cardinality.AdaptiveCounting
import org.vertx.java.core.json.JsonArray
import org.vertx.scala.core.json.Json
import scala.collection.JavaConversions

/**
 * used to count access to something
 * stores a start time as well
 */
class AccessCounter(val count: Counter, val started: Long) extends Countable {
  def this() = this(new Counter(), System.currentTimeMillis())

  /**
   * increments the counter by one
   * @return Long The updated value
   */
  def incCount: Long = count.incr()

  /**
   * see trait
   */
  override def addThisToThatAndReturnThat(that: Countable): Countable = {
    val thatOne = that.asInstanceOf[AccessCounter]
    thatOne.count.update(count() + thatOne.count())
    thatOne
  }
}


/**
 * Uses adoptive counting to count unique users
 */
class UniqueUserCounter(val started: Long, val adaptiveCounting: AdaptiveCountingPlus) extends Countable {
  def this() = this(System.currentTimeMillis(), new AdaptiveCountingPlus(16))

  private def this(adaptiveCounting: AdaptiveCountingPlus) = this(System.currentTimeMillis(), adaptiveCounting)

  /**
   * @param item stream element
   * @return false if the value returned by cardinality() is unaffected by the appearance of o in the stream.
   */
  def offer(item: AnyRef): Boolean = adaptiveCounting.offer(item)

  /**
   * @return the number of unique elements in the stream or an estimate thereof
   */
  def getCardinality: Long = adaptiveCounting.cardinality()

  /**
   * @param item item to be checked
   * @return true, if item is already counted
   */
  def isAlreadyCounted(item: AnyRef): Boolean = adaptiveCounting.isAlreadyCounted(item)

  /**
   * see trait
   */
  override def addThisToThatAndReturnThat(that: Countable): Countable = {
    val thatOne = that.asInstanceOf[UniqueUserCounter]
    val ac = new AdaptiveCountingPlus(thatOne.adaptiveCounting.merge(this.adaptiveCounting).asInstanceOf[AdaptiveCounting])
    new UniqueUserCounter(ac)
  }
}


/**
 * uses Top-K to calculate cardinality
 */
class TopKCounter(capacity: Int) extends Countable {
  private def this() = this(50)

  private val streamSummary = new StreamSummary[String](capacity)

  /**
   * @param item stream element (<i>e</i>)
   * @return false if item was already in the stream summary, true otherwise
   */
  def offer(item: String): Boolean = streamSummary.offer(item)

  /**
   * @param k amount of requested elements
   * @return Buffer of requested elements
   */
  def topK(k: Int) = JavaConversions.asScalaBuffer(streamSummary.topK(k))

  /**
   * same as topK(k: Int) but returns JSON
   */
  def topKAsJson(k: Int): JsonArray = {
    val values = new JsonArray()
    topK(k).foreach {
      each =>
        values.add(Json.arr(each.getItem, each.getCount))
    }
    values
  }

  /**
   * see trait
   */
  override def addThisToThatAndReturnThat(that: Countable): Countable = {
    import scala.collection.JavaConversions._
    val thatOne = that.asInstanceOf[TopKCounter]
    streamSummary.topK(streamSummary.getCapacity).foreach {
      each =>
        thatOne.streamSummary.offer(each.getItem, each.getCount.toInt)
    }
    thatOne
  }
}

