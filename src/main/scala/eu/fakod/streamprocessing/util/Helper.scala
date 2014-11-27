package eu.fakod.streamprocessing.util

import java.util.concurrent.atomic.AtomicLong
import com.clearspring.analytics.hash.Lookup3Hash
import com.clearspring.analytics.stream.cardinality.AdaptiveCounting
import org.vertx.scala.core.json._
import scala.language.implicitConversions


/**
 * Threadsafe Long counter
 */
class Counter(value: AtomicLong) {
  def this() = this(new AtomicLong())

  /**
   * Increment the counter by one.
   */
  def incr(): Long = value.incrementAndGet

  /**
   * Increment the counter by `n`, atomically.
   */
  def incr(n: Int): Long = value.addAndGet(n)

  /**
   * Get the current value.
   */
  def apply(): Long = value.get()

  /**
   * Set a new value, wiping the old one.
   */
  def update(n: Long) = value.set(n)

  /**
   * Clear the counter back to zero.
   */
  def reset() = update(0L)

  override def toString() = value.get().toString
}


/**
 * adds isAlreadyCounted method to AdaptiveCounting
 */
class AdaptiveCountingPlus(M: Array[Byte]) extends AdaptiveCounting(M) {

  def this(k: Int) = this(new Array[Byte](1 << k))

  def this(ac: AdaptiveCounting) = this(ac.getBytes)

  def isAlreadyCounted(o: AnyRef): Boolean = {
    val x = Lookup3Hash.lookup3ycs64(o.toString)
    val j = (x >>> (java.lang.Long.SIZE - k)).asInstanceOf[Int]
    val r = (java.lang.Long.numberOfLeadingZeros((x << k) | (1 << (k - 1))) + 1).asInstanceOf[Byte]
    if (M(j) < r) {
      false
    }
    else
      true
  }

}

/**
 *
 */
object JsonObjectHelper {
  implicit def moreEasyGetObject(obj: JsonObject) = new JsonObjectHelper(obj)
}

/**
 *
 */
class JsonObjectHelper(obj: JsonObject) {
  def \(fieldName: String): JsonObject = {
    if (obj != null && fieldName != null)
      obj.getObject(fieldName)
    else
      null
  }
}
