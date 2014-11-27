package eu.fakod.streamprocessing.util

import scala.collection.mutable.ArrayBuffer
import scala.reflect._


/**
 * Interface for all Classes counting something
 */
trait Countable {

  /**
   * add this to the that instance and returns that
   * or a new instance 'of that'
   * @param that Countable
   * @return Countable
   */
  def addThisToThatAndReturnThat(that: Countable): Countable
}


/**
 * uses windowLengthInSlots of Countables to implement a sliding window
 * 'algorithm'.
 * This Class is not Threadsafe.
 */
class SlidingWindowCounter[T <: Countable : ClassTag](windowLengthInSlots: Int) {

  if (windowLengthInSlots < 2) {
    throw new IllegalArgumentException(
      "Window length in slots must be at least two (you requested " + windowLengthInSlots + ")")
  }

  private var headSlot = 0
  private var tailSlot = slotAfter(headSlot)
  private val slots = new Array[T](windowLengthInSlots)
  slots(headSlot) = createNewInstance

  private def createNewInstance = classTag[T].runtimeClass.newInstance.asInstanceOf[T]

  private def slotAfter(slot: Int): Int = (slot + 1) % windowLengthInSlots

  private def advanceHead(): Unit = {
    headSlot = tailSlot
    tailSlot = slotAfter(tailSlot)
  }

  /**
   * Get the currently active Countable instance
   * @return T
   */
  def getCountable: T = slots(headSlot)

  /**
   * Get all Countable instances as an Array of size windowLengthInSlots
   * @return Array[T]
   */
  def getAllCountables: Array[T] = {
    val buf = new ArrayBuffer[T]()
    var t = tailSlot
    for (i <- 0 to windowLengthInSlots - 1) {
      if (slots(t) != null)
        buf += slots(t)
      t = slotAfter(t)
    }
    buf.toArray
  }

  /**
   * Sums up all available Countables and returns the sum as a Countable
   * @return T
   */
  def getSumOfCountables: T = {
    val sumInstance = slots.fold(createNewInstance) {
      case (that, b) =>
        val result =
          if (b != null)
            b.addThisToThatAndReturnThat(that)
          else
            that
        result.asInstanceOf[T]
    }
    sumInstance
  }

  /**
   * Sums up all available Countables and returns the sum as a Countable
   * Advances window and sets the next Countable to active
   * @return T
   */
  def getSumOfCountablesThenAdvanceWindow: T = {
    val sumInstance = getSumOfCountables
    slots(tailSlot) = createNewInstance
    advanceHead()
    sumInstance
  }

  /**
   * Advances window and sets the next Countable to active
   */
  def advanceWindow(): Unit = {
    slots(tailSlot) = createNewInstance
    advanceHead()
  }
}


