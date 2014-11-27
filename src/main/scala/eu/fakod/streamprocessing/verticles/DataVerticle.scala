package eu.fakod.streamprocessing.verticles

import eu.fakod.streamprocessing.util._
import org.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.json._
import org.vertx.scala.platform.Verticle


/**
 * Receives KafkaVerticle messages and processes them
 * Sends a Frontend-Data message to WebSocketVerticle
 */
class DataVerticle extends Verticle {

  val amountOfWindows = 50
  val websocketRefreshRate = 5
  val advanceWindowAfterSec = 60
  val overallWindowInSec = amountOfWindows * advanceWindowAfterSec

  /**
   * simple access counter
   */
  val accessCounter = new SlidingWindowCounter[AccessCounter](amountOfWindows)
  /**
   * Access Counter using adoptive counting
   */
  val uniqueUserCounter = new SlidingWindowCounter[UniqueUserCounter](amountOfWindows)
  /**
   * Top-k search term counter
   */
  val topSearchTerm = new SlidingWindowCounter[TopKCounter](amountOfWindows)

  override def start() = {

    vertx.eventBus.registerHandler("mpg-feed.my.kafka.topic.search", { message: Message[JsonObject] =>
      topSearchTerm.getCountable.offer(message.body().getString("term"))
    })

    vertx.eventBus.registerHandler("mpg-feed.my.kafka.topic.click", { message: Message[JsonObject] =>
      val ip = message.body().getString("IP")
      uniqueUserCounter.getCountable.offer(ip)
    })

    vertx.eventBus.registerHandler("mpg-feed.my.kafka.topic.access", { message: Message[JsonObject] =>
      accessCounter.getCountable.incCount
    })

    vertx.setPeriodic(advanceWindowAfterSec * 1000, { timerID: Long =>
      accessCounter.advanceWindow()
      topSearchTerm.advanceWindow()
      uniqueUserCounter.advanceWindow()
    })

    vertx.setPeriodic(websocketRefreshRate * 1000, { timerID: Long =>

      /**
       * creates an Array with access counts per window
       */
      val accessValues = new JsonArray()
      accessCounter.getAllCountables.foreach {
        av =>
          accessValues.add(Json.arr(av.started, av.count()))
      }

      /**
       * creates an Array with unique user per window
       */
      val uniqueUserValues = new JsonArray()
      uniqueUserCounter.getAllCountables.foreach {
        elem =>
          uniqueUserValues.add(Json.arr(elem.started, elem.getCardinality))
      }

      /**
       * creates an Array of top-k search terms
       */
      val searchStringValues = new JsonArray()
      topSearchTerm.getSumOfCountables.topK(50).foreach {
        each =>
          searchStringValues.add(Json.arr(each.getItem, each.getCount))
      }

      /**
       * creating the Frontend JSON object
       */
      val data = Json.obj(
        "topSearchString" -> searchStringValues,
        "accessChart" -> accessValues,
        "uniqueUserChart" -> uniqueUserValues
      )

      val js = Json.obj(
        "type" -> "DataVerticle",
        "overallWindowInSec" -> overallWindowInSec,
        "amountOfWindows" -> amountOfWindows,
        "advanceWindowAfterSec" -> advanceWindowAfterSec,
        "data" -> data
      )
      vertx.eventBus.publish("live-dashboard", js)
    })
  }
}
