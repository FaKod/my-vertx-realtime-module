package eu.fakod.streamprocessing.verticles

import org.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.http.ServerWebSocket
import org.vertx.scala.core.json._
import org.vertx.scala.platform.Verticle

/**
 * Verticle to create and feed one or many WebSocket connections
 */
class WebSocketVerticle extends Verticle {

  override def start() = {
    val streamName = container.config.getString("dashboard.path")

    vertx.createHttpServer().websocketHandler({ ws: ServerWebSocket =>
      ws.path() match {
        case p if p.equals("/" + streamName) =>
          val id = ws.textHandlerID()
          vertx.sharedData.getSet(streamName).add(id)

          ws.closeHandler({
            vertx.sharedData.getSet(streamName).remove(id)
          })
        case _ => ws.reject()
      }
    }).listen(container.config.getInteger("dashboard.port"))

    vertx.eventBus.registerHandler(streamName, { message: Message[JsonObject] =>
      vertx.sharedData.getSet(streamName).foreach {
        id: String =>
          vertx.eventBus.publish(id, message.body().toString)
      }
    })
  }
}
