package eu.fakod.streamprocessing.verticles

import org.vertx.scala.platform.Verticle


class MainVerticle extends Verticle {

  override def start() = {
    val appConfig = container.config()

    val kafkaVerticleConfig = appConfig.getObject("KafkaVerticle")
    container.deployVerticle("scala:eu.fakod.streamprocessing.verticles.KafkaVerticle", kafkaVerticleConfig)

    container.deployVerticle("scala:eu.fakod.streamprocessing.verticles.DataVerticle")

    val WebSocketVerticleConfig = appConfig.getObject("WebSocketVerticle")
    container.deployVerticle("scala:eu.fakod.streamprocessing.verticles.WebSocketVerticle", WebSocketVerticleConfig)
  }
}
