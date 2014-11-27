package eu.fakod.streamprocessing.verticles

import java.util.Properties
import eu.fakod.streamprocessing.kafka.{KafkaConsumer, KafkaConsumerFactory}
import eu.fakod.streamprocessing.verticles.helper.AvroTransform
import org.vertx.scala.platform.Verticle


/**
 * Verticle to work as a Kafka Consumer for all configured Topics
 * Converts messages from AVRO to JSON and sends them to the event bus
 */
class KafkaVerticle extends Verticle {

  val kafkaProperties = new Properties()
  var consumer: KafkaConsumer = null
  var avroTransform: AvroTransform = null

  override def start(): Unit = {
    kafkaProperties.put("zookeeper.connect", container.config.getString("zookeeper.connect"))
    kafkaProperties.put("group.id", container.config.getString("group.id"))

    val kafkaConsumerFactory = KafkaConsumerFactory.create(container.logger(), kafkaProperties)
    consumer = kafkaConsumerFactory.getConsumer(container.config.getString("group.id"), container.config.getString("topics"), handler _)

    avroTransform = new AvroTransform(container.logger())
  }

  private def handler(topic: String, message: Array[Byte]): Unit = {
    val result = avroTransform.transform(topic, message)
    vertx.eventBus.publish("mpg-feed." + topic, result)
  }

  override def stop(): Unit = try (consumer.stop) catch {
    case _: Throwable =>
  }
}



