package eu.fakod.streamprocessing.kafka

import java.util
import java.util.concurrent.{ExecutorService, Executors}
import java.util.{Properties, UUID}

import kafka.consumer.{ConsumerConfig, KafkaStream}
import kafka.javaapi.consumer.ConsumerConnector
import org.vertx.scala.core.logging.Logger

object KafkaConsumerFactory {
  def create(LOG: Logger, configProps: Properties): KafkaConsumerFactory = {
    return new KafkaConsumerFactory(LOG, configProps)
  }
}

class KafkaConsumerFactory(LOG: Logger, configProps: Properties) {

  private val executorService: ExecutorService = Executors.newCachedThreadPool

  def getConsumer(groupId: String, topics: String, handler: (String, Array[Byte]) => Unit): KafkaConsumer = {
    return getConsumer(groupId, topics.split(",").toList, handler)
  }

  def getConsumer(groupId: String, topics: List[String], handler: (String, Array[Byte]) => Unit): KafkaConsumer = {
    var newgroupId = groupId
    if (newgroupId.isEmpty) {
      newgroupId = UUID.randomUUID().toString
      if (configProps.containsKey("group.id")) {
        newgroupId = String.format("%s-%s", configProps.getProperty("group.id"), newgroupId)
      }
    }
    val sessionProps = configProps.clone.asInstanceOf[Properties]
    sessionProps.setProperty("group.id", newgroupId)
    val consumer = new KafkaConsumer(LOG, new ConsumerConfig(sessionProps), executorService, topics, newgroupId, handler)
    consumer.start
    return consumer
  }
}


private[kafka] class KafkaConsumerTask(stream: KafkaStream[Array[Byte], Array[Byte]], handler: (String, Array[Byte]) => Unit) extends Runnable {

  def run {
    for (messageAndMetadata <- stream) {
      val topic = messageAndMetadata.topic
      val message = messageAndMetadata.message
      handler(topic, message)
    }
  }
}

class KafkaConsumer(LOG: Logger, consumerConfig: ConsumerConfig, executorService: ExecutorService,
                    topics: List[String], id: String,
                    handler: (String, Array[Byte]) => Unit) {

  var connector: ConsumerConnector = null


  def start {
    LOG.debug(s"Starting consumer for $id")
    connector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig)
    val topicCountMap = new util.HashMap[String, Integer]()
    for (topic <- topics) {
      topicCountMap.put(topic, 1)
    }
    val consumerMap = connector.createMessageStreams(topicCountMap)
    for (topic <- topics) {
      LOG.debug("Adding stream for session {}, topic {}", id, topic)
      val streams = consumerMap.get(topic)
      import scala.collection.JavaConversions._
      for (stream <- streams) {
        executorService.submit(new KafkaConsumerTask(stream, handler))
      }
    }
  }

  def stop {
    LOG.info(s"Stopping consumer for session $id")
    if (connector != null) {
      connector.commitOffsets
      try {
        Thread.sleep(5000)
      }
      catch {
        case ie: InterruptedException => {
          LOG.error(s"Exception while waiting to shutdown consumer: $ie.getMessage")
        }
      }
      LOG.debug(s"Shutting down connector for session $id")
      connector.shutdown
    }
    LOG.info(s"Stopped consumer for session $id")
  }
}




