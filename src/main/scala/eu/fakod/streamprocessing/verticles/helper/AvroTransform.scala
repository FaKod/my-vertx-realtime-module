package eu.fakod.streamprocessing.verticles.helper

import java.util.Arrays

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DecoderFactory
import org.vertx.scala.core.json._
import org.vertx.scala.core.logging.Logger

/**
 * Transforms an Avro object to a JSON object
 */
class AvroTransform(LOG: Logger) {

  def transform(topic: String, message: Array[Byte]): JsonObject = {
    val b = Arrays.copyOfRange(message, 5, message.length)
    val decoder = DecoderFactory.get.binaryDecoder(b, null)

    var result: AnyRef = ""

    val s: Schema = topic match {
      case "my.kafka.topic.search" => null.asInstanceOf[Schema] // add your own Schema here
      case "my.kafka.topic.click" => null.asInstanceOf[Schema] // add your own Schema here
      case "my.kafka.topic.access" => null.asInstanceOf[Schema] // add your own Schema here
      case _ => return null
    }

    try {
      val reader = new GenericDatumReader[AnyRef](s)
      result = reader.read(null, decoder)
    }
    catch {
      case e: Exception =>
        LOG.error("Avro reader exception", e)
    }
    new JsonObject(result.toString)
  }
}
