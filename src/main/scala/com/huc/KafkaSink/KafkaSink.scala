package com.huc.KafkaSink

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.concurrent.Future


/**
 * Created with IntelliJ IDEA.
 *
 * @Project : SparkSqlTest2
 * @Package : com.huc.KafkaSink
 * @createTime : 2022/7/6 9:54
 * @author : huc
 * @Email : 1310259975@qq.com
 * @Description :
 */
object KafkaSink {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}

class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  lazy val producer: KafkaProducer[K, V] = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))
}
























