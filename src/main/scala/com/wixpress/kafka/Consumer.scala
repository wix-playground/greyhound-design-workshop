package com.wixpress.kafka

import java.util.concurrent.Executors

import com.wixpress.iptf.ScalaProperties.javaProperties
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

class Consumer(brokers: String, groupId: String, topic: String)(callback: String => Unit) {
  implicit private val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  private val props = javaProperties(
    "bootstrap.servers" → brokers,
    "group.id" → groupId,
    "auto.offset.reset" → "latest",
    "enable.auto.commit" → "false")

  private val consumer = new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)
  @volatile private var running = false

  def startConsuming(sleepSome: Boolean = true) = {
    running = true
    println("Starting consumer!")

    Future {
      consumer.subscribe(Seq(topic).asJavaCollection)

      while (running) {
        val records = consumer.poll(10)
        records.asScala.foreach { record =>
          new Thread(() => callback(record.value())).start()
          consumer.commitSync(Map(new TopicPartition(record.topic, record.partition) -> new OffsetAndMetadata(record.offset + 1)))
        }
      }

      consumer.close()
      println("Stopped consumer!")
    }

    if (sleepSome) Thread.sleep(2000)
  }

  def stop() = {
    running = false
  }

  sys.addShutdownHook(stop())
}
