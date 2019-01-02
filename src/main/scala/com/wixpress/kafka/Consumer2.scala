package com.wixpress.kafka

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference

import com.wixpress.iptf.ScalaProperties.javaProperties
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class Consumer2(brokers: String, groupId: String, topic: String)(callback: String => Unit) {
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
    val offsets = new AtomicReference[Map[TopicPartition, OffsetAndMetadata]](Map.empty)

    Future {
      consumer.subscribe(Seq(topic).asJavaCollection)

      while (running) {
        val records = consumer.poll(10)
        records.asScala.foreach { record =>
          new Thread(() => {
            callback(record.value())
            offsets.updateAndGet(_ ++ Map(new TopicPartition(record.topic, record.partition) -> new OffsetAndMetadata(record.offset + 1)))
          }).start()

          val offsetsToSend = offsets.getAndSet(Map.empty)
          if (offsetsToSend.nonEmpty)
            consumer.commitSync(offsetsToSend)
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
