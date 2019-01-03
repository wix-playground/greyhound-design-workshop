package com.wixpress.kafka

import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{ArrayBlockingQueue, Executors, TimeUnit}
import java.util.function.UnaryOperator

import com.wixpress.framework.scala.collections.ConcurrentHashMap
import com.wixpress.iptf.ScalaProperties.javaProperties
import com.wixpress.kafka.Types.{Offset, Partition}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
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

  private val queues = new ConcurrentHashMap[Partition, SingleThreadExecutor]
  private val offsetsToCommit = new AtomicReference[Map[Partition, Offset]](Map.empty)

  def startConsuming(sleepSome: Boolean = true) = {
    running = true
    println("Starting consumer!")

    Future {
      consumer.subscribe(Seq(topic).asJavaCollection, new ConsumerRebalanceListener {
        override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {

        }

        override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {}
      })

      while (running) {
        val records = consumer.poll(10)
        records.asScala.foreach(executeTaskInOtherThread)
        maybeCommitOffsets()
      }

      consumer.close()
      println("Stopped consumer!")
    }

    if (sleepSome) Thread.sleep(2000)
  }

  private def executeTaskInOtherThread(record: ConsumerRecord[String, String]) = {
    queues.putIfAbsent(record.partition(), new SingleThreadExecutor(callback, offsetCallback(record.partition())))
    val queue = queues(record.partition())
    queue.executeTask(record)
  }

  private def maybeCommitOffsets() = {
    val offsets = offsetsToCommit.getAndSet(Map.empty)
    if (offsets.nonEmpty) {
      val newOffsets = offsets.map { case (partition, offset) => new TopicPartition(topic, partition) -> new OffsetAndMetadata(offset, "") }
      consumer.commitSync(newOffsets)
    }
  }

  private def offsetCallback(partition: Partition)(offset: Offset): Unit =
    offsetsToCommit.updateAndGet(_ + (partition -> offset))

  def stop() = {
    running = false
  }

  sys.addShutdownHook(stop())
}

private class SingleThreadExecutor(callback: String => Unit, offsetCallback: Offset => Unit) {
  private val queue = new ArrayBlockingQueue[ConsumerRecord[String, String]](10000)
  private val singleThreadExecutor = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  private val flag = new AtomicBoolean(false)

  def executeTask(msg: ConsumerRecord[String, String]) = {
    queue.put(msg)
  }

  Future {
    while (true) {
      val record = queue.take()
      callback(record.value())
      offsetCallback(record.offset() + 1)

    }
  }(singleThreadExecutor)

  // this is called from the main loop thread - don't worry
  def clearAndWait() = {
    queue.clear()

  }
}

object Types {
  type Partition = Int
  type Offset = Long
}