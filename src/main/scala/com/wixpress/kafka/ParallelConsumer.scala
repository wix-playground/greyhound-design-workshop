package com.wixpress.kafka

import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ArrayBlockingQueue, Executors, Semaphore, TimeUnit}

import com.wixpress.framework.scala.collections.ConcurrentHashMap
import com.wixpress.iptf.ScalaProperties.javaProperties
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class ParallelConsumer(brokers: String, groupId: String, topic: String)(callback: String => Unit) {
  implicit private val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  private val props = javaProperties(
    "bootstrap.servers" → brokers,
    "group.id" → groupId,
    "auto.offset.reset" → "latest",
    "enable.auto.commit" → "false")

  private val consumer = new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)
  @volatile private var running = false
  val offsets = new AtomicReference[Map[TopicPartition, OffsetAndMetadata]](Map.empty)
  val workers = new ConcurrentHashMap[Int, WorkerThread]

  def startConsuming(sleepSome: Boolean = true) = {
    running = true
    println("Starting consumer!")

    Future {
      consumer.subscribe(Seq(topic).asJavaCollection, createRebalanceListener(workers))

      while (running) {
        try {
          val records = consumer.poll(10)
          records.asScala.foreach { record =>
            processRecord(offsets, workers, record)
            maybeCommitPendingOffsets(offsets)
          }

          if (records.isEmpty)
            maybeCommitPendingOffsets(offsets)
        } catch {
          case e:Throwable => e.printStackTrace()
        }
      }

      consumer.close()
      println("Stopped consumer!")
    }

    if (sleepSome) Thread.sleep(2000)
  }

  def createRebalanceListener(workers: ConcurrentHashMap[Int, WorkerThread]): ConsumerRebalanceListener =
    new ConsumerRebalanceListener {
      override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
        workers.values.foreach(_.waitAndClear(10000l))
        maybeCommitPendingOffsets(offsets)
      }

      override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {
        println("ASSIGNED=== " + collection.asScala.map(_.partition()).toSeq.sorted.mkString(","))
      }
    }

  private def processRecord(offsets: AtomicReference[Map[TopicPartition, OffsetAndMetadata]], workers: ConcurrentHashMap[Int, WorkerThread], record: ConsumerRecord[String, String]) = {
    maybeCreateWorker(workers, record, offsets)
    submitTask(workers, record)
  }

  private def maybeCommitPendingOffsets(offsets: AtomicReference[Map[TopicPartition, OffsetAndMetadata]]) = {
    val offsetsToSend = offsets.getAndSet(Map.empty)
    if (offsetsToSend.nonEmpty) {
      Try(consumer.commitSync(offsetsToSend)).recover { case e: Throwable =>
        e.printStackTrace()
      }
    }
  }

  private def submitTask(workers: ConcurrentHashMap[Int, WorkerThread], record: ConsumerRecord[String, String]) = {
    val worker = workers(record.partition())
    worker.submit(record)
  }

  private def maybeCreateWorker(workers: ConcurrentHashMap[Int, WorkerThread], record: ConsumerRecord[String, String],
                                offsets: AtomicReference[Map[TopicPartition, OffsetAndMetadata]]) = {
    workers.putIfAbsent(record.partition(), {
      val worker = new WorkerThread(record => {
        callback(record.value())
        offsets.updateAndGet(_ ++ Map(new TopicPartition(record.topic, record.partition) -> new OffsetAndMetadata(record.offset + 1)))
      })

      val thread = new Thread(worker)
      thread.start()
      worker
    })
  }

  def stop() = {
    running = false
  }

  sys.addShutdownHook(stop())
}

class WorkerThread(callback: ConsumerRecord[String, String] => Unit) extends Runnable {
  private val queue = new ArrayBlockingQueue[ConsumerRecord[String, String]](10000)
  private val semaphore = new Semaphore(1)

  def submit(record: ConsumerRecord[String, String]) = {
    queue.put(record)
  }

  def waitAndClear(timeoutMs: Long) = {
    queue.clear()
    waitUntilFinished(timeoutMs)
  }

  @tailrec
  private def waitUntilFinished(timeoutMs: Long): Unit = {
    if (semaphore.availablePermits() == 0 && timeoutMs >= 0) {
      Thread.sleep(100)
      waitUntilFinished(timeoutMs - 100)
    }
  }

  override def run(): Unit = {
    while (true) {
      val got = queue.poll(1000, TimeUnit.MILLISECONDS)
      for {
        msg <- Option(got)
        _ = semaphore.acquire(1)
        _ = callback(msg)
      } yield semaphore.release(1)
    }
  }
}