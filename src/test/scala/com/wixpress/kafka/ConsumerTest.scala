package com.wixpress.kafka

import java.util.concurrent.atomic.AtomicInteger

import com.wixpress.greyhound.config.{GreyhoundConfig, KafkaProducerConfig}
import com.wixpress.greyhound.{KafkaDriver, KafkaProducerFactory}
import com.wixpress.iptf.AtomicSeq
import org.apache.kafka.clients.producer.ProducerRecord
import org.joda.time.DateTime
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

import scala.concurrent.duration.DurationInt

class ConsumerTest extends SpecificationWithJUnit {
  KafkaDriver.getInstance()
  val config = GreyhoundConfig.readFromFile
  val admin = new Admin(config.zookeeperAddress)

  "Consumer" should {
    "invoke user callback" in new Context {
      admin.createTopicIfNotExists("topic1", numOfPartitions)

      val numOfMessages = 100
      val counter = new AtomicInteger(0)

      val consumer = new Consumer(config.kafkaBrokers, "myGroup1", "topic1")(
        msg => counter.incrementAndGet()
      )

      consumer.startConsuming()
      produceMessages("topic1", numOfMessages)

      eventually(10, 1.second) {
        counter.get === numOfMessages
      }
    }

    "process messages in parallel" in new Context {
      override val numOfPartitions = 10
      admin.createTopicIfNotExists("topic2", numOfPartitions)
      val numOfMessages = 10
      val counter = new AtomicInteger(0)

      val consumer = new Consumer(config.kafkaBrokers, "myGroup2", "topic2")(
        msg => {
          Thread.sleep(1000)
          println(DateTime.now + " finished sleeping...")
          counter.incrementAndGet()
        }
      )

      consumer.startConsuming()
      produceMessages("topic2", numOfMessages, blocking = false)

      eventually(2500, 1.millis) {
        counter.get === numOfMessages
      }
    }

    "process messages once" in new Context {
      override val numOfPartitions = 100
      admin.createTopicIfNotExists("topic3", numOfPartitions)
      val numOfMessages = 500
      val counter = new AtomicSeq[Int]

      def makeConsumer() = new Consumer(config.kafkaBrokers, "myGroup3", "topic3")(msg => {
        Thread.sleep(500)
        counter.addAndGet(msg.toInt)
      })

      val consumer1 = makeConsumer()
      val consumer2 = makeConsumer()

      consumer1.startConsuming()
      produceMessages("topic3", numOfMessages / 2, blocking = false)

      // === NOW TRIGGER RE-BALANCE
      consumer2.startConsuming(sleepSome = false)
      produceMessages("topic3", numOfMessages / 2, blocking = false, from = numOfMessages / 2)

      eventually(20, 1.second) {
        println(counter.get.sorted.mkString(","))
        counter.get.sorted === (0 until numOfMessages).sorted
      }
    }
  }

  abstract class Context extends Scope {
    val numOfPartitions = 3
    val numOfMessages: Int

    private lazy val kafkaProducer = KafkaProducerFactory.aProducer(KafkaProducerConfig(config.kafkaBrokers, ""))

    def produceMessages(topic: String, numOfMessages: Int, blocking: Boolean = true, from: Int = 0): Unit = {
      (from until (from + numOfMessages)).foreach { i =>
        val future = kafkaProducer.send(new ProducerRecord[String, String](topic, new Integer(i % numOfPartitions), null, i.toString))
        if (blocking)
          future.get()
      }
    }
  }

}
