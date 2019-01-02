package com.wixpress.kafka

import java.util.concurrent.atomic.AtomicInteger

import com.wixpress.greyhound.KafkaDriver
import com.wixpress.greyhound.config.GreyhoundConfig
import com.wixpress.greyhound.producer.ProduceTarget.toPartition
import com.wixpress.greyhound.producer.builder.ProducerMaker.aProducer
import org.joda.time.DateTime
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

import scala.concurrent.Await
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
      admin.createTopicIfNotExists("topic2", numOfPartitions)
      val numOfMessages = 3
      val counter = new AtomicInteger(0)

      val consumer = new Consumer(config.kafkaBrokers, "myGroup2", "topic2")(
        msg => {
          Thread.sleep(1000)
          println(DateTime.now + " finished sleeping...")
          counter.incrementAndGet()
        }
      )

      consumer.startConsuming()
      produceMessages("topic2", numOfMessages)

      eventually(2, 1.second) {
        counter.get === numOfMessages
      }
    }

    "process messages once" in new Context {
      override val numOfPartitions = 100
      admin.createTopicIfNotExists("topic3", numOfPartitions)
      val numOfMessages = 500
      val counter = new AtomicInteger(0)

      def makeConsumer() = new Consumer2(config.kafkaBrokers, "myGroup3", "topic3")(msg => {
        Thread.sleep(150)
        println(DateTime.now + " finished sleeping...")
        counter.incrementAndGet()
      })

      val consumer1 = makeConsumer()
      val consumer2 = makeConsumer()

      consumer1.startConsuming()
      produceMessages("topic3", numOfMessages / 2, blocking = false)

      // === NOW TRIGGER RE-BALANCE
      consumer2.startConsuming(sleepSome = false)
      produceMessages("topic3", numOfMessages / 2)

      eventually(10, 1.second) {
        counter.get === numOfMessages
      }
    }
  }

  abstract class Context extends Scope {
    val numOfPartitions = 3
    val numOfMessages: Int

    private lazy val producer = aProducer().buffered.ordered.build

    def produceMessages(topic: String, numOfMessages: Int, blocking: Boolean = true): Unit = {
      (0 until numOfMessages).foreach { i =>
        val future = producer.produceToTopic(topic, "SomeMessage", toPartition(i % numOfPartitions))
        if (blocking)
          Await.ready(future, 1.second)
      }
    }
  }

}
