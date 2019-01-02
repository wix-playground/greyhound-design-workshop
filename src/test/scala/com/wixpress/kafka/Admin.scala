package com.wixpress.kafka

import com.wixpress.greyhound.message.Partition
import com.wixpress.greyhound.{GreyhoundAdminSingletons, ZKStringSerializer}
import kafka.admin.AdminUtils
import org.I0Itec.zkclient.ZkClient

import scala.collection.Map
import scala.concurrent.duration.Duration
import scala.util.Try

class Admin(zookeeperAddress: String) {
  lazy private val zkUtils = GreyhoundAdminSingletons.zkUtilsFor(zookeeperAddress)

  def createTopicIfNotExists(topic: String, partitions: Int): Try[Unit] = {
    def createTopic(): Unit = {
      AdminUtils.createTopic(zkUtils, topic, partitions, 1)
      val retries = 100
      val backoffMs = 100
      val partitionLeaders: Map[Partition, Boolean] = waitForTopicToReachTheBroker(topic, partitions, retries, backoffMs)
      if (partitionLeaders.values.forall(foundLeaderForPartition => foundLeaderForPartition))
        Thread.sleep(100)

    }

    Try(createTopic())
  }

  private def waitForTopicToReachTheBroker(topic: String, numPartitions: Int, retries: Int, backoffMs: Int) =
  // wait until the update metadata request for new topic reaches all servers
    (0 until numPartitions).map(partition =>
      partition -> waitUntilLeaderIsElectedOrChanged(topic, partition, retries, backoffMs)).toMap

  private def waitUntilLeaderIsElectedOrChanged(topic: String, partition: Int, retries: Int, backOffMs: Int): Boolean = {
    val leaderFound = zkUtils.getLeaderForPartition(topic, partition).nonEmpty

    if (!leaderFound) Thread.sleep(backOffMs)

    leaderFound || (retries > 0 && waitUntilLeaderIsElectedOrChanged(topic, partition, retries - 1, backOffMs))
  }

  private def maxReplicationFactor(replicationFactor: Int): Int = {
    val availableBrokers = brokersInCluster.size
    math.min(replicationFactor, availableBrokers)
  }

  private def brokersInCluster = zkUtils.getAllBrokersInCluster()
}

object KafkaGreyhoundAdmin {
  val zookeeperConnectionTimeout: Int = KafkaUtils.zookeeperConnectionTimeout
  val defaultNumOfPartitions = 8
}

sealed trait WriteAssurance {
  def minInSyncReplicas: Int

  def uncleanLeaderElection: Boolean
}

private case object Durability extends WriteAssurance {
  override def minInSyncReplicas = 2

  override def uncleanLeaderElection = false
}

private case object Availability extends WriteAssurance {
  override def minInSyncReplicas = 1

  override def uncleanLeaderElection = true
}

object WriteAssurance {
  val durability: WriteAssurance = Durability
  val availability: WriteAssurance = Availability
}

trait TopicCompaction {
  val mode: String
}

object TopicCompaction {
  val compact: TopicCompaction = Compact
  val delete: TopicCompaction = Delete
}

private case object Compact extends TopicCompaction {
  override val mode = "compact"
}

private case object Delete extends TopicCompaction {
  override val mode = "delete"
}

case class TopicNotReadyYet(topic: String, timeout: Duration, partitions: Iterable[Partition]) extends Exception(s"partitions ${partitions.mkString(",")} don't have a broker leader for topic $topic after ${timeout.toMillis} milliseconds.")


object KafkaUtils {

  val zookeeperConnectionTimeout: Int = 20000

  def aZkClient(addresses: String, timeout: Int = zookeeperConnectionTimeout) =
    new ZkClient(addresses, 5000, timeout, ZKStringSerializer)

  def mkAddressString(addresses: Seq[String]): String = addresses.mkString(",")
}