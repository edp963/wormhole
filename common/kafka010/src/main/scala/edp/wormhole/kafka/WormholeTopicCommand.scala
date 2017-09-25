package edp.wormhole.kafka

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.admin.TopicCommand._
import kafka.common.{Topic, TopicExistsException}
import kafka.utils.{CommandLineUtils, ZkUtils}

object WormholeTopicCommand {

  def createTopic(zkUrl: String, topic: String, partition: Int = 1, refactor: Int = 1, sessionTimeOut: Int = 10000, connectionTimeout: Int = 10000): Unit = {
    val zkUtils = ZkUtils(zkUrl, sessionTimeOut, connectionTimeout, false)
    val topicCommand = new TopicCommandOptions(Array[String]("--create", "--zookeeper", s"$zkUrl", "--partitions", s"$partition", "--replication-factor", s"$refactor", "--topic", s"$topic"))
    createTopic(zkUtils, topicCommand)
  }

  def createTopic(zkUtils: ZkUtils, opts: TopicCommandOptions): Unit = {
    val topic = opts.options.valueOf(opts.topicOpt)
    val configs = parseTopicConfigsToBeAdded(opts)
    val ifNotExists = if (opts.options.has(opts.ifNotExistsOpt)) true else false
    if (Topic.hasCollisionChars(topic))
      println("WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.")
    try {
      if (opts.options.has(opts.replicaAssignmentOpt)) {
        val assignment = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
        warnOnMaxMessagesChange(configs, assignment.valuesIterator.next().length)
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignment, configs, update = false)
      } else {
        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt)
        val partitions = opts.options.valueOf(opts.partitionsOpt).intValue
        val replicas = opts.options.valueOf(opts.replicationFactorOpt).intValue
        warnOnMaxMessagesChange(configs, replicas)
        val rackAwareMode = if (opts.options.has(opts.disableRackAware)) RackAwareMode.Disabled
        else RackAwareMode.Enforced
        AdminUtils.createTopic(zkUtils, topic, partitions, replicas, configs, rackAwareMode)
      }
      println("Created topic \"%s\".".format(topic))
    } catch {
      case e: TopicExistsException => if (!ifNotExists) throw e
    }
  }
}
