package edp.wormhole.kafka

import kafka.admin.AdminUtils.getBrokerMetadatas
import kafka.admin.TopicCommand._
import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.common.TopicExistsException
import kafka.utils.{CommandLineUtils, ZkUtils}

object WormholeTopicCommand {

  def createOrAlterTopic(zkUrl: String, topic: String, partition: Int = 1, refactor: Int = 1, sessionTimeOut: Int = 10000, connectionTimeout: Int = 10000): Unit = {
    val zkUtils = ZkUtils(zkUrl, sessionTimeOut, connectionTimeout, false)
    val topicCommand = new TopicCommandOptions(Array[String]("if-not-exists", "--create", "--zookeeper", s"$zkUrl", "--partitions", s"$partition", "--replication-factor", s"$refactor", "--topic", s"$topic"))
    createOrAlterTopic(zkUtils, topicCommand)
  }

  def createOrAlterTopic(zkUtils: ZkUtils, opts: TopicCommandOptions): Unit = {
    val topic = opts.options.valueOf(opts.topicOpt)
    val configs = parseTopicConfigsToBeAdded(opts)
    val ifNotExists = if (opts.options.has(opts.ifNotExistsOpt)) true else false
    try {
      if (opts.options.has(opts.replicaAssignmentOpt)) {
        val assignment = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
        warnOnMaxMessagesChange(configs, assignment.valuesIterator.next().length)
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignment, configs, update = true)
      } else {
        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt)
        val partitions = opts.options.valueOf(opts.partitionsOpt).intValue
        val replicas = opts.options.valueOf(opts.replicationFactorOpt).intValue
        warnOnMaxMessagesChange(configs, replicas)
        val rackAwareMode = if (opts.options.has(opts.disableRackAware)) RackAwareMode.Disabled
        else RackAwareMode.Enforced
        val brokerMetadatas = getBrokerMetadatas(zkUtils, rackAwareMode)
        val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerMetadatas, partitions, replicas)
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, replicaAssignment, configs, update = true)
      }
    } catch {
      case e: TopicExistsException => if (!ifNotExists) throw e
    }
  }
}
