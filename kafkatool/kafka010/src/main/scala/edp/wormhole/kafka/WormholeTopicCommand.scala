/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */

package edp.wormhole.kafka

import kafka.admin.AdminUtils.getBrokerMetadatas
import kafka.admin.TopicCommand._
import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.utils.{CommandLineUtils, ZkUtils}

object WormholeTopicCommand {

  def createOrAlterTopic(zkUrl: String, topic: String, partition: Int = 1, refactor: Int = 1, sessionTimeOut: Int = 30000, connectionTimeout: Int = 30000): Unit = {
    val zkUtils = ZkUtils(zkUrl, sessionTimeOut, connectionTimeout, false)
    try {
      val topicCommand = new TopicCommandOptions(Array[String]("if-not-exists", "--create", "--zookeeper", s"$zkUrl", "--partitions", s"$partition", "--replication-factor", s"$refactor", "--topic", s"$topic"))
      createOrAlterTopic(zkUtils, topicCommand)
    } finally {
      zkUtils.close()
    }

  }

  def createOrAlterTopic(zkUtils: ZkUtils, opts: TopicCommandOptions): Unit = {
    val topic = opts.options.valueOf(opts.topicOpt)
    val configs = parseTopicConfigsToBeAdded(opts)
    val ifNotExists = if (opts.options.has(opts.ifNotExistsOpt)) true else false
    try {
      if (opts.options.has(opts.replicaAssignmentOpt)) {
        val assignment = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
//        warnOnMaxMessagesChange(configs, assignment.valuesIterator.next().length)
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignment, configs, update = true)
      } else {
        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt)
        val partitions = opts.options.valueOf(opts.partitionsOpt).intValue
        val replicas = opts.options.valueOf(opts.replicationFactorOpt).intValue
//        warnOnMaxMessagesChange(configs, replicas)
        val rackAwareMode = if (opts.options.has(opts.disableRackAware)) RackAwareMode.Disabled
        else RackAwareMode.Enforced
        val brokerMetadatas = getBrokerMetadatas(zkUtils, rackAwareMode)
        val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerMetadatas, partitions, replicas)
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, replicaAssignment, configs, update = true)
      }
    } catch {
      case _: kafka.admin.AdminOperationException =>
      case e: Exception => if (!ifNotExists) throw e
    }
  }

  def checkTopicExists(zkUrl: String, topic: String, sessionTimeOut: Int = 30000, connectionTimeout: Int = 30000): Boolean = {
    val zkUtils = ZkUtils(zkUrl, sessionTimeOut, connectionTimeout, false)
    try {
      AdminUtils.topicExists(zkUtils, topic)
    } catch {
      case ex: Exception =>
        throw new Exception(ex)
    } finally {
      zkUtils.close()
    }
  }
}
