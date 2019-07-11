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


package edp.wormhole.externalclient.zookeeper

import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong
import org.apache.curator.framework.recipes.cache._
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.Logger
import org.apache.zookeeper.data.Stat

object WormholeZkClient {

  @volatile var zkClient: CuratorFramework = null

  private val logger = Logger.getLogger(this.getClass)

  private val retryLimit=3;  //由于zk server问题导致创建失败不抛出异常情况下的重试次数

  //  lazy val zookeeperPath:String = null
  def getZkClient(zkAddress: String): CuratorFramework = {
    if (zkClient == null) {
      synchronized {
        if (zkClient == null) {
          System.setProperty("zookeeper.sasl.client", "false")
          val retryPolicy = new ExponentialBackoffRetry(1000, 3)
          zkClient = CuratorFrameworkFactory.newClient(getZkAddress(zkAddress), retryPolicy)
          zkClient.start()
        }
      }
    }
    zkClient
  }

  def closeZkClient(): Unit = {
    try {
      if (zkClient != null) {
        zkClient.close()
      }
    } catch {
      case e: Throwable => println("zkClient.close error")
    }
  }

  def getNextAtomicIncrement(zkAddress: String, path: String): Long = {
    // todo if it failed? try catch not work
    val atomicLong = new DistributedAtomicLong(getZkClient(zkAddress), path, new ExponentialBackoffRetry(1000, 3))
    val newValue = atomicLong.increment()
    if (!newValue.succeeded()) {
      println("getAtomic Increment failed")
    }
    newValue.postValue()
  }

  def setPathChildrenCacheListener(zkAddress: String, path: String, add: (String, String, Long) => Unit, remove: String => Unit, update: (String, String, Long) => Unit): Unit = {
    val pathChildrenCache = new PathChildrenCache(getZkClient(zkAddress), getPath(zkAddress, path), true)
    pathChildrenCache.getListenable.addListener(new PathChildrenCacheListener() {
      @Override
      def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) {
        val data = event.getData
        if (data != null) {
          event.getType match {
            case PathChildrenCacheEvent.Type.CHILD_ADDED =>
              add(data.getPath, new String(data.getData), data.getStat.getMtime)
              println("NODE_ADDED : " + data.getPath + "  content:" + new String(data.getData) + " time:" + data.getStat.getMtime)
            case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
              remove(data.getPath)
              println("NODE_REMOVED : " + data.getPath)
            case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
              update(data.getPath, new String(data.getData), data.getStat.getMtime)
              println("CHILD_UPDATED : " + data.getPath + "  content:" + new String(data.getData) + " time:" + data.getStat.getMtime)
            case _ => println("event.getType=" + event.getType + " is not support")
          }
        }
        //        else {
        //          println("data is null : " + event.getType)
        //        }
      }
    })
    pathChildrenCache.start()

  }

  def setNodeCacheListener(zkAddress: String, path: String, add: (String, String, Long) => Unit, remove: String => Unit, update: (String, String, Long) => Unit): Unit = {
    val nodeCache = new NodeCache(getZkClient(zkAddress), getPath(zkAddress, path))
    nodeCache.getListenable.addListener(new NodeCacheListener() {
      @Override
      def nodeChanged() {
        if (nodeCache.getCurrentData != null) {
          val data = nodeCache.getCurrentData
          update(data.getPath, new String(data.getData), data.getStat.getMtime)
          println("NODE_CHANGED: " + data.getPath + ", content: " + new String(data.getData))
        }
      }
    })
    nodeCache.start()

  }

  //NOT USE YET
  def setTreeCacheListener(zkAddress: String, path: String, add: (String, String, Long) => Unit, remove: String => Unit, update: (String, String, Long) => Unit): Unit = {
    val treeCache = new TreeCache(getZkClient(zkAddress), getPath(zkAddress, path))
    treeCache.getListenable.addListener(new TreeCacheListener() {
      @Override
      def childEvent(client: CuratorFramework, event: TreeCacheEvent) {
        val data = event.getData
        if (data != null) {
          event.getType match {
            case TreeCacheEvent.Type.NODE_ADDED =>
              add(data.getPath, new String(data.getData), data.getStat.getMtime)
            //              println("NODE_ADDED : " + data.getPath + "  content:" + new String(data.getData) + " time:" + data.getStat.getMtime)
            case TreeCacheEvent.Type.NODE_REMOVED =>
              remove(data.getPath)
            //              println("NODE_REMOVED : " + data.getPath)
            case TreeCacheEvent.Type.NODE_UPDATED =>
              update(data.getPath, new String(data.getData), data.getStat.getMtime)
            //              println("NODE_UPDATED : " + data.getPath + "  content:" + new String(data.getData) + " time:" + data.getStat.getMtime)
            case _ =>
            //              println("event.getType=" + event.getType + " is not support")
          }
        }
        //        else {
        //          println("data is null : " + event.getType)
        //        }
      }
    })
    treeCache.start()

  }


  def createAndSetData(zkAddress: String, path: String, payload: Array[Byte]): Unit = {
    if (!checkExist(zkAddress, path)) {
      var retryCount:Int=0
      while(!checkExist(zkAddress, path) && retryCount < retryLimit){
        getZkClient(zkAddress).create().creatingParentsIfNeeded().forPath(getPath(zkAddress, path), payload)
        retryCount+=1
        Thread.sleep(1000);
      }
    } else {
      setData(zkAddress,path,payload)
    }
  }

  def createAndSetData(zkAddress: String, path: String, payload: String): Unit = {
    createAndSetData(zkAddress, path, payload.getBytes)
  }

  def createPath(zkAddress: String, path: String): Unit = {
    var retryCount:Int=0
    while (!checkExist(zkAddress, path) && retryCount < retryLimit) {
      getZkClient(zkAddress).create().creatingParentsIfNeeded().forPath(getPath(zkAddress, path))
      retryCount+=1
      Thread.sleep(1000);
    }
  }

  def getChildren(zkAddress: String, path: String): Seq[String] = {
    val list = getZkClient(zkAddress).getChildren.forPath(getPath(zkAddress, path))
    import scala.collection.JavaConverters._
    list.asScala
  }

  def setData(zkAddress: String, path: String, payload: String): Stat = {
    setData(zkAddress, path, payload.getBytes)
  }

  def setData(zkAddress: String, path: String, payload: Array[Byte]): Stat = {
    var retryCount:Int=0
    var result: Stat= new Stat()
    while(checkExist(zkAddress, path) && !(getData(zkAddress,path) sameElements payload) && retryCount < retryLimit){
      result = getZkClient(zkAddress).setData().forPath(getPath(zkAddress, path), payload)
      retryCount+=1
      Thread.sleep(1000)
    }
    result
  }

  def getData(zkAddress: String, path: String): Array[Byte] = {
    getZkClient(zkAddress).getData.forPath(getPath(zkAddress, path))
  }


  def delete(zkAddress: String, path: String): Unit = {
    if (checkExist(zkAddress, path)) {
      getZkClient(zkAddress).delete().deletingChildrenIfNeeded().forPath(getPath(zkAddress, path))
    }
  }

  def checkExist(zkAddress: String, path: String): Boolean = {
    getZkClient(zkAddress).checkExists().forPath(getPath(zkAddress, path)) != null
  }

  def getZkAddress(zkAddress: String): String = zkAddress.split(",").map(_.split("\\/")(0)).mkString(",")

  def getPath(zkAddress: String, path: String): String = {
    val temp = zkAddress.split(",")(0).split("\\/").drop(1).mkString("/") + path
    if (temp.startsWith("/")) temp else "/" + temp
  }

  def getInterProcessMutexLock(zkAddress: String, lockPath: String): InterProcessMutex = {
    new InterProcessMutex(getZkClient(zkAddress), lockPath)
  }
}
