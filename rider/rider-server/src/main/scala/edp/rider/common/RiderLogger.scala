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


package edp.rider.common

import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.slf4j.impl.StaticLoggerBinder
import org.slf4j.{Logger, LoggerFactory}

trait RiderLogger {

  @transient private var riderLog : Logger = null

  protected def riderLogger: Logger = {
    if (riderLog == null) {
      initializeLog
      riderLog = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
    }
    riderLog
  }

  protected def initializeLog: Unit = {
    if (!RiderLogger.logInitialized) {
      RiderLogger.logLock.synchronized {
        if (!RiderLogger.logInitialized) {
          initializeLogging(false)
        }
      }
    }
  }

  private def initializeLogging(isInterpreter: Boolean): Unit = {
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    val usingLog4j12 = "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
    if (usingLog4j12) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      if (!log4j12Initialized) {
        val defaultLogProps = s"${RiderConfig.riderRootPath}/conf/log4j.properties"
        Option(getClass.getClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            PropertyConfigurator.configure(url)
            System.err.println(s"Using Rider conf dir log4j profile: $defaultLogProps")
          case None =>
            System.err.println(s"Rider was unable to load $defaultLogProps")
        }
      }
    }
    RiderLogger.logInitialized = true
    riderLogger
  }
}

private object RiderLogger {
  @volatile private var logInitialized = false
  val logLock = new Object()
  try {
    val bridgeClass = Class.forName("org.slf4j.bridge.SLF4JBridgeHandler", true, Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader))
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  } catch {
    case e: ClassNotFoundException =>
  }
}

