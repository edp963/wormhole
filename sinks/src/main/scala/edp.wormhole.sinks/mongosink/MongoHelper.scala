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


package edp.wormhole.sinks.mongosink

import java.util.concurrent.TimeUnit

import org.mongodb.scala._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object MongoHelper {

  implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: (Document) => String = (doc) => doc.toJson
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: (C) => String = (doc) => doc.toString
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: (C) => String

    def results(): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))

    def headResult() = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))

    def printResults(initial: String = ""): Unit = {
      if (initial.length > 0) print(initial)
      results().foreach(res => println(converter(res)))
    }

    def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
  }

//  def getMongoId(tuple:Seq[String],sinkSpecificConfig: MongoConfig,schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): String ={
//    val _ids = ListBuffer.empty[String]
//    if (sinkSpecificConfig.`_id.get`.nonEmpty){
//      sinkSpecificConfig.`_id.get`.foreach(keyname => {
//        val (index, _, _) = schemaMap(keyname)
//        _ids += tuple(index)
//      })
//      _ids.mkString("_")
//    } else UUID.randomUUID().toString
//  }

}
