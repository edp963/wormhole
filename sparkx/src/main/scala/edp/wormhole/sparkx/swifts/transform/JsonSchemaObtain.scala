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


package edp.wormhole.sparkx.swifts.transform

object JsonSchemaObtain {
  private [transform] def getJsonSchema(schem: String): String = {
    val temp = schem.split(",").map(kv => {
      val tuple = kv.split(":")
      if (tuple.size == 2) {
        val name = tuple(0).trim
        val typ = tuple(1).trim
        "{\"name\":\"" + name + "\",\"type\":\"" + typ + "\",\"nullable\":true}"
      } else {
        val name = tuple(0).trim
        val typ = tuple(1).trim
        val nullable = tuple(2).trim
        "{\"name\":\"" + name + "\",\"type\":\"" + typ + "\",\"nullable\":" + nullable + "}"
      }
    }).mkString(",")
    "{\"schema\": {\"namespace\": \"\",\"fields\":[" + temp + "]}}"
  }

}
