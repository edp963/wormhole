/*
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

/**
 * Config 格式校验
 * key=value&key=value一行或多行（多行时用 & 连接） 或 key=value 多行（用 , 连接）
 */
export function onConfigValue (val) {
  let configVal = ''
  if (val.includes('&')) {
    configVal = val.includes('=') ? val.replace(/\n/g, '&') : val    // key=value&key=value
  } else {
    if (val.includes('=')) {
      // 多行输入 key=value
      const conTempStr = val.trim()
      const numArr = (conTempStr.split('=')).length - 1
      configVal = numArr === 1 ? val : val.replace(/\n/g, ',')
    } else {
      configVal = val
    }
  }
  return configVal
}
