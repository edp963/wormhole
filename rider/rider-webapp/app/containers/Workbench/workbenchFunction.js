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
 * 生成 step1 的 Source/Sink Namespace Cascader 所需数据源
 */
export function generateSourceSinkNamespaceHierarchy (system, result) {
  const snsHierarchy = []
  result.forEach(item => {
    if (item.nsSys.includes(system)) {
      let instance = snsHierarchy.find(i => i.value === item.nsInstance)
      if (!instance) {
        const newInstance = {
          value: item.nsInstance,
          label: item.nsInstance,
          children: []
        }
        snsHierarchy.push(newInstance)
        instance = newInstance
      }

      let database = instance.children.find(i => i.value === item.nsDatabase)
      if (!database) {
        const newDatabase = {
          value: item.nsDatabase,
          label: item.nsDatabase,
          children: []
        }
        instance.children.push(newDatabase)
        database = newDatabase
      }

      let table = database.children.find(i => i.value === item.nsTable)
      if (!table) {
        const newTable = {
          value: item.nsTable,
          label: item.nsTable,
          id: item.id,
          nsSys: item.nsSys
        }
        database.children.push(newTable)
      }
    }
  })
  return snsHierarchy
}

/**
 * 生成 step1 的 Hdfslog Source/Sink Namespace Cascader 所需数据源
 */
export function generateHdfslogNamespaceHierarchy (system, result) {
  // const snsHierarchy = result.length === 0
  //   ? []
  //   : [{
  //     value: '*',
  //     label: '*',
  //     children: [{
  //       value: '*',
  //       label: '*',
  //       children: [{
  //         value: '*',
  //         label: '*',
  //         nsSys: 'log'
  //       }]
  //     }]
  //   }]
  const snsHierarchy = []
  result.forEach(item => {
    if (item.nsSys.includes(system)) {
      let instance = snsHierarchy.find(i => i.value === item.nsInstance)
      if (!instance) {
        const newInstance = {
          value: item.nsInstance,
          label: item.nsInstance,
          nsSys: item.nsSys,
          children: []
          // children: [{
          //   value: '*',
          //   label: '*',
          //   children: [{
          //     value: '*',
          //     label: '*'
          //   }]
          // }]
        }
        snsHierarchy.push(newInstance)
        instance = newInstance
      }

      let database = instance.children.find(i => i.value === item.nsDatabase)
      if (!database) {
        const newDatabase = {
          value: item.nsDatabase,
          label: item.nsDatabase,
          children: [{
            value: '*',
            label: '*',
            nsSys: item.nsSys
          }]
        }
        instance.children.push(newDatabase)
        database = newDatabase
      }

      let table = database.children.find(i => i.value === item.nsTable)
      if (!table) {
        const newTable = {
          value: item.nsTable,
          label: item.nsTable,
          id: item.id,
          nsSys: item.nsSys
        }
        database.children.push(newTable)
      }
    }
  })
  return snsHierarchy
}

/**
 * 生成 transformation 中 的 Sink Namespace Cascader 所需数据源
 */
export function generateTransformSinkNamespaceHierarchy (system, result) {
  const snsHierarchy = []
  result.forEach(item => {
    if (item.nsSys.includes(system)) {
      let instance = snsHierarchy.find(i => i.value === item.nsInstance)
      if (!instance) {
        const newInstance = {
          value: item.nsInstance,
          label: item.nsInstance,
          children: []
        }
        snsHierarchy.push(newInstance)
        instance = newInstance
      }

      let database = instance.children.find(i => i.value === item.nsDatabase)
      if (!database) {
        const newDatabase = {
          value: item.nsDatabase,
          label: item.nsDatabase
          // children: []
        }
        instance.children.push(newDatabase)
        // database = newDatabase
      }

      // let permission = database.children.find(i => i.value === item.permission)
      // if (!permission) {
      //   const newPermission = {
      //     value: item.permission,
      //     label: item.permission
      //   }
      //   database.children.push(newPermission)
      // }
    }
  })
  return snsHierarchy
}

/**
 * Sink Config Msg
 * @param value
 * @returns {string}
 */
export function showSinkConfigMsg (value) {
  let sinkConfigMsgTemp = ''
  if (value === 'cassandra') {
    sinkConfigMsgTemp = 'For example: {"mutation_type":"iud"}'
  } else if (value === 'mysql' || value === 'oracle' || value === 'postgresql' || value === 'kudu') {
    sinkConfigMsgTemp = 'For example: {"mutation_type":"iud"}'
  } else if (value === 'es') {
    sinkConfigMsgTemp = 'For example: {"mutation_type":"iud", "_id": "id,name"}'
  } else if (value === 'hbase') {
    const temp = "'_'"
    sinkConfigMsgTemp = `For example: {"mutation_type":"iud","hbase.columnFamily":"cf","hbase.saveAsString": true, "hbase.rowKey":"hash(id1)+${temp}+value(id2)"}`
  } else if (value === 'mongodb') {
    sinkConfigMsgTemp = 'For example: {"mutation_type":"iud", "_id": "id,name"}'
  } else {
    sinkConfigMsgTemp = ''
  }
  return sinkConfigMsgTemp
}
