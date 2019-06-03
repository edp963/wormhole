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

// 基本类型
const INT = 'int'
const LONG = 'long'
const FLOAT = 'float'
const DOUBLE = 'double'
const DECIMAL = 'decimal'
const STRING = 'string'
const BOOLEAN = 'boolean'
const DATETIME = 'datetime'
const BINARY = 'binary'

// 数组类型
const INTARRAY = 'intarray'
const LONGARRAY = 'longarray'
const FLOATARRAY = 'floatarray'
const DOUBLEARRAY = 'doublearray'
const DECIMALARRAY = 'decimalarray'
const STRINGARRAY = 'stringarray'
const BOOLEANARRAY = 'booleanarray'
const DATETIMEARRAY = 'datetimearray'
const BINARYARRAY = 'binaryarray'

// 对象类型
const JSONOBJECT = 'jsonobject'
const JSONARRAY = 'jsonarray'

// tuple 类型
const TUPLE = 'tuple'

// 获取非嵌套类型
export function getBaseType () {
  return [INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, BOOLEAN, DATETIME, BINARY,
    INTARRAY, LONGARRAY, FLOATARRAY, DOUBLEARRAY, DECIMALARRAY, STRINGARRAY, BOOLEANARRAY, DATETIMEARRAY, BINARYARRAY
  ]
}

// 获取嵌套类型
export function getNestType () {
  return [JSONOBJECT, JSONARRAY, TUPLE]
}

// just copy array, don't alter it
function copyArray (array) {
  const arrTemp = JSON.stringify(array, ['fieldName', 'fieldType', 'forbidden', 'rename', 'selected', 'ums_id_', 'ums_op_', 'ums_ts_'])
  // 重新设置key
  const umsArr = JSON.parse(arrTemp).map((s, index) => {
    s.key = index
    return s
  })
  return umsArr
}

// fieldType修改,用户选中修改类型后，不要修改原数组该index的fieldType值，直接调用该方法
// 选择的类型为tuple时，alterType="tuple##/##10"
export function fieldTypeAlter (array, index, alterType, type) {
  let newArray = copyArray(array)

  switch (type) {
    case 'source':
      if (array[index].fieldType.startsWith(TUPLE) && alterType.startsWith(TUPLE)) {
        newArray = tuple2tuple(newArray, index, alterType)
      } else {
        if (newArray[index].fieldType.startsWith(TUPLE)) {
          newArray = tuple2other(newArray, index, index + 1)
        } else if (newArray[index].fieldType.startsWith('json')) {
          newArray = jsonType2other(newArray, index, 'source')
        }

        if (alterType.startsWith(TUPLE)) {
          newArray = other2tuple(newArray, index, 0, alterType)
        } else if (alterType.startsWith('json')) {
          newArray = other2jsonType(newArray, index)
        }
      }
      break
    case 'sink':
      if (newArray[index].fieldType.startsWith('json')) {
        newArray = jsonType2other(newArray, index, 'sink')
      }
      if (alterType.startsWith('json')) {
        newArray = other2jsonType(newArray, index)
      }
      break
  }
  newArray[index].fieldType = alterType
  return newArray
}

// tuple类型修改为其他类型，删除原tuple子对象
function tuple2other (array, index, deleteIndex) {
  let newArray = copyArray(array)

  const tupleSubFieldRegrex = new RegExp(`^${newArray[index].fieldName}#[0-9]+$`)
  for (let i = index + 1; i < newArray.length; i++) {
    if (newArray[i].fieldName.search(tupleSubFieldRegrex) !== -1) {
      if (i >= deleteIndex) {
        newArray.splice(i, 1)
        i = i - 1
      }
    } else {
      break
    }
  }
  return copyArray(newArray)
}

// tuple类型修改为tuple，根据size大小调整子对象的行数
function tuple2tuple (array, index, alterType) {
  let newArray = copyArray(array)

  const preSize = Number(newArray[index].fieldType.split('##').pop())
  const alterSize = Number(alterType.split('##').pop())

  if (preSize === alterSize) {
    return newArray
  } else if (preSize > alterSize) {
    newArray = tuple2other(newArray, index, index + alterSize + 1)
    return newArray
  } else {
    newArray = other2tuple(newArray, index, preSize, alterType)
    return newArray
  }
}

// 其他类型修改为tuple
function other2tuple (array, index, preSize, alterType) {
  let newArray = copyArray(array)

  const size = Number(alterType.split('##').pop())
  const tupleArray = []
  for (let i = preSize; i < size; i++) {
    const object = {}
    object['fieldName'] = `${newArray[index].fieldName}#${i}`
    object['fieldType'] = 'string'
    object['selected'] = true
    object['rename'] = ''
    object['ums_id_'] = false
    object['ums_ts_'] = false
    object['ums_op_'] = ''
    object['forbidden'] = false
    tupleArray.push(object)
  }
  for (let i = 0; i < tupleArray.length; i++) {
    newArray.splice(index + preSize + i + 1, 0, tupleArray[i])
  }
  return copyArray(newArray)
}

// jsonobject/jsonarray类型修改为其他类型时，原嵌套子字段forbidden设置为true
function jsonType2other (array, index, type) {
  let newArray = copyArray(array)

  const prefix = `${newArray[index].fieldName}#`
  for (let i = index + 1; i < newArray.length; i++) {
    switch (type) {
      case 'source':
        if (newArray[i].fieldName.startsWith(prefix)) {
          newArray[i].forbidden = true
          newArray[i].selected = false
          newArray[i].ums_id_ = false
          newArray[i].ums_ts_ = false
          newArray[i].ums_op_ = ''
        } else {
          break
        }
        break
      case 'sink':
        if (newArray[i].fieldName.startsWith(prefix)) {
          newArray[i].forbidden = true
          newArray[i].selected = false
        } else {
          break
        }
        break
    }
  }
  return newArray
}

// 其他类型修改为jsonobject/jsonarray类型
function other2jsonType (array, index) {
  let newArray = copyArray(array)

  const prefix = `${newArray[index].fieldName}#`
  for (let i = index + 1; i < newArray.length; i++) {
    if (newArray[i].fieldName.startsWith(prefix)) {
      newArray[i].forbidden = false
      newArray[i].selected = true
    } else {
      break
    }
  }
  return newArray
}

// 若用户配置fieldName为 ums_id_或ums_ts_或ums_op_，将之前选为对应ums系统字段的行对象ums值设置为false，
// 将新选择的index所对应行的ums对应字段设置为true.value为对应ums字段的值，true或i:1,u:2,d:3
export function umsSysFieldSelected (array, index, umsSysField, value) {
  let newArray = copyArray(array)

  newArray = umsSysFieldCanceled(array, umsSysField)
  if (umsSysField === 'ums_id_') {
    newArray[index].ums_id_ = value
  } else if (umsSysField === 'ums_ts_') {
    newArray[index].ums_ts_ = value
  } else if (umsSysField === 'ums_op_') {
    newArray[index].ums_op_ = value
  }
  return newArray
}

// 若用户选择该行为ums_id_或ums_ts_或ums_op_后，又点了取消，调用 umsSysFieldCanceled 方法
export function umsSysFieldCanceled (array, umsSysField) {
  let newArray = copyArray(array)

  for (let i = 0; i < newArray.length; i++) {
    if (umsSysField === 'ums_id_') {
      newArray[i].ums_id_ = false
    } else if (umsSysField === 'ums_ts_') {
      newArray[i].ums_ts_ = false
    } else if (umsSysField === 'ums_op_') {
      newArray[i].ums_op_ = ''
    }
  }
  return newArray
}

// 修改rename
export function renameAlter (array, index, rename) {
  let newArray = copyArray(array)
  newArray[index].rename = rename
  return newArray
}
//
function genBaseField (fieldInfo, type) {
  const fieldArray = []
  const fieldObject = {}
  fieldObject['name'] = fieldInfo.fieldName.split('#').pop()
  fieldObject['type'] = fieldInfo.fieldType
  fieldObject['nullable'] = true
  // if (type === 'source') {
  if (fieldInfo.rename !== '' && fieldInfo.fieldName.split('#').pop() !== fieldInfo.rename) {
    fieldObject['rename'] = fieldInfo.rename
  }
  if (fieldInfo.fieldType && fieldInfo.fieldType.startsWith(TUPLE)) {
    fieldObject['type'] = TUPLE
    fieldObject['tuple_sep'] = fieldInfo.fieldType.split('##')[1]
  }
  fieldArray.push(fieldObject)
  // }
  if (type === 'source' && fieldInfo.ums_id_ || fieldInfo.ums_ts_ || (fieldInfo.ums_op_ && fieldInfo.ums_op_ !== '')) {
    const umsField = {}
    if (fieldInfo.ums_id_) {
      umsField['name'] = fieldObject.name
      umsField['type'] = fieldObject.type
      umsField['nullable'] = fieldObject.nullable
      umsField['rename'] = 'ums_id_'
    } else if (fieldInfo.ums_ts_) {
      umsField['name'] = fieldObject.name
      umsField['type'] = fieldObject.type
      umsField['nullable'] = fieldObject.nullable
      umsField['rename'] = 'ums_ts_'
    } else if (fieldInfo.ums_op_) {
      umsField['name'] = fieldObject.name
      umsField['type'] = fieldObject.type
      umsField['nullable'] = fieldObject.nullable
      umsField['rename'] = 'ums_op_'
      umsField['ums_sys_mapping'] = fieldInfo.ums_op_
    }
    fieldArray.push(umsField)
  }
  return fieldArray
}

  // 用户点保存后，最终table数组为array2，调用genSchema方法，生成Json
export function genSchema (array, type) {
  const fieldsObject = {}
  const fieldsArray = []
  fieldsObject['fields'] = fieldsArray
  const selectedArray = selectedFields(array)
  for (let i = 0; i < selectedArray.length; i++) {
    if (selectedArray[i].hasOwnProperty('fieldName') && !selectedArray[i].fieldName.includes('#')) {
      let fieldArray = genBaseField(selectedArray[i], type)
      fieldArray.forEach((fieldObject) => {
        if (fieldObject.type === JSONARRAY || fieldObject.type === JSONOBJECT || fieldObject.type.startsWith('tuple')) {
          fieldObject = genSubField(array.slice(i + 1, selectedArray.length), fieldObject, '', type)
        }
        fieldsArray.push(fieldObject)
      })
    }
  }
  return fieldsObject
}

function getDefaultNumType (value) {
  value = `${value}`
  if (!value.includes('.')) {
    return LONG
  } else {
    return DOUBLE
  }
}

function isExist (array, key) {
  for (let i in array) {
    if (array[i].fieldName === key) {
      return true
    }
  }
  return false
}

function sortNumber (a, b) {
  return a - b
}

function getRenameIndex (renameArray, rename) {
  for (let i = 0; i < renameArray.length; i++) {
    if (renameArray[i].rename === rename) {
      return i
    }
  }
  return -1
}

  // 用户点保存时，调用getRepeatFieldIndex方法，返回重复rename数组，检查rename字段是否有重复，
  // 若数组的length为0，表示无重复，否则提示rename重复的位置，数组中的值为rename重复的index
export function getRepeatFieldIndex (array) {
  let newArray = copyArray(array)
  const temp = newArray.filter(s => s.selected)

  const renameArray = []
  const repeatIndexArray = []
  for (let i = 0; i < temp.length; i++) {
    const p = getRenameIndex(renameArray, temp[i].rename)

    if (p === -1) {
      const renameObj = {}
      renameObj['index'] = temp[i].key
      renameObj['rename'] = temp[i].rename
      renameArray.push(renameObj)
    } else {
      if (!repeatIndexArray.includes(temp[i].key)) {
        repeatIndexArray.push(temp[i].key)
      }
      if (!repeatIndexArray.includes(renameArray[p].index)) {
        repeatIndexArray.push(renameArray[p].index)
      }
    }
  }
  return repeatIndexArray.sort(sortNumber)
}

function lastPositionOfKeyPrefix (array, key) {
  let p = -1
  const keyArray = key.split('#')
  const prefix = keyArray.slice(0, keyArray.length - 1).join('#')
  for (let i = 0; i < array.length; i++) {
    if (array[i].fieldName.startsWith(prefix)) {
      p = i
    }
  }
  return p
}

  // 点击保存时，去除 selected === false的行
function selectedFields (array) {
  for (let i = 0; i < array.length; i++) {
    if (array[i].selected === false) {
      array.splice(i, 1)
      i = i - 1
    }
  }
  return array
}

  // 生成基本字段数组array1，"jsonParseArray":array1）
export function jsonParse (jsonSample, prefix, array) {
  for (let key in jsonSample) {
    let data = {}
    if (jsonSample[key] && typeof jsonSample[key] === 'object') {
      if (jsonSample[key] instanceof Array) {
        prefix = prefix !== '' ? `${prefix}#${key}` : key
        data['fieldName'] = prefix
        data['fieldType'] = 'array'
      } else {
        prefix = prefix !== '' ? `${prefix}#${key}` : key
        data['fieldName'] = prefix
        data['fieldType'] = JSONOBJECT
      }
      array.push(data)
      jsonParse(jsonSample[key], prefix, array)
      const prefixArray = prefix.split('#')
      prefixArray.pop()
      prefix = prefixArray.join('#')
    } else {
      const fieldName = prefix !== '' ? `${prefix}#${key}` : key
      data['fieldName'] = fieldName
      const fieldType = typeof jsonSample[key]
      data['fieldType'] = fieldType === 'number' ? getDefaultNumType(jsonSample[key]) : fieldType
      array.push(data)
    }
  }
  return genFinalNameAndType(array)
}

function genFinalNameAndType (array) {
  const arrayTypeRegex = new RegExp('\\#\\d+$', 'g')
  const jsonArrayTypeRegex = new RegExp('(\\#\\d+\\#)+', 'g')
  for (let i = 0; i < array.length; i++) {
    if (array[i].fieldType === 'array') {
      if (i + 1 < array.length && array[i + 1].fieldType !== JSONOBJECT) {
        array[i].fieldType = `${array[i + 1].fieldType}array`
      } else {
        array[i].fieldType = JSONARRAY
      }
    }
    if (array[i].fieldName.search(arrayTypeRegex) !== -1) {
      array.splice(i, 1)
      i = i - 1
    }
    if (array[i].fieldName.search(jsonArrayTypeRegex) !== -1) {
      array[i].fieldName = array[i].fieldName.replace(jsonArrayTypeRegex, '#')
      const subArray = array.slice(0, i)
      if (isExist(subArray, array[i].fieldName)) {
        array.splice(i, 1)
        i = i - 1
      } else {
        let position = lastPositionOfKeyPrefix(subArray, array[i].fieldName)
        if (position !== -1 && position !== i - 1) {
          array.splice(position + 1, 0, array[i])
          array.splice(i + 1, 1)
        }
      }
    }
  }
  return array
}

  // 生成带有默认字段的完整数组，展示在表格中
export function genDefaultSchemaTable (array, type) {
  if (type === 'source') {
    let arrayFinal = array.map(i => {
      const temp = Object.assign(i, {
        selected: true,
        rename: i.fieldName.split('#').pop(),
        ums_id_: false,
        ums_ts_: false,
        ums_op_: '',
        forbidden: false
      })
      return temp
    })
    return arrayFinal
  } else {
    let arrayFinal = array.map(i => {
      const temp = Object.assign(i, {
        selected: true,
        forbidden: false
      })
      return temp
    })
    return arrayFinal
  }
}

function genSubField (array, fieldObject, prefix, type) {
  prefix = prefix === '' ? `${fieldObject.name}#` : `${prefix}${fieldObject.name}#`

  const subFieldsArray = fieldObject.hasOwnProperty('sub_fields') ? fieldObject['sub_fields'] : []

  for (let i = 0; i < array.length; i++) {
    if (array[i].hasOwnProperty('fieldName') && array[i].fieldName.startsWith(prefix)) {
      if (array[i].fieldType !== JSONARRAY && array[i].fieldType !== JSONOBJECT && !array[i].fieldType.startsWith('tuple')) {
        const fieldArray = genBaseField(array[i], type)
        fieldArray.forEach((fieldObject) => { subFieldsArray.push(fieldObject) })
      } else {
        let object = genBaseField(array[i], type)
        object = genSubField(array.slice(i + 1, array.length), object[0], prefix, type)
        subFieldsArray.push(object)
        const step = object.hasOwnProperty('sub_fields') ? object.sub_fields.length : 0
        i = i + step
      }
    } else {
      break
    }
  }
  fieldObject['sub_fields'] = subFieldsArray
  return fieldObject
}
