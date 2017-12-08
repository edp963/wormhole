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

// 对象
const JSONOBJECT = 'jsonobject'
const JSONARRAY = 'jsonarray'
const TUPLE = 'tuple'

// 获取非嵌套类型
export function getBaseType () {
  return [INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, BOOLEAN, DATETIME, BINARY,
    INTARRAY, LONGARRAY, FLOATARRAY, DOUBLEARRAY, DECIMALARRAY, STRINGARRAY, BOOLEANARRAY, DATETIMEARRAY, BINARYARRAY]
}

// 获取嵌套类型
export function getNestType () {
  return [JSONOBJECT, JSONARRAY, TUPLE]
}

// just copy array, don't alter it
function copyArray (array) {
  // 重新设置key
  const arrTemp = JSON.stringify(array, ['fieldName', 'fieldType', 'forbidden', 'rename', 'selected', 'ums_id_', 'ums_op_', 'ums_ts_'])
  const umsArr = JSON.parse(arrTemp).map((s, index) => {
    s.key = index
    return s
  })
  return umsArr
}

// fieldType修改,用户选中修改类型后，不要修改原数组该index的fieldType值，直接调用该方法
// 选择的类型为tuple时，alterType="tuple##/##10"
export function fieldTypeAlter (array, index, alterType) {
  var newArray = copyArray(array)

  if (newArray[index].fieldType.startsWith(TUPLE)) {
    newArray = tuple2other(newArray, index)
  } else if (newArray[index].fieldType.startsWith('json')) {
    newArray = jsonType2other(newArray, index)
  }

  if (alterType.startsWith(TUPLE)) {
    newArray = other2tuple(newArray, index, alterType)
  } else if (alterType.startsWith('json')) {
    newArray = other2jsonType(newArray, index)
  }
  newArray[index].fieldType = alterType
  return newArray
}

// tuple类型修改为其他类型，删除原tuple子对象
function tuple2other (array, index) {
  var newArray = copyArray(array)
  var tupleSubFieldRegrex = new RegExp(`^${newArray[index].fieldName}#[0-9]+$`)
  for (let i = index + 1; i < newArray.length; i++) {
    if (newArray[i].fieldName.search(tupleSubFieldRegrex) !== -1) {
      newArray.splice(i, 1)
      i = i - 1
    } else {
      break
    }
  }
  return copyArray(newArray)
}

// 其他类型修改为tuple
function other2tuple (array, index, alterType) {
  var newArray = copyArray(array)
  var num = Number(alterType.split('##').pop())
  var tupleArray = []
  for (let i = 0; i < num; i++) {
    var object = {}
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
    newArray.splice(index + 1, 0, tupleArray[i])
    index = index + 1
  }
  return copyArray(newArray)
}

// jsonobject/jsonarray类型修改为其他类型时，原嵌套子字段forbidden设置为true
function jsonType2other (array, index) {
  var newArray = copyArray(array)
  var prefix = `${newArray[index].fieldName}#`
  for (var i = index + 1; i < newArray.length; i++) {
    if (newArray[i].fieldName.startsWith(prefix)) {
      newArray[i].forbidden = true
      newArray[i].selected = false
      newArray[i].ums_id_ = false
      newArray[i].ums_ts_ = false
      newArray[i].ums_op_ = ''
    } else {
      break
    }
  }
  return newArray
}

// 其他类型修改为jsonobject/jsonarray类型
function other2jsonType (array, index) {
  var newArray = copyArray(array)
  var prefix = `${newArray[index].fieldName}#`
  for (var i = index + 1; i < newArray.length; i++) {
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
  var newArray = copyArray(array)
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

// 若用户选择该行为ums_id_或ums_ts_或ums_op_后，又点了取消，调用umsSysFieldUnSelected方法
function umsSysFieldCanceled (array, umsSysField) {
  var newArray = copyArray(array)
  for (let i = 0; i < newArray.length; i++) {
    if (umsSysField === 'ums_id_') {
      newArray[i].ums_id_ = false
      break
    } else if (umsSysField === 'ums_ts_') {
      newArray[i].ums_ts_ = false
      break
    } else if (umsSysField === 'ums_op_') {
      newArray[i].ums_op_ = ''
      break
    }
  }

  return newArray
}

// 修改rename
export function renameAlter (array, index, rename) {
  var newArray = copyArray(array)
  newArray[index].rename = rename
  return newArray
}

function genUmsField (array) {
  var newArray = copyArray(array)
  var umsArray = []
  for (let i = 0; i < newArray.length; i++) {
    if (newArray[i].ums_id_ === true || newArray[i].ums_ts_ === true || newArray[i].ums_op_ !== '') {
      var object = genBaseField(newArray[i])
      if (newArray[i].ums_id_ === true) {
        object.rename = 'ums_id_'
      } else if (newArray[i].ums_ts_ === true) {
        object.rename = 'ums_ts_'
      } else {
        object.rename = 'ums_op_'
        object.ums_sys_mapping = newArray[i].ums_op_
      }
      umsArray.push(object)
    }
  }
  return umsArray
}

function genBaseField (fieldInfo) {
  var fieldObject = {}
  fieldObject['name'] = fieldInfo.fieldName.split('#').pop()
  fieldObject['type'] = fieldInfo.fieldType
  fieldObject['nullable'] = true
  if (fieldInfo.rename !== '' && fieldInfo.fieldName.split('#').pop() !== fieldInfo.rename) {
    fieldObject['rename'] = fieldInfo.rename
  }
  if (fieldInfo.fieldType.startsWith(TUPLE)) {
    fieldObject['type'] = TUPLE
    fieldObject['tuple_sep'] = fieldInfo.fieldType.split('##')[1]
  }

  return fieldObject
}

// 用户点保存后，最终table数组为array2，调用genSchema方法，生成Json
export function genSchema (array) {
  var fieldsObject = {}
  var fieldsArray = []
  fieldsObject['fields'] = fieldsArray
  var selectedArray = selectedFields(array)
  for (var i = 0; i < selectedArray.length; i++) {
    if (selectedArray[i].hasOwnProperty('fieldName') && selectedArray[i].fieldName.indexOf('#') === -1) {
      var fieldObject = genBaseField(selectedArray[i])
      if (fieldObject.type === JSONARRAY || fieldObject.type === JSONOBJECT || fieldObject.type.startsWith('tuple')) {
        fieldObject = genSubField(array.slice(i + 1, selectedArray.length), fieldObject, '')
      }
      fieldsArray.push(fieldObject)
    }
  }
  var umsArray = genUmsField(array)
  for (let i = 0; i < umsArray.length; i++) {
    fieldsArray.push(umsArray[i])
  }
  return fieldsObject
}
//
//
//
//
//
//
//

function getDefaultNumType (value) {
  value = `${value}`
  if (value.indexOf('.') === -1) return LONG
  else return DOUBLE
}

function isExist (array, key) {
  for (var i in array) {
    if (array[i].fieldName === key) {
      return true
    }
  }
  return false
}

function sortNumber (a, b) {
  return a - b
}

// 用户点保存时，调用getRepeatFieldIndex方法，返回重复rename数组，检查rename字段是否有重复，
// 若数组的length为0，表示无重复，否则提示rename重复的位置，数组中的值为rename重复的index
export function getRepeatFieldIndex (array) {
  var renameArray = []
  var repeatIndexArray = []
  for (var i = 0; i < array.length; i++) {
    var p = renameArray.indexOf(array[i].rename)
    if (p !== -1 && p !== i) {
      if (repeatIndexArray.indexOf(i) === -1) {
        repeatIndexArray.push(p)
        repeatIndexArray.push(i)
      }
    } else renameArray.push(array[i].rename)
  }
  return repeatIndexArray.sort(sortNumber)
}

function lastPositionOfKeyPrefix (array, key) {
  var p = -1
  var keyArray = key.split('#')
  var prefix = keyArray.slice(0, keyArray.length - 1).join('#')
  for (var i = 0; i < array.length; i++) {
    if (array[i].fieldName.startsWith(prefix)) {
      p = i
    }
  }
  return p
}

// 点击保存时，去除 selected === false的行
function selectedFields (array) {
  for (var i = 0; i < array.length; i++) {
    if (array[i].selected === false) {
      array.splice(i, 1)
      i = i - 1
    }
  }
  return array
}

// 当row select的项有子级（如test#t1）时，
// 若 selected=true，子级的 forbidden=false；若 selected=false，子级的 forbidden=true, selected=false
// todo:edit 过滤掉forbidden = true
export function rowSelectFunc (array, index) {
  const prefix = `${array[index].fieldName}#`
  for (let i = index + 1; i < array.length; i++) {
    if (array[i].fieldName.startsWith(prefix)) {
      if (array[index].selected) {
        array[i].forbidden = false
      } else {
        array[i].forbidden = true
        array[i].selected = false
      }
    } else {
      break
    }
  }
  return array
}

// 生成基本字段数组array1，"jsonParseArray":array1）
export function jsonParse (jsonSample, prefix, array) {
  for (var key in jsonSample) {
    var data = {}
    if (jsonSample[key] && typeof jsonSample[key] === 'object') {
      if (jsonSample[key] instanceof Array) {
        if (prefix !== '') prefix = `${prefix}#${key}`
        else prefix = key
        data['fieldName'] = prefix
        data['fieldType'] = 'array'
      } else {
        if (prefix !== '') prefix = `${prefix}#${key}`
        else prefix = key
        data['fieldName'] = prefix
        data['fieldType'] = JSONOBJECT
      }
      array.push(data)
      jsonParse(jsonSample[key], prefix, array)
      var prefixArray = prefix.split('#')
      prefixArray.pop()
      prefix = prefixArray.join('#')
    } else {
      if (prefix !== '') var fieldName = `${prefix}#${key}`
      else fieldName = key
      data['fieldName'] = fieldName
      var fieldType = typeof jsonSample[key]
      if (fieldType === 'number') {
        data['fieldType'] = getDefaultNumType(jsonSample[key])
      } else {
        data['fieldType'] = fieldType
      }
      array.push(data)
    }
  }
  return genFinalNameAndType(array)
}

function genFinalNameAndType (array) {
  var arrayTypeRegex = new RegExp('\\#\\d+$', 'g')
  var jsonArrayTypeRegex = new RegExp('(\\#\\d+\\#)+', 'g')
  for (var i = 0; i < array.length; i++) {
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
      var subArray = array.slice(0, i)
      if (isExist(subArray, array[i].fieldName)) {
        array.splice(i, 1)
        i = i - 1
      } else {
        var position = lastPositionOfKeyPrefix(subArray, array[i].fieldName)
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
export function genDefaultSchemaTable (array) {
  var arrayFinal = array.map(i => {
    var temp = Object.assign({}, i, {
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
}

function genSubField (array, fieldObject, prefix) {
  if (prefix === '') prefix = `${fieldObject.name}#`
  else prefix = `${prefix}${fieldObject.name}#`
  if (fieldObject.hasOwnProperty('sub_fields')) var subFieldsArray = fieldObject['sub_fields']
  else subFieldsArray = []
  for (var i = 0; i < array.length; i++) {
    if (array[i].hasOwnProperty('fieldName') && array[i].fieldName.startsWith(prefix)) {
      if (array[i].fieldType !== JSONARRAY && array[i].fieldType !== JSONOBJECT && !array[i].fieldType.startsWith('tuple')) {
        subFieldsArray.push(genBaseField(array[i]))
      } else {
        var object = genBaseField(array[i])
        object = genSubField(array.slice(i + 1, array.length), object, prefix)
        subFieldsArray.push(object)
        if (object.hasOwnProperty('sub_fields')) var step = object.sub_fields.length
        else step = 0
        i = i + step
      }
    } else {
      break
    }
  }
  fieldObject['sub_fields'] = subFieldsArray
  return fieldObject
}
