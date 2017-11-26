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

const STRING = 'string'
const INT = 'int'
const LONG = 'long'
const FLOAT = 'float'
const DOUBLE = 'double'
const BOOLEAN = 'boolean'
const DECIMAL = 'decimal'
const BINARY = 'binary'
const DATETIME = 'ç'

const STRINGARRAY = 'stringarray'
const INTARRAY = 'intarray'
const LONGARRAY = 'longarray'
const FLOATARRAY = 'floatarray'
const DOUBLEARRAY = 'doublearray'
const BOOLEANARRAY = 'booleanarray'
const DECIMALARRAY = 'decimalarray'
const BINARYARRAY = 'binaryarray'
const DATETIMEARRAY = 'datetimearray'

const JSONOBJECT = 'jsonobject'
const JSONARRAY = 'jsonarray'
const TUPLE = 'tuple'

function getDefaultNumType (value) {
  value = `${value}`
  if (value.indexOf('.') === -1) return LONG
  else return DOUBLE
}

// function distinct (array) {
//   for (var i = 0; i < array.length; i++) {
//     for (var j = i + 1; j < array.length;) {
//       if (array[i].fieldName === array[j].fieldName) {
//         array.splice(j, 1)
//       } else {
//         j++
//       }
//     }
//   }
//   return array
// }

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

// 输出为array中重复的rename字段index组成的数组
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

// 修改某个fieldType时，传入原fieldType类型，返回可选择的类型数组
export function getAlterTypesByOriginType (fieldType) {
  var typeArray = []
  if (fieldType === INT || fieldType === LONG || fieldType === DECIMAL || fieldType === FLOAT || fieldType === DOUBLE ||
    fieldType === BINARY || fieldType === DATETIME || fieldType === BOOLEAN) {
    typeArray = [INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, BOOLEAN, DATETIME, BINARY]
  } else if (fieldType === INTARRAY || fieldType === LONGARRAY || fieldType === DECIMALARRAY || fieldType === FLOATARRAY ||
    fieldType === DOUBLEARRAY || fieldType === STRINGARRAY || fieldType === BINARYARRAY ||
    fieldType === DATETIMEARRAY || fieldType === BOOLEANARRAY) {
    typeArray = [INTARRAY, LONGARRAY, FLOATARRAY, DOUBLEARRAY, DECIMALARRAY,
      STRINGARRAY, BOOLEANARRAY, DATETIMEARRAY, BINARYARRAY]
  } else if (fieldType === JSONARRAY || fieldType === JSONOBJECT || fieldType === TUPLE) {
    typeArray = [fieldType, STRING]
  } else if (fieldType === STRING) {
    typeArray = [INT, LONG, DECIMAL, FLOAT, DOUBLE, BINARY, DATETIME, JSONOBJECT, JSONARRAY, TUPLE, BOOLEAN]
  }
  return typeArray
}

function selectedFields (array) {
  for (var i = 0; i < array.length; i++) {
    if (array[i].selected === false || array[i].forbidden === true) {
      array.splice(i, 1)
      i = i - 1
    }
  }
  return array
}

// 若用户配置fieldName为 ums_id_或ums_ts_或ums_op_，调用umsSysFieldSelected方法，获得新的数组
export function umsSysFieldSelected (array, index, umsSysField) {
  var object = array[index]
  object.rename = umsSysField
  if (umsSysField === 'ums_op_') {
    object.forbidden === true
  }
  array.splice(index, 0, object)
  return array
}

// 若用户选择该行为ums_id_或ums_ts_或ums_op_后，又点了取消，调用umsSysFieldUnSelected方法，删除刚刚生成的新行，返回新数组
export function umsSysFieldUnSelected (array, index, umsSysField) {
  if (array[index + 1].rename === umsSysField) {
    array.splice(index + 1, 1)
  }
  if (array[index + 2].rename === umsSysField) {
    array.splice(index + 2, 1)
  }
  return array
}

// fieldType值由 jsonobject/jsonarray/tuple 改为 string 时，生成新数组
export function nestType2string (array, key, index) {
  var prefix = `${key}#`
  for (var i = index; i < array.length; i++) {
    if (array[i].fieldName.startsWith(prefix)) {
      array[i].forbidden = true
    } else {
      break
    }
  }
  return array
}

// fieldType值由 string 改为 jsonobject/jsonarray/tuple 时，生成新数组
export function string2nestType (array, key, index) {
  var prefix = `${key}#`
  for (var i = index; i < array.length; i++) {
    if (array[i].fieldName.startsWith(prefix)) {
      array[i].forbidden = false
    } else {
      break
    }
  }
  return array
}

// fieldType配置为 tuple 时，需要让用户配置分隔符，假设为"/"，该fieldType的值为"tuple;/"，调用tupleFields方法，生成包含 tuple 子字段的新数组
export function tupleFields (array, index, key, seprator) {
  var tupleSplit = array[index].value.split(`\\${seprator}`)
  var tupleArray = []
  for (var i = 0; i < tupleSplit.length; i++) {
    var object = {}
    object['fieldName'] = `${key}#${i}`
    object['fieldType'] = 'string'
    object['value'] = tupleArray[i]
    object['selected'] = true
    object['rename'] = ''
    object['ums_id_'] = false
    object['ums_ts_'] = false
    object['ums_op_'] = ''
    object['forbidden'] = false
    tupleArray.push(object)
  }
  array.splice(index, 0, tupleArray)
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
        data['value'] = jsonSample[key].slice(0, 1)
      } else {
        if (prefix !== '') prefix = `${prefix}#${key}`
        else prefix = key
        data['fieldName'] = prefix
        data['fieldType'] = JSONOBJECT
        data['value'] = jsonSample[key]
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
      data['value'] = jsonSample[key]
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

function genBaseField (fieldInfo) {
  var fieldObject = {}
  fieldObject['name'] = fieldInfo.fieldName.split('#').pop()
  fieldObject['type'] = fieldInfo.fieldType
  fieldObject['nullable'] = true
  if (fieldInfo.hasOwnProperty('rename') && fieldInfo.rename !== '' && fieldInfo.rename !== fieldInfo.fieldName) {
    fieldObject['rename'] = fieldInfo.rename
  } else if (fieldInfo.hasOwnProperty('ums_op_') && fieldInfo.ums_op_ !== '') {
    fieldObject['ums_sys_mapping'] = fieldInfo.ums_op_
  } else if (fieldInfo.fieldType === TUPLE) {
    fieldObject['tuple_sep'] = fieldInfo.split(';').pop()
  }

  return fieldObject
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
  return fieldsObject
}

