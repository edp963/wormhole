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
const DATETIME = 'datetime'

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

// 点击保存时，去除selected === false的行
// todo: test
function selectedFields (array) {
  for (var i = 0; i < array.length; i++) {
    if (array[i].selected === false) {
      array.splice(i, 1)
      i = i - 1
    }
  }
  return array
}

// 若用户配置fieldName为 ums_id_或ums_ts_或ums_op_，调用umsSysFieldSelected方法，获得新的数组
// umsSysField = ums_id_或ums_ts_或ums_op_
export function umsSysFieldSelected (array, index, umsSysField) {
  const umsField = umsSysField

  const arr = []
  for (let i = 0; i < index; i++) {
    arr.push(array[i])
  }

  // index 项
  const indexVal = {
    fieldName: array[index].fieldName,
    fieldType: array[index].fieldType,
    forbidden: array[index].forbidden,
    rename: array[index].rename,
    selected: array[index].selected,
    ums_id_: (umsSysField === 'ums_id_' || array[index].ums_id_ === true),
    ums_op_: array[index].ums_op_,
    ums_ts_: (umsSysField === 'ums_ts_' || array[index].ums_ts_ === true),
    value: array[index].value
  }

  arr.push(indexVal)

  // index + 1 项
  const objectVal = {
    fieldName: array[index].fieldName,
    fieldType: array[index].fieldType,
    value: array[index].value,

    rename: umsField,
    selected: true,
    forbidden: true,

    ums_id_: umsSysField === 'ums_id_',
    ums_op_: array[index].ums_op_,
    ums_ts_: umsSysField === 'ums_ts_'
  }
  arr.push(objectVal)

  for (let j = index + 1; j < array.length; j++) {
    arr.push(array[j])
  }

  // 给新增的子数组 index + 1 项设置key
  const umsArr = arr.map((s, index) => {
    s.key = index
    return s
  })
  return umsArr
}

// 若用户选择该行为ums_id_或ums_ts_或ums_op_后，又点了取消，调用umsSysFieldUnSelected方法，删除刚刚生成的新行，返回新数组
export function umsSysFieldUnSelected (array, index, umsSysField) {
  if ((array[index + 1].ums_id_ === true && array[index + 1].forbidden === true) ||
    (array[index + 1].ums_ts_ === true && array[index + 1].forbidden === true) ||
    ((array[index + 1].ums_op_ !== '' && array[index + 1].forbidden === true))) {
    const arr = []
    for (let i = 0; i < index; i++) {
      arr.push(array[i])
    }

    let umsIdBool = false
    let umsTsBool = false
    let umsOpValue = ''
    if (umsSysField === 'ums_id_') {
      umsIdBool = false
      umsTsBool = array[index].ums_ts_
      umsOpValue = array[index].ums_op_
    } else if (umsSysField === 'ums_ts_') {
      umsIdBool = array[index].ums_id_
      umsTsBool = false
      umsOpValue = array[index].ums_op_
    } else if (umsSysField === 'ums_op_') {
      umsIdBool = array[index].ums_id_
      umsTsBool = array[index].ums_ts_
      umsOpValue = ''
    }

    const indexVal = {
      fieldName: array[index].fieldName,
      fieldType: array[index].fieldType,
      forbidden: array[index].forbidden,
      rename: array[index].rename,
      selected: array[index].selected,
      key: array[index].key,
      ums_id_: umsIdBool,
      ums_op_: umsOpValue,
      ums_ts_: umsTsBool,
      value: array[index].value
    }
    arr.push(indexVal)

    if (array[index + 1].rename === umsSysField) {
      for (let j = index + 2; j < array.length; j++) {
        arr.push(array[j])
      }
    } else if (array[index + 2].rename === umsSysField) {
      arr.push(array[index + 1])
      for (let k = index + 3; k < array.length; k++) {
        arr.push(array[k])
      }
    } else if (array[index + 3].rename === umsSysField) {
      arr.push(array[index + 1], array[index + 2])
      for (let g = index + 4; g < array.length; g++) {
        arr.push(array[g])
      }
    }

    // 重新设置key
    const arrTemp = JSON.stringify(arr, ['fieldName', 'fieldType', 'forbidden', 'rename', 'selected', 'ums_id_', 'ums_op_', 'ums_ts_', 'value'])
    const umsArr = JSON.parse(arrTemp).map((s, index) => {
      s.key = index
      return s
    })
    return umsArr
  }
}

// fieldType值由 jsonobject/jsonarray/tuple 改为 string 时，生成新数组
export function nestType2string (array, index) {
  var prefix = `${array[index].fieldName}#`
  array[index].fieldType = STRING
  for (var i = index + 1; i < array.length; i++) {
    if (array[i].fieldName.startsWith(prefix)) {
      array[i].forbidden = true
      array[i].selected = false
    } else {
      break
    }
  }
  return array
}

// fieldType值由 string 改为 jsonobject/jsonarray/tuple 时，生成新数组
export function string2nestType (array, index, alterType) {
  var prefix = `${array[index].fieldName}#`
  array[index].fieldType = alterType
  for (var i = index + 1; i < array.length; i++) {
    if (array[i].fieldName.startsWith(prefix)) {
      array[i].forbidden = false
      array[i].selected = true
    } else {
      break
    }
  }
  return array
}

// fieldType配置为 tuple 时，需要让用户配置分隔符，假设为"/"，该fieldType的值为"tuple##/"，
// 1. string 改为 tuple；2.用户修改了分隔符。调用tupleFields方法，生成包含 tuple 子字段的新数组
// 子数组的 rename 需要用户配
export function tupleFields (array, index, separator) {
  array[index].fieldType = `${TUPLE}##${separator}`
  for (let i = index + 1; i < array.length; i++) {
    if (array[i].fieldName.startsWith(`${array[index].fieldName}#`)) {
      array.splice(i, 1)
    } else {
      break
    }
  }
  var tupleSplit = array[index].value.split(separator)
  if (tupleSplit.length <= 1) {
    return array
  }
  var tupleArray = []
  for (let i = 0; i < tupleSplit.length; i++) {
    var object = {}
    object['fieldName'] = `${array[index].fieldName}#${i}`
    object['fieldType'] = 'string'
    object['value'] = tupleArray[i]
    object['selected'] = true
    object['rename'] = ''
    object['ums_id_'] = false
    object['ums_ts_'] = false
    object['ums_op_'] = ''
    object['forbidden'] = false
    object['value'] = tupleSplit[i]
    tupleArray.push(object)
  }
  for (let i = 0; i < tupleArray.length; i++) {
    array.splice(index + 1, 0, tupleArray[i])
    index = index + 1
  }

  const umsArr = array.map((s, index) => {
    s.key = index
    return s
  })
  return umsArr
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
  if (fieldInfo.hasOwnProperty('rename') && fieldInfo.rename !== '' && fieldInfo.fieldName.split('#').pop() !== fieldInfo.rename) {
    fieldObject['rename'] = fieldInfo.rename
  }
  if (fieldInfo.hasOwnProperty('ums_op_') && fieldInfo.ums_op_ !== '' && fieldInfo.forbidden === true) {
    fieldObject['rename'] = fieldInfo.rename
    fieldObject['ums_sys_mapping'] = fieldInfo.ums_op_
  }
  if (fieldInfo.hasOwnProperty('ums_id_') && fieldInfo.ums_id_ === true && fieldInfo.forbidden === true) {
    fieldObject['rename'] = fieldInfo.rename
  }
  if (fieldInfo.hasOwnProperty('ums_ts_') && fieldInfo.ums_ts_ === true && fieldInfo.forbidden === true) {
    fieldObject['rename'] = fieldInfo.rename
  }
  if (fieldInfo.fieldType.startsWith(TUPLE)) {
    fieldObject['type'] = TUPLE
    fieldObject['tuple_sep'] = fieldInfo.fieldType.split('##').pop()
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

// 若用户修改某字段类型，判断该字段是否为ums系统字段，若是，修改与之相关的ums系统字段类型
export function umsSysFieldTypeAutoChange (array, index, alterType) {
  if (array[index].ums_id_ === true || array[index].ums_ts_ === true || array[index].ums_op_ !== '') {
    if (array[index + 1].rename === 'ums_id_' || array[index + 1].rename === 'ums_ts_' || array[index + 1].rename === 'ums_op_') {
      array[index + 1].fieldType = alterType
    }
    if (array[index + 2].rename === 'ums_id_' || array[index + 2].rename === 'ums_ts_' || array[index + 2].rename === 'ums_op_') {
      array[index + 2].fieldType = alterType
    }
    if (array[index + 3].rename === 'ums_id_' || array[index + 3].rename === 'ums_ts_' || array[index + 3].rename === 'ums_op_') {
      array[index + 3].fieldType = alterType
    }
  }
  return array
}

