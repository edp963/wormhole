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

import React from 'react'

import { getAlterTypesByOriginType } from './umsFunction'

import Form from 'antd/lib/form'
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
import message from 'antd/lib/message'
import Select from 'antd/lib/select'
const Option = Select.Option

export class EditableCell extends React.Component {
  state = {
    typeValue: '',
    changeBeforeValue: '',
    editable: false,
    fieldTypeOptionsVal: [],
    tupleOrNot: false,
    sepratorValue: ''
  }

  componentWillReceiveProps (props) {
    if (props.value) {
      this.setState({
        typeValue: props.value,
        changeBeforeValue: props.value
      })
    }
  }

  onChangeFieldType = (value) => {
    const { tableDatas } = this.props
    const { changeBeforeValue } = this.state

    const tupleExit = tableDatas.find(i => i.fieldType.indexOf('tuple') > -1)
    if (tupleExit && value === 'tuple') {
      message.warning('FieldType 至多有一个 tuple 类型！', 3)
    } else {
      this.setState({
        typeValue: value,
        editable: false
      })

      if (value === 'tuple') {
        this.setState({
          tupleOrNot: true
        })
      } else {
        this.props.initChangeTypeOption(changeBeforeValue, value)
      }
    }
  }

  edit = () => {
    const { typeValue } = this.state

    let typeVal = ''
    if (typeValue.indexOf('tuple') > -1) {
      const arrTemp = typeValue.split('##')
      typeVal = arrTemp[0]
    } else {
      typeVal = typeValue
    }

    const selectTypeValue = getAlterTypesByOriginType(typeVal)

    this.setState({
      changeBeforeValue: typeVal,
      editable: true,
      fieldTypeOptionsVal: selectTypeValue
    })
  }

  handleChange = (e) => {
    const value = e.target.value
    this.setState({
      sepratorValue: value
    })
  }

  check = () => {
    const { sepratorValue } = this.state

    if (sepratorValue === '') {
      message.warning('请填写分隔符!', 3)
    } else {
      this.setState({
        value: `tuple##${sepratorValue}`,
        tupleOrNot: false
      }, () => {
        this.props.initChangeTypeOption(this.state.changeBeforeValue, this.state.value)
      })
    }
  }

  render () {
    const { typeValue, editable, fieldTypeOptionsVal, tupleOrNot } = this.state
    const { recordValue } = this.props

    const fieldTypeOptions = fieldTypeOptionsVal.map(s => (<Option key={s} value={`${s}`}>{s}</Option>))

    let editTypeIconDiasabled = ''
    let editTypeIconClass = ''
    let textClass = ''
    if (recordValue.forbidden) {
      editTypeIconDiasabled = 'edit-disabled-class'
      editTypeIconClass = 'hide'
      textClass = 'type-text-class'
    } else {
      editTypeIconDiasabled = 'hide'
      textClass = ''
    }

    return (
      <div className="editable-cell">
        {
          editable
            ? <div className="editable-cell-input-wrapper">
              <Select
                onChange={this.onChangeFieldType}
                placeholder="Select"
                className="field-type-select"
              >
                {fieldTypeOptions}
              </Select>

            </div>
            : <div className="editable-cell-text-wrapper">
              <span className={textClass}>{typeValue || ' '}</span>
              <Input
                className={tupleOrNot === true ? '' : 'hide'}
                onChange={this.handleChange}
                onPressEnter={this.check}
              />
              <Icon
                type="check"
                className={`editable-cell-icon-check ${tupleOrNot === true ? '' : 'hide'}`}
                onClick={this.check}
              />
              <Icon
                type="edit"
                className={`editable-cell-icon ${editTypeIconClass}`}
                onClick={this.edit}
              />
              <Icon
                type="edit"
                className={`editable-cell-icon ${editTypeIconDiasabled}`}
              />
            </div>
        }
      </div>
    )
  }
}

EditableCell.propTypes = {
  initChangeTypeOption: React.PropTypes.func,
  tableDatas: React.PropTypes.array,
  recordValue: React.PropTypes.object
}

export default Form.create({wrappedComponentRef: true})(EditableCell)
