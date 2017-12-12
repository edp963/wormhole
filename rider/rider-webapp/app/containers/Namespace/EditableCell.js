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

import Form from 'antd/lib/form'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Icon from 'antd/lib/icon'
import message from 'antd/lib/message'
const FormItem = Form.Item

export class EditableCell extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      tupleFormFinal: props.tupleForm
    }
  }

  componentWillReceiveProps (props) {
    const ft = props.recordVal.fieldType

    this.setState({
      tupleFormFinal: (ft.indexOf('tuple') > -1 || props.selectTypeVal === 'tuple') ? props.tupleForm : ''
    })
  }

  checkFieldType = () => {
    this.props.form.validateFieldsAndScroll((err, values) => { // eslint-disable-line
      // if (!err) {
      //   this.props.initcheckFieldType(this.props.recordVal, values)
      // }
      if (values.sizeValue) {
        const reg = /^[0-9]*$/
        if (!reg.test(values.sizeValue)) {
          message.error('长度必须是正整数！', 3)
        } else if (values.delimiterValue) {
          this.props.initcheckFieldType(this.props.recordVal, values)
        } else if (!values.delimiterValue) {
          message.error('请填写分隔符！', 3)
        }
      } else if (!values.delimiterValue) {
        message.error('请填写分隔符！', 3)
      } else if (!values.sizeValue) {
        message.error('请正确填写长度！', 3)
      }
    })
  }

  editFieldType = () => {
    this.props.initeditFieldType(this.props.recordVal)
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { delimiterValue, sizeValue } = this.props
    const { tupleFormFinal } = this.state

    let htmlFieldType = ''
    if (tupleFormFinal === '') {
      htmlFieldType = ''
    } else if (tupleFormFinal === 'edit') {
      htmlFieldType = (
        <Row gutter={4} style={{ marginBottom: '-28px' }}>
          <Col span={9}>
            <FormItem label="">
              {getFieldDecorator('delimiterValue', {
                // rules: [{
                //   required: true,
                //   message: '请填写'
                // }]
              })(
                <Input placeholder="Sep" />
              )}
            </FormItem>
          </Col>
          <Col span={11}>
            <FormItem label="">
              {getFieldDecorator('sizeValue', {
                // rules: [{
                //   required: true,
                //   message: '必须是正整数'
                // }, {
                //   // validator: this.forceCheckSize
                // }]
              })(
                <InputNumber placeholder="Size" style={{ width: '100%' }} />
              )}
            </FormItem>
          </Col>
          <Col span={4} className="field-type-check-class">
            <Icon
              type="check"
              onClick={this.checkFieldType}
            />
          </Col>
        </Row>
      )
    } else if (tupleFormFinal === 'text') {
      htmlFieldType = (
        <Row gutter={4}>
          <Col span={9}>
            <span style={{ marginLeft: '2px' }}>{`Sep: ${delimiterValue}`}</span>
          </Col>
          <Col span={11}>
            <span>{`Size: ${sizeValue}`}</span>
          </Col>
          <Col span={4}>
            <Icon
              type="edit"
              onClick={this.editFieldType}
            />
          </Col>
        </Row>
      )
    }

    return (
      <Form className="field-type-form-class">
        {htmlFieldType}
      </Form>
    )
  }
}

EditableCell.propTypes = {
  form: React.PropTypes.any,
  sizeValue: React.PropTypes.number,
  delimiterValue: React.PropTypes.string,
  recordVal: React.PropTypes.object,
  tupleForm: React.PropTypes.string,
  initcheckFieldType: React.PropTypes.func,
  initeditFieldType: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(EditableCell)
