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
const FormItem = Form.Item

export class EditableCell extends React.Component {
  constructor (props) {
    super(props)
    this.state = {

    }
  }

  componentWillMount () {
    // console.log('this.props', this.props.recordVal)
  }

  componentWillReceiveProps (props) {

  }

  forceCheckSize = (rule, value, callback) => {
    const reg = /^[0-9]*$/
    if (reg.test(value)) {
      callback()
    } else {
      callback('必须是正整数')
    }
  }

  checkFieldType = () => {
    this.props.form.validateFieldsAndScroll((err, values) => { // eslint-disable-line
      if (!err) {
        this.props.initcheckFieldType(this.props.recordVal, values)
      }
    })
  }

  editFieldType = () => {
    this.props.initeditFieldType(this.props.recordVal)
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { tupleForm, delimiterValue, sizeValue } = this.props
    // const { recordVal, tupleForm, currentKey,  } = this.props

    let htmlUmsop = ''
    if (tupleForm === '') {
      htmlUmsop = ''
    } else if (tupleForm === 'edit') {
      htmlUmsop = (
        <Row gutter={8} style={{ marginBottom: '-25px', marginTop: '5px' }}>
          <Col span={10}>
            <FormItem label="">
              {getFieldDecorator('delimiterValue', {
                rules: [{
                  required: true,
                  message: '请填写'
                }]
              })(
                <Input
                  onChange={this.handleChangeDelimiter}
                />
              )}
            </FormItem>
          </Col>
          <Col span={10}>
            <FormItem label="">
              {getFieldDecorator('sizeValue', {
                rules: [{
                  required: true,
                  message: '必须是正整数'
                }, {
                  validator: this.forceCheckSize
                }]
              })(
                <InputNumber
                  style={{ width: '100%' }}
                  onChange={this.handleChangeSize}
                />
              )}
            </FormItem>
          </Col>
          <Col span={4}>
            <Icon
              type="check"
              onClick={this.checkFieldType}
            />
          </Col>
        </Row>
      )
    } else if (tupleForm === 'text') {
      htmlUmsop = (
        <Row gutter={8}>
          <Col span={12}>
            <span>{`Delimiter: ${delimiterValue}`}</span>
          </Col>
          <Col span={10}>
            <span>{`Size: ${sizeValue}`}</span>
          </Col>
          <Col span={2}>
            <Icon
              type="edit"
              onClick={this.editFieldType}
            />
          </Col>
        </Row>
      )
    }

    return (
      <Form>
        {htmlUmsop}
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
