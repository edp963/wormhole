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
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
const FormItem = Form.Item
import Radio from 'antd/lib/radio'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button

export class UdfForm extends React.Component {
  render () {
    const { getFieldDecorator } = this.props.form
    const { type } = this.props

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 16 }
    }

    const disabledOrNot = type === 'edit'

    return (
      <Form>
        <Row gutter={8}>
          <Col span={24}>
            <FormItem className="hide">
              {getFieldDecorator('id', {
                hidden: type === 'add' || type === 'copy'
              })(
                <Input />
              )}
            </FormItem>
            <FormItem label="Function Name" {...itemStyle}>
              {getFieldDecorator('functionName', {
                rules: [{
                  required: true,
                  message: 'Function Name 不能为空'
                }]
              })(
                <Input placeholder="Function Name" disabled={disabledOrNot} />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Description" {...itemStyle}>
              {getFieldDecorator('desc', {})(
                <Input placeholder="Description" />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Full Class Name" {...itemStyle}>
              {getFieldDecorator('fullName', {
                rules: [{
                  required: true,
                  message: '请输入 Full Class Name'
                }]
              })(
                <Input placeholder="Full Class Name" disabled={disabledOrNot} />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Jar Name" {...itemStyle}>
              {getFieldDecorator('jarName', {
                rules: [{
                  required: true,
                  message: '请输入 Jar Name'
                }]
              })(
                <Input placeholder="Jar Name" />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Public" {...itemStyle}>
              {getFieldDecorator('public', {
                rules: [{
                  required: true,
                  message: '请填写 Public'
                }],
                initialValue: 'true'
              })(
                <RadioGroup className="radio-group-style" size="default">
                  <RadioButton value="true" className="read-only-style">True</RadioButton>
                  <RadioButton value="false" className="read-write-style">False</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
        </Row>
      </Form>
    )
  }
}

UdfForm.propTypes = {
  form: React.PropTypes.any,
  type: React.PropTypes.string
}

export default Form.create({wrappedComponentRef: true})(UdfForm)
