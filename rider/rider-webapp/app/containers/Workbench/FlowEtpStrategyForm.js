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
import InputNumber from 'antd/lib/input-number'
const FormItem = Form.Item
import Radio from 'antd/lib/radio'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button

export class FlowEtpConfigForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      ruleModeValue: '',
      againstActionValue: ''
    }
  }

  onCheckRuleChange = (e) => this.setState({ radioValue: e.target.value })

  onRuleModeChange = (e) => this.setState({ ruleModeValue: e.target.value })

  onAgainstActionChange = (e) => this.setState({ againstActionValue: e.target.value })

  forceCheckNumSave = (rule, value, callback) => {
    const reg = /^\d+$/
    if (reg.test(value)) {
      callback()
    } else {
      callback('必须是数字')
    }
  }

  forceChecColumnSave = (rule, value, callback) => {
    if (value.indexOf('，')) {
      callback()
    } else {
      callback('不允许出现中文逗号')
    }
  }

  render () {
    const { form } = this.props
    const { getFieldDecorator } = form
    const itemStyle = {
      labelCol: { span: 7 },
      wrapperCol: { span: 16 }
    }

    return (
      <Form>
        <Row>
          <Col span={24}>
            <FormItem label="Check Columns" {...itemStyle}>
              {getFieldDecorator('checkColumns', {
                rules: [{
                  required: true,
                  message: '请填写 Check Columns'
                }, {
                  validator: this.forceChecColumnSave
                }]
              })(
                <Input placeholder="字段名，用英文逗号隔开" />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Check Rule" {...itemStyle}>
              {getFieldDecorator('checkRule', {
                rules: [{
                  required: true,
                  message: '请选择 Check Rule'
                }]
              })(
                <RadioGroup onChange={this.onCheckRuleChange}>
                  <RadioButton value="and">and</RadioButton>
                  <RadioButton value="or">or</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Rule Mode" {...itemStyle}>
              {getFieldDecorator('ruleMode', {
                rules: [{
                  required: true,
                  message: '请选择 Rule Mode'
                }]
              })(
                <RadioGroup onChange={this.onRuleModeChange}>
                  <RadioButton value="timeout">timeout</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Rule Params (Sec)" {...itemStyle}>
              {getFieldDecorator('ruleParams', {
                rules: [{
                  required: true,
                  message: '请填写 Rule Params'
                }, {
                  validator: this.forceCheckNumSave
                }]
              })(
                <InputNumber min={10} max={1800} step={1} placeholder="超时时间" style={{width: '50%'}} />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Against Action" {...itemStyle}>
              {getFieldDecorator('againstAction', {
                rules: [{
                  required: true,
                  message: '请选择 Against Action'
                }]
              })(
                <RadioGroup onChange={this.onAgainstActionChange}>
                  <RadioButton value="send">send</RadioButton>
                  <RadioButton value="drop">drop</RadioButton>
                  <RadioButton value="alert">alert</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
        </Row>
      </Form>
    )
  }
}

FlowEtpConfigForm.propTypes = {
  form: React.PropTypes.any
}

export default Form.create({wrappedComponentRef: true})(FlowEtpConfigForm)
