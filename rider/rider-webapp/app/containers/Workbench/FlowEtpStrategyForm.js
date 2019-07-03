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
import PropTypes from 'prop-types'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'

import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
const FormItem = Form.Item
import Radio from 'antd/lib/radio'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button
import { forceCheckNum } from '../../utils/util'
import { selectLocale } from '../LanguageProvider/selectors'

export class FlowEtpConfigForm extends React.Component {
  forceChecColumnSave = (rule, value, callback) => {
    const { locale } = this.props
    if (!value.includes('，')) {
      callback()
    } else {
      callback(locale === 'en' ? 'No full-shaped comma' : '不允许出现中文逗号')
    }
  }

  render () {
    const { form, locale } = this.props
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
                  message: locale === 'en' ? 'Please fill in check columns' : '请填写 Check Columns'
                }, {
                  validator: this.forceChecColumnSave
                }]
              })(
                <Input placeholder={locale === 'en' ? 'separate field names with half-angle commas' : '字段名，用英文逗号隔开'} />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Check Rule" {...itemStyle}>
              {getFieldDecorator('checkRule', {
                rules: [{
                  required: true,
                  message: locale === 'en' ? 'Please select check rule' : '请选择 Check Rule'
                }]
              })(
                <RadioGroup>
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
                  message: locale === 'en' ? 'Please select rule mode' : '请选择 Rule Mode'
                }]
              })(
                <RadioGroup>
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
                  message: locale === 'en' ? 'Please fill in rule params' : '请填写 Rule Params'
                }, {
                  validator: forceCheckNum
                }]
              })(
                <InputNumber
                  min={10}
                  step={1}
                  placeholder={locale === 'en' ? 'Timeout' : '超时时间'}
                  style={{width: '50%'}}
                />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Against Action" {...itemStyle}>
              {getFieldDecorator('againstAction', {
                rules: [{
                  required: true,
                  message: locale === 'en' ? 'Please select against action' : '请选择 Against Action'
                }]
              })(
                <RadioGroup>
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
  form: PropTypes.any,
  locale: PropTypes.string
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, null)(FlowEtpConfigForm))
