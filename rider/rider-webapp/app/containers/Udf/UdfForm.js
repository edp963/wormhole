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
const FormItem = Form.Item
import Radio from 'antd/lib/radio'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button
import { selectLocale } from '../LanguageProvider/selectors'

export class UdfForm extends React.Component {

  render () {
    const { getFieldDecorator } = this.props.form
    const { type, locale, streamType } = this.props
    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 16 }
    }
    const udfDisabledOrNot = type === 'edit'

    return (
      <Form>
        <Row gutter={8}>
          <Col span={24}>
            <FormItem label="Stream type" {...itemStyle}>
              {getFieldDecorator('streamType', {
                rules: [{
                  required: true,
                  message: locale === 'en' ? 'Stream type cannot be empty' : 'Stream Type 不能为空'
                }],
                initialValue: 'spark'
              })(
                <RadioGroup className="radio-group-style" disabled={udfDisabledOrNot} size="default" onChange={this.props.changeUdfStreamType}>
                  <RadioButton value="spark" className="radio-btn-style radio-btn-extra">Spark</RadioButton>
                  <RadioButton value="flink" className="radio-btn-style radio-btn-extra">Flink</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="map or agg" {...itemStyle}>
              {getFieldDecorator('mapOrAgg', {
                rules: [{
                  required: true,
                  message: locale === 'en' ? 'Map or agg cannot be empty' : 'map or agg 不能为空'
                }],
                initialValue: 'udf'
              })(
                <RadioGroup className="radio-group-style" disabled={udfDisabledOrNot} size="default" onChange={this.props.changeUdfMapOrAgg}>
                  <RadioButton value="udf" className="radio-btn-style radio-btn-extra">udf</RadioButton>
                  {
                    streamType === 'flink' ? (<RadioButton value="udaf" className="radio-btn-style radio-btn-extra">udaf</RadioButton>) : ''
                  }
                </RadioGroup>
              )}
            </FormItem>
          </Col>
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
                  message: locale === 'en' ? 'Function Name cannot be empty' : 'Function Name 不能为空'
                }]
              })(
                <Input placeholder="Function Name" disabled={type === 'edit'} />
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
                  message: locale === 'en' ? 'Please fill in full class name' : '请填写 Full Class Name'
                }]
              })(
                <Input placeholder="Full Class Name" disabled={type === 'edit'} />
              )}
            </FormItem>
          </Col>
          {
            streamType === 'spark' ? (
              <Col span={24}>
                <FormItem label="Jar Name" {...itemStyle}>
                  {getFieldDecorator('jarName', {
                    rules: [{
                      required: true,
                      message: locale === 'en' ? 'Please fill in jar name' : '请填写 Jar Name'
                    }]
                  })(
                    <Input placeholder="Jar Name" />
                  )}
                </FormItem>
              </Col>
            ) : ''
          }
          <Col span={24}>
            <FormItem label="Public" {...itemStyle}>
              {getFieldDecorator('public', {
                rules: [{
                  required: true,
                  message: locale === 'en' ? 'Please fill in public' : '请填写 Public'
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
  form: PropTypes.any,
  type: PropTypes.string,
  locale: PropTypes.string,
  streamType: PropTypes.string,
  changeUdfStreamType: PropTypes.func,
  changeUdfMapOrAgg: PropTypes.func
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, null)(UdfForm))
