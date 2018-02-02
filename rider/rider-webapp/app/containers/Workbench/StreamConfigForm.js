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

import { forceCheckNum, forceCheckNumsPart } from '../../utils/util'
import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
const FormItem = Form.Item

export class StreamConfigForm extends React.Component {
  render () {
    const { form, tabPanelKey } = this.props
    const { getFieldDecorator } = form

    const itemStyle = {
      labelCol: { span: 4 },
      wrapperCol: { span: 19 }
    }
    const itemStyleOthers = {
      labelCol: { span: 12 },
      wrapperCol: { span: 6 }
    }
    const itemStylePEM = {
      labelCol: { span: 15 },
      wrapperCol: { span: 6 }
    }

    return (
      <Form>
        <Row>
          <Col span={24}>
            <FormItem label="JVM：" {...itemStyle}>
              {getFieldDecorator('jvm', {
                rules: [{
                  required: true,
                  message: '不能为空'
                }]
              })(
                <Input type="textarea" placeholder="JVM" autosize={{ minRows: 4, maxRows: 6 }} />
              )}
            </FormItem>
          </Col>
          <Col span={8}>
            <FormItem label="Driver Cores：" {...itemStyleOthers}>
              {getFieldDecorator('driverCores', {
                rules: [{
                  required: true,
                  message: '不能为空'
                }, {
                  validator: forceCheckNum
                }],
                initialValue: 1
              })(
                <InputNumber min={1} step={1} />
              )}
            </FormItem>
          </Col>
          <Col span={8}>
            <FormItem label="Driver Memory (GB)：" {...itemStyleOthers}>
              {getFieldDecorator('driverMemory', {
                rules: [{
                  required: true,
                  message: '不能为空'
                }, {
                  validator: forceCheckNum
                }],
                initialValue: 2
              })(
                <InputNumber min={1} step={1} />
              )}
            </FormItem>
          </Col>
        </Row>
        <Row>
          <Col span={8}>
            <FormItem label="Executors Number：" {...itemStyleOthers}>
              {getFieldDecorator('executorNums', {
                rules: [{
                  required: true,
                  message: '不能为空'
                }, {
                  validator: forceCheckNum
                }],
                initialValue: 6
              })(
                <InputNumber min={1} step={1} />
              )}
            </FormItem>
          </Col>
          <Col span={8}>
            <FormItem label="Per Executor Cores：" {...itemStyleOthers}>
              {getFieldDecorator('perExecutorCores', {
                rules: [{
                  required: true,
                  message: '不能为空'
                }, {
                  validator: forceCheckNum
                }],
                initialValue: 1
              })(
                <InputNumber min={1} step={1} />
              )}
            </FormItem>
          </Col>
          <Col span={8}>
            <FormItem label="Per Executor Memory (GB)：" {...itemStylePEM}>
              {getFieldDecorator('perExecutorMemory', {
                rules: [{
                  required: true,
                  message: '不能为空'
                }, {
                  validator: forceCheckNum
                }],
                initialValue: 2
              })(
                <InputNumber min={1} step={1} />
              )}
            </FormItem>
          </Col>
        </Row>
        <Row>
          <Col span={8} className={`${tabPanelKey === 'stream' ? '' : 'hide'}`}>
            <FormItem label="Batch Duration (Sec)：" {...itemStyleOthers}>
              {getFieldDecorator('durations', {
                rules: [{
                  required: true,
                  message: '不能为空'
                }, {
                  validator: forceCheckNum
                }],
                initialValue: 10
              })(
                <InputNumber min={1} step={1} />
              )}
            </FormItem>
          </Col>
          <Col span={8} className={`${tabPanelKey === 'stream' ? '' : 'hide'}`}>
            <FormItem label="Parallelism Partition：" {...itemStyleOthers}>
              {getFieldDecorator('partitions', {
                rules: [{
                  required: true,
                  message: '不能为空'
                }, {
                  validator: forceCheckNumsPart
                }],
                initialValue: 6
              })(
                <InputNumber min={-1} step={1} />
              )}
            </FormItem>
          </Col>
          <Col span={8} className={`${tabPanelKey === 'stream' ? '' : 'hide'}`}>
            <FormItem label="Max Batch Data Size (Mb)：" {...itemStylePEM}>
              {getFieldDecorator('maxRecords', {
                rules: [{
                  required: true,
                  message: '不能为空'
                }, {
                  validator: forceCheckNum
                }],
                initialValue: 50
              })(
                <InputNumber min={10} max={50} />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Others：" {...itemStyle}>
              {getFieldDecorator('personalConf', {})(
                <Input type="textarea" placeholder="格式如：key=value，多条时换行输入" autosize={{ minRows: 6, maxRows: 10 }} />
              )}
            </FormItem>
          </Col>
        </Row>
      </Form>
    )
  }
}

StreamConfigForm.propTypes = {
  form: React.PropTypes.any,
  tabPanelKey: React.PropTypes.string
}

export default Form.create({wrappedComponentRef: true})(StreamConfigForm)
