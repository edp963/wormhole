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
import { forceCheckNum } from '../../utils/util'
import { selectLocale } from '../LanguageProvider/selectors'

export class StreamConfigForm extends React.Component {
  render () {
    const { form, tabPanelKey, locale, streamSubPanelKey } = this.props
    const { getFieldDecorator } = form
    const textMessage = locale === 'en' ? 'It cannot be empty' : '不能为空'

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
        {streamSubPanelKey === 'spark' || tabPanelKey === 'job' ? (
          <Row>
            <Col span={24}>
              <FormItem label="JVM Driver Config：" {...itemStyle}>
                {getFieldDecorator('JVMDriverConfig', {
                  rules: [{
                    required: true,
                    message: textMessage
                  }]
                })(
                  <Input type="textarea" placeholder="JVM Driver Config" autosize={{ minRows: 4, maxRows: 6 }} />
                )}
              </FormItem>
            </Col>
          </Row>
          ) : ''
        }
        {streamSubPanelKey === 'spark' || tabPanelKey === 'job' ? (
          <Row>
            <Col span={24}>
              <FormItem label="JVM Executor Config：" {...itemStyle}>
                {getFieldDecorator('JVMExecutorConfig', {
                  rules: [{
                    required: true,
                    message: textMessage
                  }]
                })(
                  <Input type="textarea" placeholder="JVM Executor Config" autosize={{ minRows: 4, maxRows: 6 }} />
                )}
              </FormItem>
            </Col>
          </Row>
          ) : ''
        }
        {/* spark */}
        {streamSubPanelKey === 'spark' || tabPanelKey === 'job' ? (
          <Row>
            <Col span={8}>
              <FormItem label="Driver Cores：" {...itemStyleOthers}>
                {getFieldDecorator('driverCores', {
                  rules: [{
                    required: true,
                    message: textMessage
                  }, {
                    validator: forceCheckNum
                  }]
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
                    message: textMessage
                  }, {
                    validator: forceCheckNum
                  }]
                })(
                  <InputNumber min={1} step={1} />
                )}
              </FormItem>
            </Col>
          </Row>
          ) : ''
        }
        {streamSubPanelKey === 'spark' || tabPanelKey === 'job' ? (

          <Row>
            <Col span={8}>
              <FormItem label="Executors Number：" {...itemStyleOthers}>
                {getFieldDecorator('executorNums', {
                  rules: [{
                    required: true,
                    message: textMessage
                  }, {
                    validator: forceCheckNum
                  }]
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
                    message: textMessage
                  }, {
                    validator: forceCheckNum
                  }]
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
                    message: textMessage
                  }, {
                    validator: forceCheckNum
                  }]
                })(
                  <InputNumber min={1} step={1} />
                )}
              </FormItem>
            </Col>
          </Row>
          ) : ''
        }
        {streamSubPanelKey === 'spark' ? (
          <Row>
            <Col span={8}>
              <FormItem label="Batch Duration (Sec)：" {...itemStyleOthers}>
                {getFieldDecorator('durations', {
                  rules: [{
                    required: true,
                    message: textMessage
                  }, {
                    validator: forceCheckNum
                  }]
                })(
                  <InputNumber min={1} step={1} />
                )}
              </FormItem>
            </Col>
            <Col span={8}>
              <FormItem label="Parallelism Partition：" {...itemStyleOthers}>
                {getFieldDecorator('partitions', {
                  rules: [{
                    required: true,
                    message: textMessage
                  }]
                })(
                  <InputNumber step={1} />
                )}
              </FormItem>
            </Col>
            <Col span={8} className="hide">
              <FormItem label="Max Batch Data Size (Mb)：" {...itemStylePEM}>
                {getFieldDecorator('maxRecords', {
                  rules: [{
                    required: true,
                    message: textMessage
                  }, {
                    validator: forceCheckNum
                  }]
                })(
                  <InputNumber min={10} max={50} />
                )}
              </FormItem>
            </Col>
          </Row>
          ) : ''
        }
        {streamSubPanelKey === 'spark' || tabPanelKey === 'job' ? (
          <Row>
            <Col span={24}>
              <FormItem label="Others：" {...itemStyle}>
                {getFieldDecorator('personalConf', {})(
                  <Input
                    type="textarea"
                    placeholder={locale === 'en' ? 'Format: key=value; enter into a new line as long as there is a new item' : '格式如：key=value，多条时换行输入'}
                    autosize={{ minRows: 6, maxRows: 10 }}
                  />
                )}
              </FormItem>
            </Col>
          </Row>
          ) : ''
        }
        {/* flink */}
        {streamSubPanelKey === 'flink' ? (
          <Row>
            <Col span={12}>
              <FormItem label="JobManager Memory(GB)：" {...itemStyleOthers}>
                {getFieldDecorator('jobManagerMemoryGB', {
                  rules: [{
                    required: true,
                    message: textMessage
                  }, {
                    validator: forceCheckNum
                  }]
                })(
                  <InputNumber min={1} step={1} />
                )}
              </FormItem>
            </Col>
            <Col span={12}>
              <FormItem label="TaskManager Number：" {...itemStyleOthers}>
                {getFieldDecorator('taskManagersNumber', {
                  rules: [{
                    required: true,
                    message: textMessage
                  }, {
                    validator: forceCheckNum
                  }]
                })(
                  <InputNumber min={1} step={1} />
                )}
              </FormItem>
            </Col>
          </Row>
          ) : ''
        }
        {streamSubPanelKey === 'flink' ? (
          <Row>
            <Col span={12}>
              <FormItem label="Per TaskManager Memory(GB)：" {...itemStyleOthers}>
                {getFieldDecorator('perTaskManagerMemoryGB', {
                  rules: [{
                    required: true,
                    message: textMessage
                  }, {
                    validator: forceCheckNum
                  }]
                })(
                  <InputNumber min={1} step={1} />
                )}
              </FormItem>
            </Col>
            <Col span={12}>
              <FormItem label="Per TaskManager Slots：" {...itemStyleOthers}>
                {getFieldDecorator('perTaskManagerSlots', {
                  rules: [{
                    required: true,
                    message: textMessage
                  }, {
                    validator: forceCheckNum
                  }]
                })(
                  <InputNumber min={1} step={1} />
                )}
              </FormItem>
            </Col>
          </Row>
          ) : ''
        }
      </Form>
    )
  }
}

StreamConfigForm.propTypes = {
  form: PropTypes.any,
  tabPanelKey: PropTypes.string,
  locale: PropTypes.string,
  streamSubPanelKey: PropTypes.string
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, null)(StreamConfigForm))
