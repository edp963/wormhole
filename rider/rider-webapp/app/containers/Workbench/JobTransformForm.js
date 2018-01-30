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
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import Form from 'antd/lib/form'
const FormItem = Form.Item
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Popover from 'antd/lib/popover'
import Tooltip from 'antd/lib/tooltip'
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
import Radio from 'antd/lib/radio'
const RadioButton = Radio.Button
const RadioGroup = Radio.Group

export class JobTransformForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }
  onJobTransTypeSelect = (e) => {
    this.props.onInitJobTransValue(e.target.value)
  }

  forceCheckTimeoutSave = (rule, value, callback) => {
    const reg = /^\d+$/
    if (reg.test(value)) {
      callback()
    } else {
      callback('必须是数字')
    }
  }

  render () {
    const { form } = this.props
    const { transformValue } = this.props
    const { getFieldDecorator } = form

    const itemStyle = {
      labelCol: { span: 7 },
      wrapperCol: { span: 16 }
    }

    const transformTypeClassNames = [
      transformValue === 'sparkSql' ? '' : 'hide',
      transformValue === 'transformClassName' ? '' : 'hide'
    ]

    const transformTypeHiddens = [
      transformValue !== 'sparkSql',
      transformValue !== 'transformClassName'
    ]

    const sparkSqlMsg = (
      <span>
        SQL
        <Tooltip title={<FormattedMessage {...messages.workbenchHelp} />}>
          <Popover
            placement="top"
            content={<div style={{ width: '400px', height: '90px' }}>
              <p>sql 语句中的 table 为 source namespace 中第四层, </p>
              <p>for example: source namespace 为 kafka.test.test1.test2.*.*.*, sql 语句为 select * from test2;</p>
            </div>}
            title={<h3><FormattedMessage {...messages.workbenchHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    return (
      <Form className="transform-modal-style">
        <Row>
          <Col span={24}>
            <FormItem label="Source Namespace" {...itemStyle}>
              {getFieldDecorator('step2SourceNamespace', {})(
                <strong className="value-font-style">{this.props.step2SourceNamespace}</strong>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Sink Namespace" {...itemStyle}>
              {getFieldDecorator('step2SinkNamespace', {})(
                <strong className="value-font-style">{this.props.step2SinkNamespace}</strong>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem className="hide">
              {getFieldDecorator('editTransformId', {})(
                <Input />
              )}
            </FormItem>
            <FormItem label="Transformation" {...itemStyle}>
              {getFieldDecorator('transformation', {
                rules: [{
                  required: true,
                  message: '请选择 Transformation'
                }]
              })(
                <RadioGroup onChange={this.onJobTransTypeSelect}>
                  <RadioButton value="sparkSql">Spark SQL</RadioButton>
                  <RadioButton value="transformClassName">ClassName</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>

          {/* 设置 Spark Sql */}
          <Col span={7} className={transformTypeClassNames[0]}>
            <FormItem label={sparkSqlMsg} className="tran-sql-label">
              {getFieldDecorator('sparkSql', {
                hidden: transformTypeHiddens[0]
              })(
                <Input className="hide" />
              )}
            </FormItem>

          </Col>
          <Col span={16} className={`${transformTypeClassNames[0]} cm-sql-textarea`}>
            <textarea
              id="jobSparkSqlTextarea"
              placeholder="Spark SQL"
            />
          </Col>

          {/* 设置 ClassName */}
          <Col span={24} className={transformTypeClassNames[1]}>
            <FormItem label="ClassName" {...itemStyle}>
              {getFieldDecorator('transformClassName', {
                rules: [{
                  required: true,
                  message: '请填写 ClassName'
                }],
                hidden: transformTypeHiddens[1]
              })(
                <Input type="textarea" placeholder="ClassName" autosize={{ minRows: 5, maxRows: 8 }} />
              )}
            </FormItem>
          </Col>
        </Row>
      </Form>

    )
  }
}

JobTransformForm.propTypes = {
  form: React.PropTypes.any,
  transformValue: React.PropTypes.string,
  step2SinkNamespace: React.PropTypes.string,
  step2SourceNamespace: React.PropTypes.string,
  onInitJobTransValue: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(JobTransformForm)
