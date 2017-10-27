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
require('../../../node_modules/codemirror/addon/display/placeholder')
require('../../../node_modules/codemirror/mode/javascript/javascript')

import DataSystemSelector from '../../components/DataSystemSelector'
import Form from 'antd/lib/form'
const FormItem = Form.Item
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'

export class WorkbenchSinkForm extends React.PureComponent {
  constructor (props) {
    super(props)
    this.state = {
      sinkMode: ''
    }
  }

  componentWillReceiveProps (props) {
    this.setState({
      sinkMode: props.sinkMode
    })
  }

  onSinkTypeSelect = (val) => this.props.onSinkTypeSelect(val)

  render () {
    const { getFieldDecorator } = this.props.form
    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 17 }
    }

    const sourceDataSystemData = [
      { value: 'oracle', icon: 'icon-amy-db-oracle' },
      { value: 'mysql', icon: 'icon-mysql' },
      { value: 'kafka', icon: 'icon-kafka', style: {fontSize: '35px'} }
    ]

    return (
      <Form className="ri-workbench-form">
        <Row gutter={8}>
          <Col span={24}>
            <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
              {getFieldDecorator('dataSystem', {
                rules: [{
                  required: true,
                  message: '请选择 Data System'
                }]
              })(
                <DataSystemSelector
                  data={sourceDataSystemData}
                  onItemSelect={this.onSourceDataSystemItemSelect}
                />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Instance" {...itemStyle}>
              {getFieldDecorator('instance', {
                rules: [{
                  required: true,
                  message: 'Instance 不能为空'
                }, {
                  // validator: this.forceCheckSave
                }]
              })(
                <Input placeholder="Instance" />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Database" {...itemStyle}>
              {getFieldDecorator('database', {
                rules: [{
                  required: true,
                  message: 'Database 不能为空'
                }]
              })(
                <Input placeholder="Database" />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="User Name" {...itemStyle}>
              {getFieldDecorator('username', {
                rules: [{
                  required: true,
                  message: 'User Name 不能为空'
                }]
              })(
                <Input placeholder="User Name" />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Password" {...itemStyle}>
              {getFieldDecorator('password', {
                rules: [{
                  required: true,
                  message: 'Password 不能为空'
                }]
              })(
                <Input placeholder="Password" />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Connection URL" {...itemStyle}>
              {getFieldDecorator('connectionUrl', {
                initialValue: ''
              })(
                <Input placeholder="Connection URL" />
              )}
            </FormItem>
          </Col>
        </Row>
      </Form>
    )
  }
}

WorkbenchSinkForm.propTypes = {
  form: React.PropTypes.any,
  onSinkTypeSelect: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(WorkbenchSinkForm)
