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

import DataSystemSelector from '../../components/DataSystemSelector'
import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
const FormItem = Form.Item

export class InstanceForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      instanceDSValue: ''
    }
  }

  onUrlInputChange = (e) => {
    this.props.onInitInstanceInputValue(e.target.value)
  }

  onInstanceInputChange = (e) => {
    this.props.onInitInstanceExited(e.target.value)
  }

  onSourceDataSystemItemSelect = (e) => {
    this.setState({
      instanceDSValue: e
    })
    this.props.onInitInstanceSourceDs(e)
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { instanceFormType } = this.props
    const { instanceDSValue } = this.state

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 16 }
    }

    const instanceDataSystemData = [
      { value: 'kafka', icon: 'icon-kafka', style: {fontSize: '35px'} },
      { value: 'oracle', icon: 'icon-amy-db-oracle', style: {lineHeight: '40px'} },
      { value: 'mysql', icon: 'icon-mysql' },
      { value: 'es', icon: 'icon-elastic', style: {fontSize: '24px'} },
      { value: 'hbase', icon: 'icon-hbase1' },
      { value: 'phoenix', text: 'Phoenix' },
      { value: 'cassandra', icon: 'icon-cass', style: {fontSize: '52px', lineHeight: '60px'} },
      // { value: 'log', text: 'Log' },
      { value: 'postgresql', icon: 'icon-postgresql', style: {fontSize: '31px'} },
      { value: 'mongodb', icon: 'icon-mongodb', style: {fontSize: '26px'} }
    ]

    // edit 时，不能修改部分元素
    let disabledOrNot = false
    if (instanceFormType === 'add') {
      disabledOrNot = false
    } else if (instanceFormType === 'edit') {
      disabledOrNot = true
    }

    // help
    let questionDS = ''
    if (instanceDSValue === 'oracle' || instanceDSValue === 'mysql' || instanceDSValue === 'postgresql' || instanceDSValue === 'cassandra') {
      questionDS = `${instanceDSValue.substring(0, 1).toUpperCase()}${instanceDSValue.substring(1)} 时, 为 ip:port 格式`
    } else if (instanceDSValue === 'es') {
      questionDS = 'Elastic 时, 为 http(s)://ip:port 格式'
    } else if (instanceDSValue === 'hbase') {
      questionDS = 'Hbase 时, 为 zookeeper url list, 如localhost:2181/hbase, 多条用逗号隔开'
    } else if (instanceDSValue === 'phoenix') {
      questionDS = 'Phoenix 时, 为 zookeeper url, 如localhost:2181'
    } else if (instanceDSValue === 'kafka') {
      questionDS = 'Kafka 时, 为 borker list, localhost:9092, 多条用逗号隔开'
    } else {
      questionDS = '请选择 Data System'
    }

    const connectionURLMsg = (
      <span>
        Connection URL
        <Tooltip title="帮助">
          <Popover
            placement="top"
            content={<div style={{ width: '260px', height: '32px' }}>
              <p>{questionDS}</p>
            </div>}
            title={<h3>帮助</h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    return (
      <Form>
        <Row gutter={8}>
          <Col span={24}>
            <FormItem className="hide">
              {getFieldDecorator('id', {
                hidden: this.props.type === 'add'
              })(
                <Input />
              )}
            </FormItem>
            <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
              {getFieldDecorator('instanceDataSystem', {
                rules: [{
                  required: true,
                  message: '请选择 Data System'
                }]
              })(
                <DataSystemSelector
                  data={instanceDataSystemData}
                  onItemSelect={this.onSourceDataSystemItemSelect}
                  dataSystemDisabled={disabledOrNot}
                />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Instance" {...itemStyle}>
              {getFieldDecorator('instance', {
                rules: [{
                  required: true,
                  message: '请填写 Instance'
                }]
              })(
                <Input
                  placeholder="Instance"
                  onChange={this.onInstanceInputChange}
                />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label={connectionURLMsg} {...itemStyle}>
              {getFieldDecorator('connectionUrl', {
                rules: [{
                  required: true,
                  message: '请填写 Connection URL'
                }]
              })(
                <Input
                  placeholder="Connection URL"
                  onChange={this.onUrlInputChange}
                />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Description" {...itemStyle}>
              {getFieldDecorator('description', {})(
                <Input type="textarea" placeholder="Description" autosize={{ minRows: 3, maxRows: 8 }} />
              )}
            </FormItem>
          </Col>

        </Row>
      </Form>
    )
  }
}

InstanceForm.propTypes = {
  form: React.PropTypes.any,
  type: React.PropTypes.string,
  instanceFormType: React.PropTypes.string,
  onInitInstanceInputValue: React.PropTypes.func,
  onInitInstanceExited: React.PropTypes.func,
  onInitInstanceSourceDs: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(InstanceForm)
