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

import Form from 'antd/lib/form'
const FormItem = Form.Item
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
import Tag from 'antd/lib/tag'
import Icon from 'antd/lib/icon'
import Radio from 'antd/lib/radio'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button

import Select from 'antd/lib/select'
const Option = Select.Option

export class WorkbenchStreamForm extends React.PureComponent {
  constructor (props) {
    super(props)
    this.state = {
      streamMode: ''
    }
  }

  componentWillReceiveProps (props) {
    this.setState({
      streamMode: props.streamMode
    })
  }

  // 验证 stream name 是否存在
  onNameInputChange = (e) => this.props.onInitStreamNameValue(e.target.value)

  onStreamTypeSelect = (e) => {
    // console.log('val', e.target.value)
  }

  forceCheckSave = (rule, value, callback) => {
    const reg = /^\w+$/
    if (reg.test(value)) {
      callback()
    } else {
      callback('必须是字母、数字或下划线')
    }
  }

  // handleTopicsChange = (value) => {
    // const { topicsValues } = this.props
    // if (value === '全选') {
    //   this.setState({
    //     topicChildren: ['全选']
    //   })
    // } else {
    //   for (let i = 0; i < topicsValues.length; i++) {
    //     this.setState({
    //       topicChildren: this.state.topicChildren.push(<Option key={topicsValues[i].id} value={`${topicsValues[i].id}`}>{topicsValues[i].name}</Option>)
    //     })
    //   }
    // }
  // }

  selectTopic = (value) => {
    console.log('va', value)
  }

  render () {
    const { isWormhole, onShowConfigModal, streamConfigCheck, kafkaValues } = this.props
    const { streamMode } = this.state
    const { getFieldDecorator } = this.props.form
    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 17 }
    }

    // edit 时，不能修改部分元素
    let disabledOrNot = false
    if (streamMode === 'add') {
      disabledOrNot = false
    } else if (streamMode === 'edit') {
      disabledOrNot = true
    }

    // const topicChildren = []
    // for (let i = 0; i < topicsValues.length; i++) {
    //   topicChildren.push(<Option key={topicsValues[i].id} value={`${topicsValues[i].id}`}>{topicsValues[i].name}</Option>)
    // }

    // const topicChildren = topicsValues.map(i => (<Option key={i.id} value={`${i.id}`}>{i.name}</Option>))

    const kafkaOptions = kafkaValues.map(s => (<Option key={s.id} value={`${s.id}`}>{s.nsInstance}</Option>))

    const streamConfigTag = streamConfigCheck === true
      ? (
        <Tag color="#7CB342" onClick={onShowConfigModal}>
          <Icon type="check-circle-o" /> 点击修改
        </Tag>
      )
      : (
        <Tag onClick={onShowConfigModal}>
          <Icon type="minus-circle-o" /> 点击修改
        </Tag>
      )

    return (
      <Form className="ri-workbench-form workbench-stream-form">
        <Row gutter={8}>
          <Col span={24}>
            <FormItem label="Name" {...itemStyle}>
              {getFieldDecorator('streamName', {
                rules: [{
                  required: true,
                  message: 'Name 不能为空'
                }, {
                  validator: this.forceCheckSave
                }]
              })(
                <Input placeholder="Name" disabled={disabledOrNot} onChange={this.onNameInputChange} />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Type" {...itemStyle}>
              {getFieldDecorator('type', {
                rules: [{
                  required: true,
                  message: '请选择 Type'
                }]
              })(
                <RadioGroup className="radio-group-style" onChange={this.onStreamTypeSelect} disabled={disabledOrNot} size="default">
                  <RadioButton value="default" className="radio-btn-style radio-btn-extra">Default</RadioButton>
                  <RadioButton value="hdfslog" className="radio-btn-style radio-btn-extra">Hdfslog</RadioButton>
                  <RadioButton value="routing" className="radio-btn-style radio-btn-final">Routing</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Description" {...itemStyle}>
              {getFieldDecorator('desc', {
                initialValue: ''
              })(
                <Input placeholder="Description" />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Kafka" {...itemStyle}>
              {getFieldDecorator('kafka', {
                rules: [{
                  required: true,
                  message: '请选择一个 Kafka'
                }]
              })(
                <Select
                  dropdownClassName="ri-workbench-select-dropdown"
                  placeholder="Select a Kafka"
                  disabled={disabledOrNot}
                >
                  {kafkaOptions}
                </Select>
              )}
            </FormItem>
          </Col>

          {/* <Col span={24}>
            <FormItem label="Topics" {...itemStyle}>
              {getFieldDecorator('topics', {
                rules: [{
                  required: true,
                  message: '请选择 Topic'
                }]
              })(
                <Select
                  tags
                  // mode="multiple"
                  placeholder="Select Topics"
                  searchPlaceholder="标签模式"
                  onChange={this.handleTopicsChange}
                  onSelect={this.selectTopic}
                >
                  {topicChildren}
                </Select>
              )}
            </FormItem>
          </Col> */}

          <Col span={24}>
            <div className="ant-col-6 ant-form-item-label">
              <label htmlFor="#">Configs</label>
            </div>
            <div className="ant-col-17">
              <div className="ant-form-item-control">
                {streamConfigTag}
              </div>
            </div>
          </Col>
          <Col span={24} className="hide">
            <FormItem>
              {getFieldDecorator('config', {})(<Input />)}
            </FormItem>
          </Col>

          <Col span={24} className={isWormhole ? 'hide' : ''}>
            <FormItem label="Source Topic Name" {...itemStyle}>
              {getFieldDecorator('sourceTopicName', {
                rules: [{
                  required: true,
                  message: 'Source Topic Name 不能为空'
                }],
                hidden: isWormhole
              })(
                <Input placeholder="Source Topic Name" />
              )}
            </FormItem>
          </Col>
          <Col span={24} className={isWormhole ? 'hide' : ''}>
            <FormItem label="Description" {...itemStyle}>
              {getFieldDecorator('sourceTopicDescription', {
                initialValue: '',
                hidden: isWormhole
              })(
                <Input placeholder="Source Topic Description" />
              )}
            </FormItem>
          </Col>
          <Col span={24} className={isWormhole ? 'hide' : ''}>
            <FormItem label="Sink Topic Name" {...itemStyle}>
              {getFieldDecorator('sinkTopicName', {
                rules: [{
                  required: true,
                  message: 'Sink Topic Name 不能为空'
                }],
                hidden: isWormhole
              })(
                <Input placeholder="Sink Topic Name" />
              )}
            </FormItem>
          </Col>
          <Col span={24} className={isWormhole ? 'hide' : ''}>
            <FormItem label="Description" {...itemStyle}>
              {getFieldDecorator('sinkTopicDescription', {
                initialValue: '',
                hidden: isWormhole
              })(
                <Input placeholder="Sink Topic Description" />
              )}
            </FormItem>
          </Col>
        </Row>
      </Form>
    )
  }
}

WorkbenchStreamForm.propTypes = {
  form: React.PropTypes.any,
  isWormhole: React.PropTypes.bool,
  kafkaValues: React.PropTypes.array,
  // topicsValues: React.PropTypes.array,
  onShowConfigModal: React.PropTypes.func,
  onInitStreamNameValue: React.PropTypes.func,
  streamConfigCheck: React.PropTypes.bool
}

export default Form.create({wrappedComponentRef: true})(WorkbenchStreamForm)
