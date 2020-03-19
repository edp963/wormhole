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
require('../../../node_modules/codemirror/addon/display/placeholder')
require('../../../node_modules/codemirror/mode/javascript/javascript')
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import Form from 'antd/lib/form'
const FormItem = Form.Item
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
import Popover from 'antd/lib/popover'
import Tooltip from 'antd/lib/tooltip'
import Tag from 'antd/lib/tag'
import Icon from 'antd/lib/icon'
import Radio from 'antd/lib/radio'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button

import Select from 'antd/lib/select'
const Option = Select.Option

import { operateLanguageSelect } from '../../utils/util'
import { loadStreamNameValue } from '../Manager/action'
import { selectLocale } from '../LanguageProvider/selectors'

export class WorkbenchStreamForm extends React.PureComponent {
  constructor (props) {
    super(props)
    this.state = {
      streamMode: ''
    }
  }

  componentWillReceiveProps (props) {
    this.setState({ streamMode: props.streamMode })
  }

  checkStreamName = (rule, value = '', callback) => {
    const { onLoadStreamNameValue, projectId, locale } = this.props

    const reg = /^[a-zA-Z0-9_-]*$/
    if (reg.test(value)) {
      onLoadStreamNameValue(projectId, value, res => callback(), (err) => callback(err))
    } else {
      const textZh = '必须是字母、数字、下划线或中划线'
      const textEn = 'It should be letters, figures, underscore or hyphen'
      callback(locale === 'en' ? textEn : textZh)
    }
  }

  render () {
    const { isWormhole, onShowConfigModal, streamConfigCheck, kafkaValues, locale, streamSubPanelKey } = this.props
    const { streamMode } = this.state
    const { getFieldDecorator } = this.props.form
    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 17 }
    }

    const kafkaOptions = kafkaValues.map(s => (<Option key={s.id} value={`${s.id}`}>{s.nsInstance}</Option>))

    const streamConfigTag = streamConfigCheck
      ? (
        <Tag color="#7CB342" onClick={onShowConfigModal}>
          <Icon type="check-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
        </Tag>
      )
      : (
        <Tag onClick={onShowConfigModal}>
          <Icon type="minus-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
        </Tag>
      )

    const warningMsg = (
      <span>
        Configs
        <Tooltip title={<FormattedMessage {...messages.workbenchHelp} />} placement="bottom">
          <Popover
            placement="top"
            content={
              <div style={{ width: '221px', height: '25px' }}>
                <p><FormattedMessage {...messages.workbenchTransResource} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.workbenchHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    return (
      <Form className="ri-workbench-form workbench-stream-form">
        <Row gutter={8}>

          <Col span={24}>
            <FormItem label="Stream type" {...itemStyle}>
              {getFieldDecorator('streamType', {
                rules: [{
                  required: true,
                  message: operateLanguageSelect('type', 'Type')
                }],
                initialValue: 'spark'
              })(
                <RadioGroup className="radio-group-style" disabled={streamMode === 'edit'} size="default" onChange={this.props.changeStreamType('stream')}>
                  <RadioButton value="spark" className="radio-btn-style radio-btn-extra">Spark</RadioButton>
                  <RadioButton value="flink" className="radio-btn-style radio-btn-extra">Flink</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Name" {...itemStyle}>
              {getFieldDecorator('streamName', {
                rules: [{
                  required: true,
                  message: locale === 'en' ? 'Name cannot be empty' : 'Name 不能为空'
                }, {
                  validator: this.checkStreamName
                }]
              })(
                <Input placeholder="Name" disabled={streamMode === 'edit'} />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Function type" {...itemStyle}>
              {getFieldDecorator('type', {
                rules: [{
                  required: true,
                  message: operateLanguageSelect('type', 'Type')
                }],
                initialValue: 'default'
              })(
                <RadioGroup className="radio-group-style" disabled={streamMode === 'edit'} size="default">
                  <RadioButton value="default" className="radio-btn-style radio-btn-extra">Default</RadioButton>
                  <RadioButton value="hdfslog" className={`radio-btn-style radio-btn-extra ${streamSubPanelKey === 'flink' ? 'hide' : ''}`}>Hdfslog</RadioButton>
                  <RadioButton value="routing" className={`radio-btn-style radio-btn-extra ${streamSubPanelKey === 'flink' ? 'hide' : ''}`}>Routing</RadioButton>
                  <RadioButton value="hdfscsv" className={`radio-btn-style radio-btn-extra ${streamSubPanelKey === 'flink' ? 'hide' : ''}`}>Hdfscsv</RadioButton>
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
                  message: operateLanguageSelect('type', 'Type')
                }]
              })(
                <Select
                  dropdownClassName="ri-workbench-select-dropdown"
                  placeholder="Select a Kafka"
                  disabled={streamMode === 'edit'}
                >
                  {kafkaOptions}
                </Select>
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <div className="ant-col-6 ant-form-item-label">
              <label htmlFor="#" className="sink-config-class">{warningMsg}</label>
            </div>
            <div className="ant-col-17">
              <div className="ant-form-item-control">
                {streamConfigTag}
              </div>
            </div>
          </Col>
          <Col span={24}>
            <FormItem label="Special Config" {...itemStyle}>
              {getFieldDecorator('specialConfig', {})(
                <textarea
                  placeholder="Paste your Sink Config JSON here."
                  className="ant-input ant-input-extra"
                  rows="5">
                </textarea>
              )}
            </FormItem>
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
                  message: locale === 'en' ? 'Source topic name cannot be empty' : 'Source Topic Name 不能为空'
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
                  message: locale === 'en' ? 'Sink topic name cannot be empty' : 'Sink Topic Name 不能为空'
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
  form: PropTypes.any,
  isWormhole: PropTypes.bool,
  kafkaValues: PropTypes.array,
  onShowConfigModal: PropTypes.func,
  onLoadStreamNameValue: PropTypes.func,
  streamConfigCheck: PropTypes.bool,
  projectId: PropTypes.string,
  locale: PropTypes.string,
  changeStreamType: PropTypes.func,
  streamSubPanelKey: PropTypes.string
}

function mapDispatchToProps (dispatch) {
  return {
    onLoadStreamNameValue: (projectId, name, resolve, reject) => dispatch(loadStreamNameValue(projectId, name, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, mapDispatchToProps)(WorkbenchStreamForm))
