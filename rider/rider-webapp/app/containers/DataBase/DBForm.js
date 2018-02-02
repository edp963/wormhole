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

import { forceCheckNum } from '../../utils/util'
import DataSystemSelector from '../../components/DataSystemSelector'
import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
const FormItem = Form.Item
import Select from 'antd/lib/select'
const Option = Select.Option

export class DBForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      databaseDSValue: '',
      currentDatabaseUrlValue: [],
      connUrlText: ''
    }
  }

  componentWillReceiveProps (props) {
    if (props.databaseUrlValue) {
      this.setState({ currentDatabaseUrlValue: props.databaseUrlValue })
    }
  }

  // 显示 instance 下拉框的内容
  onDatabaseDataSystemItemSelect = (value) => {
    this.setState({ databaseDSValue: value })
    if (this.props.databaseFormType === 'add') {
      this.props.onInitDatabaseUrlValue(value)
    }
  }

  // 选择不同的 instance 显示不同的 connection url
  onHandleChangeInstance = (e) => {
    const selUrl = this.state.currentDatabaseUrlValue.find(s => s.id === Number(e))

    this.props.form.setFieldsValue({ connectionUrl: selUrl.connUrl })
    this.setState({ connUrlText: selUrl.connUrl })
  }

  // 验证 name 是否存在
  onNameInputChange = (e) => this.props.onInitDatabaseInputValue(e.target.value)

  // config 是否包含必须的字段
  onConfigValChange = (e) => this.props.onInitDatabaseConfigValue(e.target.value)

  render () {
    const { getFieldDecorator } = this.props.form
    const { databaseFormType, queryConnUrl } = this.props
    const { databaseDSValue, currentDatabaseUrlValue, connUrlText } = this.state
    const languageText = localStorage.getItem('preferredLanguage')

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 16 }
    }

    const DBDataSystemData = [
      { value: 'kafka', icon: 'icon-kafka', style: {fontSize: '35px'} },
      { value: 'oracle', icon: 'icon-amy-db-oracle', style: {lineHeight: '40px'} },
      { value: 'mysql', icon: 'icon-mysql' },
      { value: 'es', icon: 'icon-elastic', style: {fontSize: '24px'} },
      { value: 'hbase', icon: 'icon-hbase1' },
      { value: 'phoenix', text: 'Phoenix' },
      { value: 'cassandra', icon: 'icon-cass', style: {fontSize: '52px', lineHeight: '60px'} },
      { value: 'postgresql', icon: 'icon-postgresql', style: {fontSize: '31px'} },
      { value: 'mongodb', icon: 'icon-mongodb', style: {fontSize: '26px'} },
      { value: 'redis', icon: 'icon-redis', style: {fontSize: '31px'} },
      { value: 'vertica', icon: 'icon-vertica', style: {fontSize: '45px'} },
      { value: 'hdfs', icon: 'icon-hdfs1', style: {fontSize: '67px'} }
    ]

    // kafka 独立样式 hide /show
    const databaseDSKafkaShowClass = databaseDSValue === 'kafka' ? '' : 'hide'

    // kafka 实际隐藏(必填hide/show)
    const kafkaTypeHiddens = [
      databaseDSValue !== 'kafka',
      databaseDSValue === 'kafka'
    ]

    // user/password 样式/实际数据的 hide/show
    let uerPwdRequiredClass = ''
    let userPwdHiddensRequired = false
    if (databaseDSValue === 'oracle' || databaseDSValue === 'mysql' ||
      databaseDSValue === 'postgresql' || databaseDSValue === 'vertica') {
      uerPwdRequiredClass = ''
      userPwdHiddensRequired = false
    } else {
      uerPwdRequiredClass = 'hide'
      userPwdHiddensRequired = true
    }

    let uerPwdClass = ''
    let userPwdHiddens = false
    if (databaseDSValue === 'oracle' || databaseDSValue === 'mysql' ||
      databaseDSValue === 'postgresql' || databaseDSValue === 'kafka' ||
      databaseDSValue === 'vertica') {
      uerPwdClass = 'hide'
      userPwdHiddens = true
    } else {
      uerPwdClass = ''
      userPwdHiddens = false
    }

    let databaseDSLabel = ''
    let databaseDSPlace = ''
    if (databaseDSValue === 'kafka') {
      databaseDSLabel = 'Topic Name'
      databaseDSPlace = 'Topic Name'
    } else if (databaseDSValue === 'es') {
      databaseDSLabel = 'Index Name'
      databaseDSPlace = 'Index Name'
    } else if (databaseDSValue === 'hbase') {
      databaseDSLabel = 'Namespace Name'
      databaseDSPlace = `Namespace Name（${languageText === 'en' ? 'Fill in "default" if it is missing' : '若无, 填写 default'}）`
    } else if (databaseDSValue === 'redis') {
      databaseDSLabel = 'Database Name'
      databaseDSPlace = `${languageText === 'en' ? 'You can fill in "default"' : '可填写 default'}`
    } else {
      databaseDSLabel = 'Database Name'
      databaseDSPlace = 'Database Name'
    }

    let diffPlacehodler = ''
    if (languageText === 'en') {
      diffPlacehodler = databaseDSValue === 'oracle'
        ? 'Form: multiple lines of key=value or one line of key=value&key=value. When you select Oracle, "service_name" should be contained in Config.'
        : 'Form: multiple lines of key=value or one line of key=value&key=value'
    } else {
      diffPlacehodler = databaseDSValue === 'oracle'
        ? '格式为: 多行key=value 或 一行key=value&key=value。Oracle时, 必须包含"service_name"字段'
        : '格式为: 多行key=value 或 一行key=value&key=value'
    }

    // edit 时，不能修改部分元素
    let disabledOrNot = false
    let urlText = ''
    if (databaseFormType === 'add') {
      disabledOrNot = false
      urlText = connUrlText
    } else if (databaseFormType === 'edit') {
      disabledOrNot = true
      urlText = queryConnUrl
    }

    // oracle config 显示必填
    const onlyOracleClass = databaseDSValue === 'oracle' ? 'only-oracle-class' : ''

    const instanceOptions = currentDatabaseUrlValue.map(s => (<Option key={s.id} value={`${s.id}`}>{s.nsInstance}</Option>))

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
              {getFieldDecorator('dataBaseDataSystem', {
                rules: [{
                  required: true,
                  message: <FormattedMessage {...messages.dbTableList} />
                }]
              })(
                <DataSystemSelector
                  data={DBDataSystemData}
                  onItemSelect={this.onDatabaseDataSystemItemSelect}
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
                  message: `${languageText === 'en' ? 'Please select Instance' : '请填写 Instance'}`
                }]
              })(
                <Select
                  dropdownClassName="ri-workbench-select-dropdown db-workbench-select-dropdown"
                  onChange={this.onHandleChangeInstance}
                  placeholder="Select an Instance"
                  disabled={disabledOrNot}
                >
                  {instanceOptions}
                </Select>
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Connection URL" {...itemStyle}>
              {getFieldDecorator('connectionUrl', {})(
                <strong>{urlText}</strong>
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label={databaseDSLabel} {...itemStyle}>
              {getFieldDecorator('nsDatabase', {
                rules: [{
                  required: true,
                  message: `${languageText === 'en' ? 'Please fill in the' : '请填写'} ${databaseDSLabel}`
                }]
              })(
                <Input
                  placeholder={databaseDSPlace}
                  disabled={disabledOrNot}
                  onChange={this.onNameInputChange}
                />
              )}
            </FormItem>
          </Col>

          <Col span={24} className={uerPwdClass}>
            <FormItem label="User" {...itemStyle}>
              {getFieldDecorator('user', {
                hidden: userPwdHiddens
              })(
                <Input placeholder="User" />
              )}
            </FormItem>
          </Col>
          <Col span={24} className={uerPwdClass}>
            <FormItem label="Password" {...itemStyle}>
              {getFieldDecorator('password', {
                hidden: userPwdHiddens
              })(
                <Input placeholder="Password" />
              )}
            </FormItem>
          </Col>

          <Col span={24} className={uerPwdRequiredClass}>
            <FormItem label="User" {...itemStyle}>
              {getFieldDecorator('userRequired', {
                rules: [{
                  required: true,
                  message: `${languageText === 'en' ? 'Please fill in the User' : '请填写 User'}`
                }],
                hidden: userPwdHiddensRequired
              })(
                <Input placeholder="User" />
              )}
            </FormItem>
          </Col>
          <Col span={24} className={uerPwdRequiredClass}>
            <FormItem label="Password" {...itemStyle}>
              {getFieldDecorator('passwordRequired', {
                rules: [{
                  required: true,
                  message: `${languageText === 'en' ? 'Please fill in the Password' : '请填写 Password'}`
                }],
                hidden: userPwdHiddensRequired
              })(
                <Input type="password" placeholder="Password" />
              )}
            </FormItem>
          </Col>

          <Col span={24} className={databaseDSKafkaShowClass}>
            <FormItem label="Partition" {...itemStyle}>
              {getFieldDecorator('partition', {
                rules: [{
                  required: true,
                  message: `${languageText === 'en' ? 'Please fill in the Partition' : '请填写 Partition'}`
                }, {
                  validator: forceCheckNum
                }],
                hidden: kafkaTypeHiddens[0]
              })(
                <InputNumber min={1} step={1} placeholder="Partition" tyle={{ width: '100%' }} />
              )}
            </FormItem>
          </Col>

          <Col span={24} className={onlyOracleClass}>
            <FormItem label="Config" {...itemStyle}>
              {getFieldDecorator('config', {})(
                <Input
                  type="textarea"
                  placeholder={diffPlacehodler}
                  autosize={{ minRows: 3, maxRows: 8 }}
                  onChange={this.onConfigValChange}
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

DBForm.propTypes = {
  form: React.PropTypes.any,
  type: React.PropTypes.string,
  databaseFormType: React.PropTypes.string,
  queryConnUrl: React.PropTypes.string,
  onInitDatabaseInputValue: React.PropTypes.func,
  onInitDatabaseConfigValue: React.PropTypes.func,
  onInitDatabaseUrlValue: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(DBForm)
