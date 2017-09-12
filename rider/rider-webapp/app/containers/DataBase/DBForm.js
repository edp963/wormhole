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
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
const FormItem = Form.Item
import Select from 'antd/lib/select'
const Option = Select.Option
import Radio from 'antd/lib/radio'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button

export class DBForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      databaseDSValue: '',
      permissionValue: ''
    }
  }

  onChangeDBROOrRW = (e) => {
    this.setState({
      permissionValue: e.target.value
    })
    this.props.form.setFieldsValue({
      nsDatabase: ''
    })
  }

  // 显示 connection url 下拉框的内容
  onDatabaseDataSystemItemSelect = (value) => {
    this.setState({
      databaseDSValue: value
    })
    if (this.props.databaseFormType === 'add') {
      this.props.onInitDatabaseUrlValue(value)
    }
  }

  // 选择不同的 connection url 显示不同的 instance
  onHandleChangeUrl = (e) => {
    const selUrl = this.props.databaseUrlValue.find(s => s.id === Number(e))
    this.props.form.setFieldsValue({
      instance: selUrl.nsInstance
    })
  }

  // 验证 name 是否存在
  onNameInputChange = (e) => {
    this.props.onInitDatabaseInputValue(e.target.value)
  }

  // config 是否包含必须的字段
  onConfigValChange = (e) => {
    this.props.onInitDatabaseConfigValue(e.target.value)
  }

  forceCheckNumSave = (rule, value, callback) => {
    const reg = /^\d+$/
    if (reg.test(value)) {
      callback()
    } else {
      callback('必须是数字')
    }
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { databaseFormType, databaseUrlValue } = this.props
    const { databaseDSValue } = this.state

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 16 }
    }

    const DBDataSystemData = [
      { value: 'oracle', icon: 'icon-amy-db-oracle', style: {lineHeight: '40px'} },
      { value: 'mysql', icon: 'icon-mysql' },
      { value: 'es', icon: 'icon-elastic', style: {fontSize: '24px'} },
      { value: 'hbase', icon: 'icon-hbase1' },
      { value: 'phoenix', text: 'Phoenix' },
      { value: 'cassandra', icon: 'icon-cass', style: {fontSize: '52px', lineHeight: '60px'} },
      { value: 'log', text: 'Log' },
      { value: 'kafka', icon: 'icon-kafka', style: {fontSize: '35px'} }
    ]

    // kafka 独立样式hide/show
    const databaseDSKafkaShowClass = databaseDSValue === 'kafka' ? '' : 'hide'
    const databaseDSKafkaHideClass = databaseDSValue === 'kafka' ? 'hide' : ''

    // kafka 实际隐藏(必填hide/show)
    const kafkaTypeHiddens = [
      databaseDSValue !== 'kafka',
      databaseDSValue === 'kafka'
    ]

    // user/password 样式/实际数据的 hide/show
    let uerPwdRequiredClass = ''
    let userPwdHiddensRequired = false
    if (databaseDSValue === 'oracle' || databaseDSValue === 'mysql') {
      uerPwdRequiredClass = ''
      userPwdHiddensRequired = false
    } else {
      uerPwdRequiredClass = 'hide'
      userPwdHiddensRequired = true
    }

    let uerPwdClass = ''
    let userPwdHiddens = false
    if (databaseDSValue === 'oracle' || databaseDSValue === 'mysql' || databaseDSValue === 'kafka') {
      uerPwdClass = 'hide'
      userPwdHiddens = true
    } else {
      uerPwdClass = ''
      userPwdHiddens = false
    }

    const databaseDSLabel = databaseDSValue === 'kafka' ? 'Topic Name' : 'Database Name'
    const diffPlacehodler = databaseDSValue === 'oracle' ? 'Oracle 时，Config 必须包含"service_name"字段' : 'Config'

    // edit 时，不能修改部分元素
    let disabledOrNot = false
    if (databaseFormType === 'add') {
      disabledOrNot = false
    } else if (databaseFormType === 'edit') {
      disabledOrNot = true
    }

    const urlOptions = databaseUrlValue.map(s => (<Option key={s.id} value={`${s.id}`}>{s.connUrl}</Option>))

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
                  message: '请选择 Data System'
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
            <FormItem label="Connection URL" {...itemStyle}>
              {getFieldDecorator('connectionUrl', {
                rules: [{
                  required: true,
                  message: '请选择 Connection URL'
                }]
              })(
                <Select
                  dropdownClassName="ri-workbench-select-dropdown db-workbench-select-dropdown"
                  onChange={this.onHandleChangeUrl}
                  placeholder="Select a Connection URL"
                  disabled={disabledOrNot}
                >
                  {urlOptions}
                </Select>
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
                <Input placeholder="Instance" disabled />
              )}
            </FormItem>
          </Col>

          <Col span={24} className={databaseDSKafkaHideClass}>
            <FormItem label="Permission" {...itemStyle}>
              {getFieldDecorator('permission', {
                rules: [{
                  required: true,
                  message: '请填写 Permission'
                }],
                hidden: kafkaTypeHiddens[1]
              })(
                <RadioGroup className="ro-rw-style" onChange={this.onChangeDBROOrRW} disabled={disabledOrNot}>
                  <RadioButton value="ReadOnly" className="read-only-style">ReadOnly</RadioButton>
                  <RadioButton value="ReadWrite" className="read-write-style">ReadWrite</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label={databaseDSLabel} {...itemStyle}>
              {getFieldDecorator('nsDatabase', {
                rules: [{
                  required: true,
                  message: `请填写 ${databaseDSLabel}`
                }]
              })(
                <Input
                  placeholder={databaseDSLabel}
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
                  message: '请填写 User'
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
                  message: '请填写 Password'
                }],
                hidden: userPwdHiddensRequired
              })(
                <Input placeholder="Password" />
              )}
            </FormItem>
          </Col>

          <Col span={24} className={databaseDSKafkaShowClass}>
            <FormItem label="Partition" {...itemStyle}>
              {getFieldDecorator('partition', {
                rules: [{
                  required: true,
                  message: '请填写 Partition'
                }, {
                  validator: this.forceCheckNumSave
                }],
                hidden: kafkaTypeHiddens[0]
              })(
                <InputNumber min={1} step={1} placeholder="Partition" disabled={disabledOrNot} style={{ width: '100%' }} />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Config" {...itemStyle}>
              {getFieldDecorator('config', {})(
                <Input
                  type="textarea"
                  placeholder={diffPlacehodler}
                  autosize={{ minRows: 2, maxRows: 8 }}
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
  databaseUrlValue: React.PropTypes.array,
  onInitDatabaseInputValue: React.PropTypes.func,
  onInitDatabaseConfigValue: React.PropTypes.func,
  onInitDatabaseUrlValue: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(DBForm)
