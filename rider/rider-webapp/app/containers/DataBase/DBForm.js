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
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
const FormItem = Form.Item
import Select from 'antd/lib/select'
const Option = Select.Option

import { forceCheckNum } from '../../utils/util'
import DataSystemSelector from '../../components/DataSystemSelector'
import { loadDataSystemData } from '../../components/DataSystemSelector/dataSystemFunction'
import { checkDatabaseName, loadDatabasesInstance } from './action'
import { selectLocale } from '../LanguageProvider/selectors'

export class DBForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      databaseDSValue: '',
      currentDatabaseUrlValue: [],
      currentInstanceId: '',
      connUrlText: ''
    }
  }

  componentWillReceiveProps (props) {
    if (props.databaseUrlValue) {
      this.setState({ currentDatabaseUrlValue: props.databaseUrlValue })
    }
  }

  checkDatabaseName = (rule, value, callback) => {
    const { oncheckDatabaseName, databaseFormType } = this.props
    const { currentInstanceId } = this.state

    databaseFormType === 'add'
      ? oncheckDatabaseName(currentInstanceId, value, res => callback(), err => callback(err))
      : callback()
  }

  // 显示 instance 下拉框内容
  onDatabaseDataSystemItemSelect = (value) => {
    this.setState({ databaseDSValue: value })
    if (this.props.databaseFormType === 'add') {
      this.props.onLoadDatabasesInstance(value, () => {
        // dbForm placeholder
        this.props.form.setFieldsValue({
          instance: undefined,
          nsDatabase: '',
          user: '',
          password: '',
          userRequired: '',
          passwordRequired: '',
          partition: '',
          config: '',
          description: ''
        })
        this.setState({
          connUrlText: ''
        })
      })
    }
  }

  checkConfig = (rules, value, callback) => {
    const { databaseDSValue } = this.state
    const { locale } = this.props
    const oracleErrorFormat = locale === 'en' ? 'When you select Oracle, "service_name" should be contained in Config.' : 'Oracle时, 必须包含"service_name"字段'
    if (databaseDSValue === 'oracle' && !value.includes('service_name')) {
      callback(oracleErrorFormat)
    } else {
      callback()
    }
  }

  onHandleChange = (e) => {
    // 不同 instance 显示不同 connection url
    const selUrl = this.state.currentDatabaseUrlValue.find(s => Object.is(s.id, Number(e)))
    this.props.form.setFieldsValue({
      connectionUrl: selUrl.connUrl
    })
    this.setState({
      connUrlText: selUrl.connUrl,
      currentInstanceId: selUrl.id
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { type, databaseFormType, queryConnUrl, locale } = this.props
    const { databaseDSValue, currentDatabaseUrlValue, connUrlText } = this.state

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 16 }
    }

    // kafka 实际隐藏(必填hide/show)
    const kafkaTypeHiddens = [
      databaseDSValue !== 'kafka',
      databaseDSValue === 'kafka'
    ]

    // user/password 样式/实际数据的 hide/show
    const dsValRequired = (databaseDSValue === 'oracle' || databaseDSValue === 'mysql' ||
      databaseDSValue === 'postgresql' || databaseDSValue === 'vertica')
    const uerPwdRequiredClass = dsValRequired ? '' : 'hide'
    const userPwdHiddensRequired = !dsValRequired

    const dsVal = (databaseDSValue === 'oracle' || databaseDSValue === 'mysql' ||
      databaseDSValue === 'postgresql' || databaseDSValue === 'kafka' || databaseDSValue === 'vertica')
    const userPwdHiddens = dsVal
    const uerPwdClass = dsVal ? 'hide' : ''

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
      databaseDSPlace = `Namespace Name（${locale === 'en' ? 'Fill in "default" if it is missing' : '若无, 填写 default'}）`
    } else if (databaseDSValue === 'redis') {
      databaseDSLabel = 'Database Name'
      databaseDSPlace = `${locale === 'en' ? 'You can fill in "default"' : '可填写 default'}`
    } else if (databaseDSValue === 'kudu') {
      databaseDSLabel = 'Database Name'
      databaseDSPlace = `Database Name（${locale === 'en' ? 'Fill in "default" if it is missing' : '若无, 填写 default'}）`
    } else {
      databaseDSLabel = 'Database Name'
      databaseDSPlace = 'Database Name'
    }

    let diffPlacehodler = ''
    if (locale === 'en') {
      diffPlacehodler = databaseDSValue === 'oracle'
        ? 'Form: multiple lines of key=value or one line of key=value&key=value. When you select Oracle, "service_name" should be contained in Config.'
        : 'Form: multiple lines of key=value or one line of key=value&key=value'
    } else {
      diffPlacehodler = databaseDSValue === 'oracle'
        ? '格式为: 多行key=value 或 一行key=value&key=value。Oracle时, 必须包含"service_name"字段'
        : '格式为: 多行key=value 或 一行key=value&key=value'
    }

    const configMsg = (
      <span>
        Config
        <Tooltip title={<FormattedMessage {...messages.dbHelp} />}>
          <Popover
            placement="top"
            content={
              <div style={{ width: '300px', height: '45px' }}>
                <p>
                  {databaseDSValue === 'oracle'
                    ? <FormattedMessage {...messages.dbHelpOrcale} />
                    : <FormattedMessage {...messages.dbHelpOthers} />}
                </p>
              </div>
            }
            title={
              <h3><FormattedMessage {...messages.dbHelp} /></h3>
            }
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const instanceOptions = currentDatabaseUrlValue.map(s => (<Option key={s.id} value={`${s.id}`}>{s.nsInstance}</Option>))

    return (
      <Form>
        <Row gutter={8}>
          <Col span={24}>
            <FormItem className="hide">
              {getFieldDecorator('id', {
                hidden: type === 'add'
              })(
                <Input />
              )}
            </FormItem>
            <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
              {getFieldDecorator('dataBaseDataSystem', {
                rules: [{
                  required: true,
                  message: `${locale === 'en' ? 'Please select Data System' : '请选择 Data System'}`
                }]
              })(
                <DataSystemSelector
                  data={loadDataSystemData()}
                  onItemSelect={this.onDatabaseDataSystemItemSelect}
                  dataSystemDisabled={databaseFormType === 'edit'}
                />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Instance" {...itemStyle}>
              {getFieldDecorator('instance', {
                rules: [{
                  required: true,
                  message: `${locale === 'en' ? 'Please select Instance' : '请选择 Instance'}`
                }]
              })(
                <Select
                  dropdownClassName="ri-workbench-select-dropdown db-workbench-select-dropdown"
                  onChange={this.onHandleChange}
                  placeholder={locale === 'en' ? 'Select an instance' : '请选择 Instance'}
                  disabled={databaseFormType === 'edit'}
                >
                  {instanceOptions}
                </Select>
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Connection URL" {...itemStyle}>
              {getFieldDecorator('connectionUrl', {})(
                <strong>
                  {databaseFormType === 'edit' ? queryConnUrl : connUrlText}
                </strong>
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label={databaseDSLabel} {...itemStyle}>
              {getFieldDecorator('nsDatabase', {
                rules: [{
                  required: true,
                  message: `${locale === 'en' ? 'Please fill in ' : '请填写'} ${databaseDSLabel}`
                }, {
                  validator: this.checkDatabaseName
                }]
              })(
                <Input placeholder={databaseDSPlace} disabled={databaseFormType === 'edit'} />
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
                  message: `${locale === 'en' ? 'Please fill in the User' : '请填写 User'}`
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
                  message: `${locale === 'en' ? 'Please fill in the Password' : '请填写 Password'}`
                }],
                hidden: userPwdHiddensRequired
              })(
                <Input type="password" placeholder="Password" />
              )}
            </FormItem>
          </Col>

          <Col span={24} className={databaseDSValue === 'kafka' ? '' : 'hide'}>
            <FormItem label="Partition" {...itemStyle}>
              {getFieldDecorator('partition', {
                rules: [{
                  required: true,
                  message: `${locale === 'en' ? 'Please fill in the Partition' : '请填写 Partition'}`
                }, {
                  validator: forceCheckNum
                }],
                hidden: kafkaTypeHiddens[0]
              })(
                <InputNumber min={1} step={1} placeholder="Partition" />
              )}
            </FormItem>
          </Col>

          <Col span={24} className={databaseDSValue === 'oracle' ? 'only-oracle-class' : ''}>
            <FormItem label={configMsg} {...itemStyle}>
              {getFieldDecorator('config', {
                rules: [{
                  validator: this.checkConfig
                }]
              })(
                <Input type="textarea" placeholder={diffPlacehodler} autosize={{ minRows: 3, maxRows: 8 }} />
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
  form: PropTypes.any,
  type: PropTypes.string,
  databaseFormType: PropTypes.string,
  queryConnUrl: PropTypes.string,
  onLoadDatabasesInstance: PropTypes.func,
  oncheckDatabaseName: PropTypes.func,
  locale: PropTypes.string
}

function mapDispatchToProps (dispatch) {
  return {
    oncheckDatabaseName: (id, name, resolve, reject) => dispatch(checkDatabaseName(id, name, resolve, reject)),
    onLoadDatabasesInstance: (value, resolve) => dispatch(loadDatabasesInstance(value, resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, mapDispatchToProps)(DBForm))
