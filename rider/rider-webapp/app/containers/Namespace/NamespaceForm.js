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
import {connect} from 'react-redux'
import { createStructuredSelector } from 'reselect'
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Table from 'antd/lib/table'
import Input from 'antd/lib/input'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
import Icon from 'antd/lib/icon'
import Button from 'antd/lib/button'
import Popconfirm from 'antd/lib/popconfirm'
const FormItem = Form.Item
import Select from 'antd/lib/select'
const Option = Select.Option

import DataSystemSelector from '../../components/DataSystemSelector'
import { loadDataSystemData } from '../../components/DataSystemSelector/dataSystemFunction'

import { selectLocale } from '../LanguageProvider/selectors'

export class NamespaceForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      namespaceDSValue: '',
      currentNamespaceUrlValue: []
    }
  }

  componentWillReceiveProps (props) {
    if (props.namespaceUrlValue) {
      this.setState({
        currentNamespaceUrlValue: props.namespaceUrlValue
      }, () => {

      })
    }
  }

  // 显示 connection url 内容
  onDatabaseDataSystemItemSelect = (value) => {
    this.setState({ namespaceDSValue: value })
    if (this.props.namespaceFormType === 'add') {
      this.props.onInitNamespaceUrlValue(value)
    }
  }

  onHandleChange = (e) => {
    const { currentNamespaceUrlValue } = this.state

    const selUrl = currentNamespaceUrlValue.find(s => s.id === Number(e))
    this.props.form.setFieldsValue({ connectionUrl: selUrl.connUrl })

    // 通过 instance id 显示 database 下拉框
    this.props.onInitDatabaseSelectValue(selUrl.id, selUrl.connUrl)
    this.props.cleanNsTableData()
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { currentNamespaceUrlValue, namespaceDSValue } = this.state
    const {
      namespaceFormType, databaseSelectValue, queryConnUrl, namespaceTableSource, onDeleteTable,
      onAddTable, deleteTableClass, addTableClass, addTableClassTable, addBtnDisabled, locale
    } = this.props

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 17 }
    }

    const disabledOrNot = namespaceFormType === 'edit'

    const instanceOptions = currentNamespaceUrlValue.map(s => (<Option key={s.id} value={`${s.id}`}>{s.nsInstance}</Option>))
    const databaseOptions = databaseSelectValue.map((s) => (<Option key={s.id} value={`${s.id}`}>{s.nsDatabase}</Option>))

    let namespaceDBLabel = ''
    let namespaceDBPlace = ''
    if (namespaceDSValue === 'es') {
      namespaceDBLabel = 'Index'
      namespaceDBPlace = locale === 'en' ? 'select an Index' : '请选择 Index'
    } else if (namespaceDSValue === 'hbase') {
      namespaceDBLabel = 'Namespace'
      namespaceDBPlace = locale === 'en' ? 'select a Hbase Namespace' : '请选择 Hbase Namespace'
    } else if (namespaceDSValue === 'kafka') {
      namespaceDBLabel = 'Topic'
      namespaceDBPlace = locale === 'en' ? 'select a Topic' : '请选择 Topic'
    } else {
      namespaceDBLabel = 'Database'
      namespaceDBPlace = locale === 'en' ? 'select a Database' : '请选择 Database'
    }

    let namespaceTablePlace = ''
    if (namespaceDSValue === 'es') {
      namespaceTablePlace = 'Type'
    } else if (namespaceDSValue === 'redis') {
      namespaceTablePlace = locale === 'en' ? 'You can fill in "default"' : '可填写 default'
    } else {
      namespaceTablePlace = 'Table'
    }

    let namespaceKeyPlaceholder = ''
    if (namespaceDSValue === 'redis') {
      namespaceKeyPlaceholder = locale === 'en' ? 'No config for Key' : 'Key 无需配置'
    } else {
      namespaceKeyPlaceholder = locale === 'en' ? 'Sep keys with commas' : '多个主键用逗号隔开'
    }

    const questionOrNot = namespaceDSValue === 'kafka'
      ? (
        <Tooltip title={<FormattedMessage {...messages.nsHelp} />}>
          <Popover
            placement="top"
            content={
              <div style={{ width: '400px', height: '38px' }}>
                <p><FormattedMessage {...messages.nsModalTablesTablesKafkaMsg} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.nsHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>)
      : ''

    const namespaceTableMsg = (
      <span>
        {namespaceDSValue === 'es' ? 'Types' : 'Tables'}
        {questionOrNot}
      </span>
    )

    const columns = [{
      title: 'Table',
      dataIndex: 'nsModalTable',
      key: 'nsModalTable',
      width: '41.4%'
    }, {
      title: 'Key',
      dataIndex: 'nsModalKey',
      key: 'nsModalKey',
      width: '42%'
    }, {
      title: 'Action',
      key: 'action',
      className: `${deleteTableClass} ns-table-delete-btn`,
      render: (text, record, index) => (
        <span className="ant-table-action-column">
          <Popconfirm placement="bottom" title={<FormattedMessage {...messages.nsModalSureDeleteTable} />} okText="Yes" cancelText="No" onConfirm={onDeleteTable(index)}>
            <Tooltip title={<FormattedMessage {...messages.nsModalDeleteTable} />}>
              <Button shape="circle" type="ghost">
                <i className="iconfont icon-jian"></i>
              </Button>
            </Tooltip>
          </Popconfirm>
        </span>
      )
    }]

    const pagination = {
      defaultPageSize: 5,
      pageSizeOptions: ['5', '10', '15'],
      showSizeChanger: true,
      onShowSizeChange: (current, pageSize) => {
        this.setState({
          pageIndex: current,
          pageSize: pageSize
        })
      },
      onChange: (current) => {
        this.setState({ pageIndex: current })
      }
    }

    return (
      <Form>
        <Row gutter={8}>
          <Col span={24}>
            <FormItem className="hide">
              {getFieldDecorator('id', {
                hidden: this.props.namespaceFormType === 'add'
              })(
                <Input />
              )}
            </FormItem>
            <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
              {getFieldDecorator('dataBaseDataSystem', {
                rules: [{
                  required: true,
                  message: locale === 'en' ? 'Please select Data System' : '请选择 Data System'
                }]
              })(
                <DataSystemSelector
                  data={loadDataSystemData()}
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
                  message: locale === 'en' ? 'Please select an Instance' : '请选择 Instance'
                }]
              })(
                <Select
                  dropdownClassName="ri-workbench-select-dropdown db-workbench-select-dropdown"
                  onChange={this.onHandleChange}
                  placeholder={locale === 'en' ? 'Select an Instance' : '请选择 Instance'}
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
                <strong>{queryConnUrl}</strong>
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label={namespaceDBLabel} {...itemStyle}>
              {getFieldDecorator('nsDatabase', {
                rules: [{
                  required: true,
                  message: `${locale === 'en' ? 'Please select a' : '请选择'} ${namespaceDBLabel}`
                }]
              })(
                <Select
                  dropdownClassName="ri-workbench-select-dropdown db-workbench-select-dropdown"
                  onChange={() => this.props.cleanNsTableData()}
                  placeholder={namespaceDBPlace}
                  disabled={disabledOrNot}
                >
                  {databaseOptions}
                </Select>
              )}
            </FormItem>
          </Col>

          <span>
            <Col span={6} className="ns-add-table-label-class">
              <FormItem label={namespaceTableMsg} style={{ marginRight: '-2px' }}>
                {getFieldDecorator('nsTables', {})(
                  <Input className="hide" />
                )}
              </FormItem>
            </Col>
            <Col span={7}>
              <FormItem label="" style={{ marginLeft: '2px' }}>
                {getFieldDecorator('nsSingleTableName', {})(
                  <Input
                    placeholder={namespaceTablePlace}
                    onChange={(e) => this.props.onInitNsNameInputValue(e.target.value)}
                    disabled={disabledOrNot}
                    />
                  )}
              </FormItem>
            </Col>
            <Col span={7}>
              <FormItem label="">
                {getFieldDecorator('nsSingleKeyValue', {})(
                  <Input
                    placeholder={namespaceKeyPlaceholder}
                    onChange={(e) => this.props.onInitNsKeyInputValue(e.target.value)}
                    disabled={namespaceDSValue === 'redis'}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={1} style={{width: '2.7%'}}></Col>
            <Col span={2} className={addTableClass}>
              <Tooltip title={<FormattedMessage {...messages.nsModalAddTable} />}>
                <Button shape="circle" type="ghost" style={{ borderColor: '#cfcfcf', marginTop: '2px' }} onClick={onAddTable} disabled={addBtnDisabled}>
                  <i className="iconfont icon-jia"></i>
                </Button>
              </Tooltip>
            </Col>
          </span>
          <Col span={6} className={addTableClassTable}></Col>
          <Col span={17} className="ns-add-table-class" style={{ paddingLeft: '2px' }}>
            <Table
              className={`${addTableClassTable} ns-add-table`}
              dataSource={namespaceTableSource}
              columns={columns}
                  // showHeader={false}
              pagination={pagination}
              bordered
            />
          </Col>
        </Row>
      </Form>
    )
  }
}

NamespaceForm.propTypes = {
  form: PropTypes.any,
  namespaceFormType: PropTypes.string,
  namespaceTableSource: PropTypes.array,
  databaseSelectValue: PropTypes.array,
  deleteTableClass: PropTypes.string,
  addTableClass: PropTypes.string,
  addTableClassTable: PropTypes.string,
  queryConnUrl: PropTypes.string,
  addBtnDisabled: PropTypes.bool,
  onInitNamespaceUrlValue: PropTypes.func,
  onInitDatabaseSelectValue: PropTypes.func,
  onDeleteTable: PropTypes.func,
  onAddTable: PropTypes.func,
  cleanNsTableData: PropTypes.func,
  onInitNsNameInputValue: PropTypes.func,
  onInitNsKeyInputValue: PropTypes.func,
  locale: PropTypes.string
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, null)(NamespaceForm))
