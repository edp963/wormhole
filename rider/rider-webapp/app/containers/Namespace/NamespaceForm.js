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

export class NamespaceForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      namespaceDSValue: '',
      instanceIdGeted: 0,
      currentNamespaceUrlValue: []
    }
  }

  componentWillReceiveProps (props) {
    if (props.namespaceUrlValue) {
      this.setState({
        currentNamespaceUrlValue: props.namespaceUrlValue
      })
    }
  }

  // 显示 connection url 下拉框的内容
  onDatabaseDataSystemItemSelect = (value) => {
    this.setState({
      namespaceDSValue: value
    })
    if (this.props.namespaceFormType === 'add') {
      this.props.onInitNamespaceUrlValue(value)
    }
  }

  // 选择 instance 显示不同的 connection url
  onHandleChangeInstance = (e) => {
    const selUrl = this.state.currentNamespaceUrlValue.find(s => s.id === Number(e))
    this.props.form.setFieldsValue({
      connectionUrl: selUrl.connUrl
    })
    this.props.cleanNsTableData()
    this.setState({
      instanceIdGeted: selUrl.id
    }, () => {
      // 通过 instance id 显示 database 下拉框
      this.props.onInitDatabaseSelectValue(this.state.instanceIdGeted)
    })
  }

  onHandleChangeDatabase = (e) => {
    this.props.cleanNsTableData()
  }

  onHandleNsTableName = (e) => {
    this.props.onInitNsNameInputValue(e.target.value)
  }

  onHandleNsKey = (e) => {
    this.props.onInitNsKeyInputValue(e.target.value)
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { currentNamespaceUrlValue, namespaceDSValue } = this.state
    const { namespaceFormType, databaseSelectValue } = this.props
    const { namespaceTableSource, onDeleteTable, onAddTable, deleteTableClass, addTableClass, addTableClassTable, addBtnDisabled } = this.props

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 17 }
    }

    const DBDataSystemData = [
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
    if (namespaceFormType === 'add') {
      disabledOrNot = false
    } else if (namespaceFormType === 'edit') {
      disabledOrNot = true
    }

    const instanceOptions = currentNamespaceUrlValue.map(s => (<Option key={s.id} value={`${s.id}`}>{s.nsInstance}</Option>))
    const databaseOptions = databaseSelectValue.map((s) => (<Option key={s.id} value={`${s.id}`}>{`${s.nsDatabase} (${s.permission})`}</Option>))

    let namespaceDBLabel = ''
    let namespaceDBPlace = ''
    if (namespaceDSValue === 'es') {
      namespaceDBLabel = 'Index'
      namespaceDBPlace = 'select an Index'
    } else if (namespaceDSValue === 'hbase') {
      namespaceDBLabel = 'Namespace'
      namespaceDBPlace = 'select a Hbase Namespace'
    } else if (namespaceDSValue === 'kafka') {
      namespaceDBLabel = 'Topic'
      namespaceDBPlace = 'select a Topic'
    } else {
      namespaceDBLabel = 'Database'
      namespaceDBPlace = 'select a Database'
    }

    const namespaceTableLabel = namespaceDSValue === 'es' ? 'Types' : 'Tables'
    const namespaceTablePlace = namespaceDSValue === 'es' ? 'Type' : 'Table'
    const disabledKeyOrNot = namespaceDSValue === 'hbase'
    const namespaceKeyPlaceholder = namespaceDSValue === 'kafka' ? '多个数据主键用逗号隔开' : '多个业务主键用逗号隔开'

    const questionOrNot = namespaceDSValue === 'kafka'
      ? (
        <Tooltip title="帮助">
          <Popover
            placement="top"
            content={<div style={{ width: '400px', height: '38px' }}>
              <p>Kafka 时，Table 为 ums schema.namespace 中的第四层，如: ums schema.namespace 为 kafka.test.test1.test2.*.*.*， table 为 test2</p>
            </div>}
            title={<h3>帮助</h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>)
      : ''

    const namespaceTableMsg = (
      <span>
        {namespaceTableLabel}
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
          <Popconfirm placement="bottom" title="确定删除吗？" okText="Yes" cancelText="No" onConfirm={onDeleteTable(index)}>
            <Tooltip title="删除">
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
        this.setState({
          pageIndex: current
        })
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
            <FormItem label="Instance" {...itemStyle}>
              {getFieldDecorator('instance', {
                rules: [{
                  required: true,
                  message: '请填写 Instance'
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
              {getFieldDecorator('connectionUrl', {
                rules: [{
                  required: true,
                  message: '请选择 Connection URL'
                }]
              })(
                <Input placeholder="Connection URL" disabled />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label={namespaceDBLabel} {...itemStyle}>
              {getFieldDecorator('nsDatabase', {
                rules: [{
                  required: true,
                  message: `请选择 ${namespaceDBLabel}`
                }]
              })(
                <Select
                  dropdownClassName="ri-workbench-select-dropdown db-workbench-select-dropdown"
                  onChange={this.onHandleChangeDatabase}
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
                {getFieldDecorator('nsTables', {
                  // rules: [{
                  //   required: true,
                  //   message: '请填写 Tables'
                  // }]
                })(
                  <Input className="hide" />
                )}
              </FormItem>
            </Col>

            <Col span={7}>
              <FormItem label="" style={{ marginLeft: '2px' }}>
                {getFieldDecorator('nsSingleTableName', {})(
                  <Input
                    placeholder={namespaceTablePlace}
                    onChange={this.onHandleNsTableName}
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
                    onChange={this.onHandleNsKey}
                    disabled={disabledKeyOrNot}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={1} style={{width: '2.7%'}}></Col>
            <Col span={2} className={addTableClass}>
              <Tooltip title="添加">
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
  form: React.PropTypes.any,
  namespaceFormType: React.PropTypes.string,
  namespaceTableSource: React.PropTypes.array,
  databaseSelectValue: React.PropTypes.array,
  deleteTableClass: React.PropTypes.string,
  addTableClass: React.PropTypes.string,
  addTableClassTable: React.PropTypes.string,
  addBtnDisabled: React.PropTypes.bool,
  onInitNamespaceUrlValue: React.PropTypes.func,
  onInitDatabaseSelectValue: React.PropTypes.func,
  onDeleteTable: React.PropTypes.func,
  onAddTable: React.PropTypes.func,
  cleanNsTableData: React.PropTypes.func,
  onInitNsNameInputValue: React.PropTypes.func,
  onInitNsKeyInputValue: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(NamespaceForm)
