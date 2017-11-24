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

import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Table from 'antd/lib/table'
import Popconfirm from 'antd/lib/popconfirm'
import Tooltip from 'antd/lib/tooltip'
import Button from 'antd/lib/button'
import Radio from 'antd/lib/radio'
import Icon from 'antd/lib/icon'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button
const FormItem = Form.Item

export class SchemaTypeConfig extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      selectedRowKeys: []
    }
  }

  onChangeUmsType = (e) => {
    this.props.initChangeUmsType(e.target.value)
  }

  render () {
    const { form } = this.props
    const { getFieldDecorator } = form

    const itemStyle = {
      labelCol: { span: 4 },
      wrapperCol: { span: 19 }
    }

    const columns = [{
      title: 'FieldName',
      dataIndex: 'fieldName',
      key: 'fieldName'
    }, {
      title: 'Rename',
      dataIndex: 'rename',
      key: 'rename'
    }, {
      title: 'FieldType',
      dataIndex: 'fieldType',
      key: 'fieldType'
    }, {
      title: 'ums_id_',
      dataIndex: 'umsId',
      key: 'umsId'
    }, {
      title: 'ums_ts_',
      dataIndex: 'umsTs',
      key: 'umsTs'
    }, {
      title: 'ums_op_',
      dataIndex: 'umsOp',
      key: 'umsOp'
    }, {
      title: 'Action',
      key: 'action',
      render: (text, record) => (
        <span className="ant-table-action-column">
          <Tooltip title="编辑">
            <Button icon="edit" shape="circle" type="ghost"></Button>
          </Tooltip>

          <Popconfirm placement="bottom" title="确定删除吗？" okText="Yes" cancelText="No">
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
      defaultPageSize: 10,
      pageSizeOptions: ['10', '20', '30', '40'],
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

    const { selectedRowKeys } = this.state
    const { umsTypeSeleted } = this.props

    const rowSelection = {
      selectedRowKeys,
      onChange: this.onSelectChange,
      onShowSizeChange: this.onShowSizeChange
    }

    return (
      <Form>
        <Row>
          <Col span={24}>
            <FormItem label="Ums Type" {...itemStyle}>
              {getFieldDecorator('umsType', {
                rules: [{
                  required: true,
                  message: '请选择 Ums Type'
                }],
                initialValue: 'ums'
              })(
                <RadioGroup className="radio-group-style" size="default" onChange={this.onChangeUmsType}>
                  <RadioButton value="ums" className="radio-btn-style radio-btn-extra">Ums</RadioButton>
                  <RadioButton value="ums_extension" className="ums-extension radio-btn-extra">Ums_extension</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
        </Row>
        <Row className={umsTypeSeleted === 'ums' ? 'hide' : ''}>
          <Col span={8}>
            <FormItem label="" {...itemStyle}>
              {getFieldDecorator('jsonSample', {})(
                <textarea
                  placeholder="Paste your Sink Config JSON here."
                  className="ant-input ant-input-extra"
                  rows="5">
                </textarea>
              )}
            </FormItem>
          </Col>
          <Col span={1}>
            <Button type="primary" onClick={this.props.onChangeJsonToTable}>
              <Icon type="caret-right" />
            </Button>
          </Col>
          <Col span={15} className="schema-config-table">
            <Table
              dataSource={this.props.umsTableDataSource}
              columns={columns}
              pagination={pagination}
              rowSelection={rowSelection}
              bordered
              className="tran-table-style"
            />
          </Col>
        </Row>
      </Form>
    )
  }
}

SchemaTypeConfig.propTypes = {
  form: React.PropTypes.any,
  initChangeUmsType: React.PropTypes.func,
  onChangeJsonToTable: React.PropTypes.func,
  umsTableDataSource: React.PropTypes.array,
  umsTypeSeleted: React.PropTypes.string
}

export default Form.create({wrappedComponentRef: true})(SchemaTypeConfig)
