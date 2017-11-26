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

import { getAlterTypesByOriginType } from './umsFunction'

import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Radio from 'antd/lib/radio'
import Icon from 'antd/lib/icon'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button
const FormItem = Form.Item
import Select from 'antd/lib/select'
const Option = Select.Option

class EditableCell extends React.Component {
  state = {
    value: this.props.value, // eslint-disable-line
    editable: false,
    fieldTypeOptionsVal: []
  }

  onChangeFieldType = (value) => {
    this.setState({ value })
  }

  check = () => {
    this.setState({ editable: false })
  }

  edit = () => {
    const selectTypeValue = getAlterTypesByOriginType(this.state.value)
    this.setState({
      editable: true,
      fieldTypeOptionsVal: selectTypeValue
    })
  }

  render () {
    const { value, editable, fieldTypeOptionsVal } = this.state

    const fieldTypeOptions = fieldTypeOptionsVal.map(s => (<Option key={s} value={`${s}`}>{s}</Option>))

    return (
      <div className="editable-cell">
        {
          editable
            ? <div className="editable-cell-input-wrapper">
              {/* <Input
                value={value}
                onChange={this.onChangeFieldType}
                onPressEnter={this.check}
              /> */}
              <Select
              // dropdownClassName="ri-workbench-select-dropdown db-workbench-select-dropdown"
                onChange={this.onChangeFieldType}
                placeholder="Select"
                className="field-type-select"
                onPressEnter={this.check}
              >
                {fieldTypeOptions}
              </Select>
              <Icon
                type="check"
                className="editable-cell-icon-check"
                onClick={this.check}
              />
            </div>
            : <div className="editable-cell-text-wrapper">
              {value || ' '}
              <Icon
                type="edit"
                className="editable-cell-icon"
                onClick={this.edit}
              />
            </div>
        }
      </div>
    )
  }
}

export class SchemaTypeConfig extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      selectedRowKeys: []
    }
  }

  componentWillReceiveProps (props) {
    if (props.umsTableDataSource) {
      props.umsTableDataSource.map(s => {
        s.key = s.fieldName
        return s
      })
    }
  }

  onChangeUmsType = (e) => {
    this.props.initChangeUmsType(e.target.value)
  }

  onCellChange = (key, dataIndex) => {
    console.log(key, dataIndex)
    return (value) => {
      const dataSource = [...this.state.dataSource]
      const target = dataSource.find(item => item.key === key)
      if (target) {
        target[dataIndex] = value
        this.setState({ dataSource })
      }
    }
  }

  render () {
    const { form } = this.props
    const { getFieldDecorator } = form

    const itemStyle = {
      labelCol: { span: 2 },
      wrapperCol: { span: 22 }
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
      key: 'fieldType',
      render: (text, record) => (
        <EditableCell
          value={text}
          onChange={this.onCellChange(record.key, 'name')}
        />
      )
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
    }]

    const pagination = {
      defaultPageSize: 15,
      pageSizeOptions: ['15', '30', '45', '60'],
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
                  <RadioButton value="ums_extension" className="ums-extension">Ums_extension</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
        </Row>
        <Row className={umsTypeSeleted === 'ums' ? 'hide' : ''}>
          <Col span={7} className="code-mirror-content">
            <FormItem label="">
              {getFieldDecorator('jsonSample', {})(
                <textarea
                  placeholder="Paste your JSON Sample here."
                  className="ant-input ant-input-extra"
                  rows="5">
                </textarea>
              )}
            </FormItem>
          </Col>
          <Col span={1} className="change-btn">
            <Button type="primary" onClick={this.props.onChangeJsonToTable}>
              <Icon type="caret-right" />
            </Button>
          </Col>
          <Col span={16} className="schema-config-table">
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
