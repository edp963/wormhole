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
import Button from 'antd/lib/button'
import Icon from 'antd/lib/icon'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
import Select from 'antd/lib/select'
const { Option, OptGroup } = Select

export class SinkSchemaTypeConfig extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      currentSinkTableData: []
    }
  }

  componentWillReceiveProps (props) {
    this.setState({
      currentSinkTableData: props.sinkTableDataSource.length !== 0
        ? props.sinkTableDataSource.filter(s => !s.forbidden)
        : []
    })
  }

  onChangeRowSelect = (record) => (e) => this.props.initSinkChangeSelected(record)

  onRowSelectAll = () => this.props.initSinkRowSelectedAll()

  handleChangeFieldType = (record) => (afterType) => this.props.initChangeSinkType(record.key, afterType)

  render () {
    const { sinkSelectAllState } = this.props

    let finalClass = ''
    if (sinkSelectAllState === 'all') {
      finalClass = 'ant-checkbox-checked'
    } else if (sinkSelectAllState === 'not') {
      finalClass = ''
    } else if (sinkSelectAllState === 'part') {
      finalClass = 'ant-checkbox-indeterminate'
    }

    const selectAll = (
      <div>
        <span className="ant-checkbox-wrapper">
          <span className={`ant-checkbox ${finalClass}`}>
            <input type="checkbox" className="ant-checkbox-input" value="on" onChange={this.onRowSelectAll} />
            <span className="ant-checkbox-inner"></span>
          </span>
        </span>
      </div>
    )

    const fieldTypeMsg = (
      <span>
        FieldType
        <Tooltip title="帮助">
          <Popover
            placement="top"
            content={<div style={{ width: '450px', height: '125px' }}>
              <p className="schema-msg-class">{`1. **array 举例说明：字段 classid 为 intarray，数据格式：{"classid":[1,4,7,9]}；`}</p>
              <p className="schema-msg-class">{`2. jsonobject 举例说明：字段address为jsonobject类型，数据格式：{"province": "北京", "city": "北京", "area": "朝阳区"}；`}</p>
              <p className="schema-msg-class">{`3. jsonarray 举例说明：字段contracts为jsonarray类型，数据格式："contracts": [{"name": "Jack", "phone": "18012345423"}, {"name": "Tom", "phone": "18012346423"}]；`}</p>
            </div>}
            title={<h3>帮助</h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const columns = [{
      title: selectAll,
      dataIndex: 'selected',
      key: 'selected',
      width: '15%',
      className: 'text-align-center',
      render: (text, record) => (
        <div className="editable-cell">
          {record.forbidden
            ? (
              <div className="table-ums-class">
                <span className="ant-checkbox-wrapper">
                  <span className={`ant-checkbox ${record.selected ? 'ant-checkbox-checked' : ''}`}>
                    <input type="checkbox" className="ant-checkbox-input" value="on" />
                    <span className="ant-checkbox-inner"></span>
                  </span>
                </span>
              </div>
            )
            : (
              <div>
                <span className="ant-checkbox-wrapper">
                  <span className={`ant-checkbox ${record.selected ? 'ant-checkbox-checked' : ''}`}>
                    <input type="checkbox" className="ant-checkbox-input" value="on" onChange={this.onChangeRowSelect(record)} />
                    <span className="ant-checkbox-inner"></span>
                  </span>
                </span>
              </div>
            )
          }
        </div>
      )
    }, {
      title: 'FieldName',
      dataIndex: 'fieldName',
      key: 'fieldName',
      width: '50%'
    }, {
      title: fieldTypeMsg,
      dataIndex: 'fieldType',
      key: 'fieldType',
      render: (text, record) => (
        <div className="ums_field_type_class">
          <Select
            value={record.fieldType}
            onChange={this.handleChangeFieldType(record)}
          >
            <OptGroup label="Basic Type">
              <Option value="int">int</Option>
              <Option value="long">long</Option>
              <Option value="float">float</Option>
              <Option value="double">double</Option>
              <Option value="decimal">decimal</Option>
              <Option value="string">string</Option>
              <Option value="boolean">boolean</Option>
              <Option value="datetime">datetime</Option>
              <Option value="binary">binary</Option>
            </OptGroup>
            <OptGroup label="Array Type">
              <Option value="intarray">intarray</Option>
              <Option value="longarray">longarray</Option>
              <Option value="floatarray">floatarray</Option>
              <Option value="doublearray">doublearray</Option>
              <Option value="decimalarray">decimalarray</Option>
              <Option value="stringarray">stringarray</Option>
              <Option value="booleanarray">booleanarray</Option>
              <Option value="datetimearray">datetimearray</Option>
              <Option value="binaryarray">binaryarray</Option>
            </OptGroup>
            <OptGroup label="Object Type">
              <Option value="jsonobject">jsonobject</Option>
              <Option value="jsonarray">jsonarray</Option>
            </OptGroup>
          </Select>
        </div>
        )
    }]

    return (
      <Form>
        <Row>
          <Col span={15} className="schema-table-title"><span>Sink Schema Table：</span></Col>
          <Col span={2}></Col>
          <Col span={7} className="schema-json-title"><span>Sink JSON Sample：</span></Col>
        </Row>
        <Row>
          <Col span={15} className="schema-config-table">
            <Table
              dataSource={this.state.currentSinkTableData}
              columns={columns}
              pagination={false}
              scroll={{ y: 500 }}
              bordered
              className="tran-table-style"
            />
          </Col>
          <Col span={2} className="sink-change-btn">
            <Button type="primary" onClick={this.props.onChangeSinkJsonToTable}>
              <Icon type="caret-left" />反推
            </Button>
          </Col>
          <Col span={7}>
            <textarea
              id="sinkJsonSampleTextarea"
              placeholder="Paste your JSON Sample here."
            />
          </Col>
        </Row>
      </Form>
    )
  }
}

SinkSchemaTypeConfig.propTypes = {
  initSinkChangeSelected: React.PropTypes.func,
  onChangeSinkJsonToTable: React.PropTypes.func,
  sinkSelectAllState: React.PropTypes.string,
  initSinkRowSelectedAll: React.PropTypes.func,
  initChangeSinkType: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(SinkSchemaTypeConfig)
