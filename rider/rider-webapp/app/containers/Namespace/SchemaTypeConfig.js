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

import EditableCell from './EditableCell'
import EditUmsOp from './EditUmsOp'

import Form from 'antd/lib/form'
import Input from 'antd/lib/input'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Radio from 'antd/lib/radio'
import Icon from 'antd/lib/icon'
import message from 'antd/lib/message'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button
const FormItem = Form.Item

export class SchemaTypeConfig extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      selectedRowKeys: [],
      currentUmsTableData: [],
      currentFieldName: '',
      renameEditable: false,
      renameEditVal: '',
      editUmsOpable: false,
      editUmsOpKey: false || '',

      searchFieldName: '',
      filterDropdownVisibleFieldName: false,
      searchReName: '',
      filterDropdownVisibleReName: false
    }
  }

  componentWillReceiveProps (props) {
    if (props.umsTableDataSource) {
      this.setState({
        currentUmsTableData: props.umsTableDataSource
      }, () => {
        const selectRowArr = this.state.currentUmsTableData.filter(s => s.selected === true)
        this.setState({
          selectedRowKeys: [...selectRowArr.keys()]
        })
      })
    }
  }

  onInputChange = (value) => (e) => this.setState({ [value]: e.target.value })

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      [visible]: false,
      currentUmsTableData: this.props.umsTableDataSource.map((record) => {
        const match = String(record[columnName]).match(reg)
        if (!match) {
          return null
        }
        return {
          ...record,
          [`${columnName}Origin`]: record[columnName],
          [columnName]: (
            <span>
              {String(record[columnName]).split(reg).map((text, i) => (
                i > 0 ? [<span className="highlight">{match[0]}</span>, text] : text
              ))}
            </span>
          )
        }
      }).filter(record => !!record)
    })
  }

  onSelectChange = (selectedRowKeys) => {
    console.log('selectedRowKeys', selectedRowKeys)

    this.setState({ selectedRowKeys })
  }

  onChangeUmsType = (e) => {
    this.props.initChangeUmsType(e.target.value)
  }

  onCellChange = (key, dataIndex) => {
    // console.log('key', key)

    // return (value) => {
    //   console.log('va', value)
    //   const dataSource = [...this.state.dataSource]
    //   const target = dataSource.find(item => item.key === key)
    //   if (target) {
    //     target[dataIndex] = value
    //     this.setState({ dataSource })
    //   }
    // }
  }

  initChangeType = (fieldName) => (va1, va2) => {
    this.props.initChangeType(fieldName, va1, va2)
  }

  editRename = () => {
    this.setState({
      renameEditable: true,
      renameEditVal: ''
    })
  }

  handleChangeRename = (e) => {
    this.setState({
      renameEditVal: e.target.value
    })
  }

  checkRename = (record) => (e) => {
    const { renameEditVal } = this.state

    if (renameEditVal === '') {
      message.warning('请填写 Rename！', 3)
    } else {
      this.setState({
        renameEditable: false
      })
      this.props.initEditRename(record, renameEditVal)  // 组成新数组
    }
  }

  onChangeUmsId = (record) => (e) => {
    const { currentUmsTableData } = this.state

    if (record.fieldType === 'string' || record.fieldType === 'long' || record.fieldType === 'datetime') {
      const tempData = currentUmsTableData.filter(i => i.forbidden === false) // 过滤掉 forbidden === true 的项
      const umsIdExitNum = tempData.find(i => i.ums_id_ === true)

      if (umsIdExitNum) {
        record.ums_id_
          ? this.props.cancelSelectUmsId(record, 'ums_id_')
          : message.warning('UMS_ID_有且只能有一个！', 3)
      } else {
        this.props.initSelectUmsId(record, 'ums_id_')
      }
    } else {
      message.warning('必须为 string/long/datetime 类型！', 3)
    }
  }

  onChangeUmsTs = (record) => (e) => {
    const { currentUmsTableData } = this.state
    const tempData = currentUmsTableData.filter(i => i.forbidden === false)
    const umsTsExitNum = tempData.find(i => i.ums_ts_ === true)
    if (umsTsExitNum) {
      record.ums_ts_
        ? this.props.cancelSelectUmsId(record, 'ums_ts_')
        : message.warning('UMS_TS_有且只能有一个！', 3)
    } else {
      this.props.initSelectUmsId(record, 'ums_ts_')
    }
  }

  onChangeUmsOp = (record) => (e) => {
    this.setState({
      editUmsOpKey: record.key
    })

    // record.ums_op_ !== ''
    //   ? this.props.cancelSelectUmsId(record, 'ums_op_')
    //   : this.props.initSelectUmsId(record, 'ums_op_')
  }

  initCheckUmsOp = (record) => (umsopValue) => {
    this.props.initCheckUmsOp(record, umsopValue)
    // this.props.initSelectUmsId(record, umsopValue)
  }

  // editUmsOP = (record) => (e) =>{
  //   console.log('re', record)
  //   this.props.initCancelUmsOp(record)
  // }

  render () {
    const { form } = this.props
    const { getFieldDecorator } = form
    const { renameEditable, editUmsOpKey } = this.state

    const itemStyle = {
      labelCol: { span: 2 },
      wrapperCol: { span: 22 }
    }

    let { filteredInfo } = this.state
    filteredInfo = filteredInfo || {}

    const columns = [{
      title: 'FieldName',
      dataIndex: 'fieldName',
      key: 'fieldName',
      width: '25%',
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="FieldName"
            value={this.state.searchFieldName}
            onChange={this.onInputChange('searchFieldName')}
            onPressEnter={this.onSearch('fieldName', 'searchFieldName', 'filterDropdownVisibleFieldName')}
          />
          <Button type="primary" onClick={this.onSearch('fieldName', 'searchFieldName', 'filterDropdownVisibleFieldName')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleFieldName,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleFieldName: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Rename',
      dataIndex: 'rename',
      key: 'rename',
      width: '15%',
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Rename"
            value={this.state.searchRename}
            onChange={this.onInputChange('searchRename')}
            onPressEnter={this.onSearch('rename', 'searchRename', 'filterDropdownVisibleReName')}
          />
          <Button type="primary" onClick={this.onSearch('rename', 'searchRename', 'filterDropdownVisibleReName')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleReName,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleReName: visible
      }, () => this.searchInput.focus()),
      render: (text, record) => {
        const temp1 = (text === '' && renameEditable === true) ? '' : 'hide'
        const temp2 = (text === '' && renameEditable === false) ? '' : 'hide'

        return (
          <div className="editable-cell">
            {
              <div className="editable-rename-cell-text-wrapper">
                {text || ' '}
                <Input
                  // value={value}
                  className={temp1}
                  onChange={this.handleChangeRename}
                  onPressEnter={this.checkRename(record)}
                />
                <Icon
                  type="check"
                  className={`editable-cell-icon-check ${temp1}`}
                  onClick={this.checkRename(record)}
                 />
                <Icon
                  type="edit"
                  className={`editable-cell-icon ${temp2}`}
                  onClick={this.editRename}
                />
              </div>
            }
          </div>
        )
      }
    }, {
      title: 'FieldType',
      dataIndex: 'fieldType',
      key: 'fieldType',
      width: '17%',
      filters: [
        {text: 'string', value: 'string'},
        {text: 'int', value: 'int'},
        {text: 'long', value: 'long'},
        {text: 'float', value: 'float'},
        {text: 'double', value: 'double'},
        {text: 'boolean', value: 'boolean'},
        {text: 'decimal', value: 'decimal'},
        {text: 'binary', value: 'binary'},
        {text: 'datetime', value: 'datetime'},

        {text: 'stringarray', value: 'stringarray'},
        {text: 'intarray', value: 'intarray'},
        {text: 'longarray', value: 'longarray'},
        {text: 'floatarray', value: 'floatarray'},
        {text: 'doublearray', value: 'doublearray'},
        {text: 'booleanarray', value: 'booleanarray'},
        {text: 'decimalarray', value: 'decimalarray'},
        {text: 'binaryarray', value: 'binaryarray'},
        {text: 'datetimearray', value: 'datetimearray'},

        {text: 'jsonobject', value: 'jsonobject'},
        {text: 'jsonarray', value: 'jsonarray'},
        {text: 'tuple', value: 'tuple'}
      ],
      filteredValue: filteredInfo.fieldType,
      onFilter: (value, record) => record.fieldType.includes(value),
      render: (text, record) => (
        <EditableCell
          value={text}
          // onChange={this.onCellChange(record.fieldName, 'name')}
          initChangeTypeOption={this.initChangeType(record.fieldName)}
          tableDatas={this.state.currentUmsTableData}
        />
        )
    }, {
      title: 'UMS_ID_',
      dataIndex: 'umsId',
      key: 'umsId',
      width: '8%',
      className: 'text-align-center',
      render: (text, record) => (
        <div className="editable-cell">
          {
            <div className="editable-rename-cell-text-wrapper">
              <span className="ant-checkbox-wrapper">
                <span className={`ant-checkbox ${record.ums_id_ === true ? 'ant-checkbox-checked' : ''}`}>
                  <input type="checkbox" className="ant-checkbox-input" value="on" onChange={this.onChangeUmsId(record)} />
                  <span className="ant-checkbox-inner"></span>
                </span>
              </span>
            </div>
          }
        </div>
      )
    }, {
      title: 'UMS_TS_',
      dataIndex: 'umsTs',
      key: 'umsTs',
      width: '8%',
      className: 'text-align-center',
      render: (text, record) => (
        <div className="editable-cell">
          {
            <div className="editable-rename-cell-text-wrapper">
              <span className="ant-checkbox-wrapper">
                <span className={`ant-checkbox ${record.ums_ts_ === true ? 'ant-checkbox-checked' : ''}`}>
                  <input type="checkbox" className="ant-checkbox-input" value="on" onChange={this.onChangeUmsTs(record)} />
                  <span className="ant-checkbox-inner"></span>
                </span>
              </span>
            </div>
          }
        </div>
      )
    }, {
      title: 'UMS_OP_',
      dataIndex: 'umsOp',
      key: 'umsOp',
      className: 'text-align-center',
      render: (text, record) => {
        let umsopHtml = ''
        if (record.key === editUmsOpKey) {
          umsopHtml = (
            <EditUmsOp
              initCheckUmsOp={this.initCheckUmsOp(record)}
              ref={(f) => { this.editUmsOp = f }}
            />
          )
        } else {
          if (record.ums_op_ !== '') {
            umsopHtml = record.ums_op_
          } else {
            umsopHtml = (
              <div className="editable-umsop-cell-text-wrapper">
                <span className="ant-checkbox-wrapper">
                  <span className="ant-checkbox">
                    <input type="checkbox" className="ant-checkbox-input" value="on" onChange={this.onChangeUmsOp(record)} />
                    <span className="ant-checkbox-inner"></span>
                  </span>
                </span>
              </div>
            )
          }
        }

        return (
          <div>
            {umsopHtml}
            {/* <Icon
              type="edit"
              className={`editable-cell-icon`}
              onClick={this.editUmsOP(record)}
            /> */}
          </div>
        )
      }
    }]

    const { selectedRowKeys } = this.state
    const { umsTypeSeleted } = this.props

    const rowSelection = {
      selectedRowKeys,
      onChange: this.onSelectChange,
      onShowSizeChange: this.onShowSizeChange
      // selections: [{}]
    }

    return (
      <Form>
        <Row>
          <Col span={24}>
            <FormItem label="UMS Type" {...itemStyle}>
              {getFieldDecorator('umsType', {
                rules: [{
                  required: true,
                  message: '请选择 UMS Type'
                }],
                initialValue: 'ums'
              })(
                <RadioGroup className="radio-group-style" size="default" onChange={this.onChangeUmsType}>
                  <RadioButton value="ums" className="radio-btn-style radio-btn-extra">UMS</RadioButton>
                  <RadioButton value="ums_extension" className="ums-extension">UMS_Extension</RadioButton>
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
              dataSource={this.state.currentUmsTableData}
              columns={columns}
              pagination={false}
              scroll={{ y: 500 }}
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
  initChangeType: React.PropTypes.func,
  initEditRename: React.PropTypes.func,
  initSelectUmsId: React.PropTypes.func,
  cancelSelectUmsId: React.PropTypes.func,
  initCheckUmsOp: React.PropTypes.func,
  // initCancelUmsOp: React.PropTypes.func,
  umsTableDataSource: React.PropTypes.array,
  umsTypeSeleted: React.PropTypes.string
}

export default Form.create({wrappedComponentRef: true})(SchemaTypeConfig)
