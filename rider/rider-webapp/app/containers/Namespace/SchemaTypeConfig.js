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

// import EditableCell from './EditableCell'
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
import Select from 'antd/lib/select'
const { Option, OptGroup } = Select
const RadioGroup = Radio.Group
const RadioButton = Radio.Button
const FormItem = Form.Item
import InputNumber from 'antd/lib/input-number'

export class SchemaTypeConfig extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      currentUmsTableData: [],
      editUmsOpable: false,
      editUmsOpKey: false || '',
      umsOPForm: '',
      tupleForm: '',
      currentKey: 0,
      delimiterValue: '',
      sizeValue: '',
      umsTsSelect: ''
    }
  }

  componentWillReceiveProps (props) {
    if (props.umsTableDataSource.length !== 0) {
      this.setState({
        currentUmsTableData: props.umsTableDataSource.filter(s => !s.forbidden)
      }, () => {
        // const temp = this.state.currentUmsTableData.find(i => i.fieldType.indexOf('##') > -1)
        // if (temp) {
        //   this.setState({
        //     tupleForm: 'text'
        //   })
        // }
      })
    }
  }

  onChangeRowSelect = (record) => (e) => {
    this.props.initChangeSelected(record)
  }

  onChangeUmsType = (e) => {
    this.props.initChangeUmsType(e.target.value)
  }

  onChangeUmsId = (record) => (e) => {
    console.log('rec', record)
    this.props.initSelectUmsIdTs(record, 'ums_id_')
  }

  onChangeUmsTs = (record) => (e) => {
    this.props.initSelectUmsIdTs(record, 'ums_ts_')
  }

  onChangeUmsOp = (record) => (e) => {
    const { currentUmsTableData } = this.state
    const tempData = currentUmsTableData.filter(i => i.forbidden === false)
    const umsOpExitNum = tempData.find(i => i.ums_op_ !== '')

    if (record.fieldType === 'string' || record.fieldType === 'long' || record.fieldType === 'int' || record.fieldType === 'array') {
      if (umsOpExitNum) {
        message.warning('UMS_OP_最多有一个！', 3)
      } else {
        this.setState({
          editUmsOpKey: record.key,
          umsOPForm: 'check'
        })
      }
    } else {
      message.warning('必须为 string/long/int/array 类型！', 3)
    }
  }

  initCheckUmsOp = (record) => (umsopValue) => {
    this.setState({
      umsOPForm: 'edit'
    })
    this.props.initCheckUmsOp(record, umsopValue)
  }

  editUmsOP = (record) => (e) => {
    this.setState({
      umsOPForm: ''
    })
    this.props.initCancelUmsOp(record)
  }

  onRowSelectAll = () => {
    this.props.initRowSelectedAll()
  }

  handleChangeType = (record) => (afterType) => {
    this.setState({
      delimiterValue: '',
      sizeValue: ''
    })

    const originType = record.fieldType
    const currentType = originType.indexOf('##') > -1 ? 'tuple' : originType

    let tupleTypeTemp = ''
    if ((currentType !== 'tuple' && afterType !== 'tuple') || (currentType === 'tuple' && afterType !== 'tuple')) {
      tupleTypeTemp = ''
      this.props.umsFieldTypeSelectOk(record.key, afterType)
    } else if ((currentType !== 'tuple' && afterType === 'tuple') || (currentType === 'tuple' && afterType === 'tuple')) {
      tupleTypeTemp = 'edit'
    }
    this.setState({
      tupleForm: tupleTypeTemp,
      currentKey: record.key
    })
  }

  handleChangeDelimiter = (e) => {
    this.setState({
      delimiterValue: e.target.value
    })
  }

  handleChangeSize = (value) => {
    const reg = /^[0-9]*$/
    if (reg.test(value)) {
      this.setState({
        sizeValue: value
      })
    } else {
      message.warning('Tuple 时，行数必须是数字！', 3)
    }
  }

  checkFieldType = () => {
    const { delimiterValue, sizeValue, currentKey } = this.state

    if (delimiterValue !== '' && sizeValue !== '') {
      const afterType = `tuple##${delimiterValue}##${sizeValue}`
      this.props.umsFieldTypeSelectOk(currentKey, afterType)
      this.setState({
        tupleForm: 'text'
      })
    } else {
      message.warning('Tuple 时，必须填写分隔符和行数！', 3)
    }
  }

  editFieldType = () => {
    this.setState({
      tupleForm: 'edit'
    })
  }

  handleChangeRename = (record) => (e) => {
    const val = e.target.value
    this.props.initEditRename(record.key, val)
  }

  render () {
    const { form } = this.props
    const { getFieldDecorator } = form
    // const { editUmsOpKey, umsOPForm } = this.state
    const { selectAllState, repeatRenameArr } = this.props

    const itemStyle = {
      labelCol: { span: 2 },
      wrapperCol: { span: 22 }
    }

    let finalClass = ''
    if (selectAllState === 'all') {
      finalClass = 'ant-checkbox-checked'
    } else if (selectAllState === 'not') {
      finalClass = ''
    } else if (selectAllState === 'part') {
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

    const columns = [{
      title: selectAll,
      dataIndex: 'selected',
      key: 'selected',
      width: '7%',
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
      width: '20%',
      render: (text, record) => <span className={record.forbidden ? 'type-text-class' : ''}>{text}</span>
    }, {
      title: 'Rename',
      dataIndex: 'rename',
      key: 'rename',
      width: '12%',
      render: (text, record) => {
        const repeatKey = repeatRenameArr.length === 0 ? undefined : repeatRenameArr.find(i => i === record.key)

        return (
          <Row className={repeatKey ? 'rename-text-class' : ''}>
            <Input
              value={record.rename}
              onChange={this.handleChangeRename(record)}
            />
          </Row>
        )
      }
    }, {
      title: 'FieldType',
      dataIndex: 'fieldType',
      key: 'fieldType',
      width: '25%',
      render: (text, record) => {
        const { currentKey, tupleForm, delimiterValue, sizeValue } = this.state

        const initType = record.fieldType.indexOf('##') > -1 ? 'tuple' : record.fieldType

        let delimiterTemp = ''
        let sizeTemp = ''
        // let inputClass = ''
        // let textClass = ''
        if (record.fieldType.indexOf('##') > -1) {
          const typeArrTemp = record.fieldType.split('##')
          delimiterTemp = typeArrTemp[1]
          sizeTemp = typeArrTemp[2]

          if (currentKey === record.key) {
            if (tupleForm === 'edit') {
              // inputClass = ''
              // textClass = 'hide'
            } else if (tupleForm === 'text') {
              // inputClass = 'hide'
              // textClass = ''
            } else {
              // inputClass = 'hide'
              // textClass = 'hide'
            }
          }
        } else {
          delimiterTemp = delimiterValue
          sizeTemp = sizeValue

          // inputClass = 'hide'
          // textClass = 'hide'
        }

        return (
          <div className="ums_field_type_class">
            <Select
              defaultValue={initType}
              onChange={this.handleChangeType(record)}
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
              <OptGroup label="Tuple Type">
                <Option value="tuple">tuple</Option>
              </OptGroup>
            </Select>
            <Row gutter={8} className={(currentKey === record.key && tupleForm === 'edit') ? '' : 'hide'}>
              <Col span={10}>
                <Input
                  value={delimiterValue}
                  onChange={this.handleChangeDelimiter}
                />
              </Col>
              <Col span={10}>
                <InputNumber
                  value={sizeValue}
                  onChange={this.handleChangeSize}
                />
              </Col>
              <Col span={4}>
                <Icon
                  type="check"
                  onClick={this.checkFieldType}
                />
              </Col>
            </Row>
            <Row gutter={8} className={(currentKey === record.key && tupleForm === 'text') ? '' : 'hide'}>
              <Col span={10}>
                <span>{`Delimiter: ${delimiterTemp}  `}</span>
              </Col>
              <Col span={10}>
                <span>{`Size: ${sizeTemp}`}</span>
              </Col>
              <Col span={4}>
                <Icon
                  type="edit"
                  onClick={this.editFieldType}
                />
              </Col>
            </Row>

            {/* <EditableCell
              typeFormVisible={this.state.typeFormVisible}
              initHideTypeModal={this.initHideTypeModal}
              initTypeModalOk={this.initTypeModalOk}
            /> */}
          </div>
        )
      }
    }, {
      title: 'ums_id_',
      dataIndex: 'umsId',
      key: 'umsId',
      width: '8.4%',
      className: 'text-align-center',
      render: (text, record) => {
        const tempHtml = (record.fieldType === 'long' || record.fieldType === 'datetime' ||
        record.fieldType === 'longarray' || record.fieldType === 'datetimearray' || record.fieldType === 'array')
          ? (
            <span className="ant-checkbox-wrapper">
              <span className={`ant-checkbox ${record.ums_id_ ? 'ant-checkbox-checked' : ''}`}>
                <input type="checkbox" className="ant-checkbox-input" value="on" onChange={this.onChangeUmsId(record)} />
                <span className="ant-checkbox-inner"></span>
              </span>
            </span>
          )
          : ''
        return (
          <div>
            {tempHtml}
          </div>
        )
      }
    }, {
      title: 'ums_ts_',
      dataIndex: 'umsTs',
      key: 'umsTs',
      width: '8.6%',
      className: 'text-align-center',
      render: (text, record) => {
        const tempHtml = (record.fieldType === 'string' || record.fieldType === 'long' ||
        record.fieldType === 'datetime' || record.fieldType === 'array')
          ? (
            <span className={`ant-radio-wrapper`}>
              <span className={`ant-radio ${record.ums_ts_ ? 'ant-radio-checked' : ''}`}>
                <input type="radio" className="ant-radio-input" onClick={this.onChangeUmsTs(record)} />
                <span className="ant-radio-inner"></span>
              </span>
            </span>
          )
          : ''

        return (
          <div>
            {tempHtml}
          </div>
        )
      }
    }, {
      title: 'ums_op_',
      dataIndex: 'umsOp',
      key: 'umsOp',
      className: 'text-align-center',
      render: (text, record) => {
        const umsopHtml = (record.fieldType === 'string' || record.fieldType === 'long' ||
        record.fieldType === 'int' || record.fieldType === 'array')
          ? (
            <span className="ant-checkbox-wrapper">
              <span className="ant-checkbox">
                <input type="checkbox" className="ant-checkbox-input" value="on" onChange={this.onChangeUmsOp(record)} />
                <span className="ant-checkbox-inner"></span>
              </span>
            </span>
          )
          : ''

        return (
          <div>
            {umsopHtml}
            <EditUmsOp
              ref={(f) => { this.editUmsOp = f }}
            />
          </div>
        )
      }
    }]

    const { umsTypeSeleted } = this.props

    return (
      <Form>
        <Row>
          <Col span={24}>
            <FormItem label="UMS Type" {...itemStyle}>
              {getFieldDecorator('umsType', {
                rules: [{
                  required: true,
                  message: '请选择 UMS Type'
                }]
                // initialValue: 'ums'
              })(
                <RadioGroup className="radio-group-style" size="default" onChange={this.onChangeUmsType}>
                  <RadioButton value="ums" className="radio-btn-style radio-btn-extra">UMS</RadioButton>
                  <RadioButton value="ums_extension" className="ums-extension">UMS_Extension</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
        </Row>
        <Row className={umsTypeSeleted === 'ums_extension' ? '' : 'hide'}>
          <Col span={7}>
            <textarea
              id="jsonSampleTextarea"
              placeholder="Paste your JSON Sample here."
            />
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
  initChangeSelected: React.PropTypes.func,
  initChangeUmsType: React.PropTypes.func,
  onChangeJsonToTable: React.PropTypes.func,
  umsFieldTypeSelectOk: React.PropTypes.func,
  initEditRename: React.PropTypes.func,
  initSelectUmsIdTs: React.PropTypes.func,
  initCheckUmsOp: React.PropTypes.func,
  initCancelUmsOp: React.PropTypes.func,
  // umsTableDataSource: React.PropTypes.array,
  repeatRenameArr: React.PropTypes.array,
  umsTypeSeleted: React.PropTypes.string,
  selectAllState: React.PropTypes.string,
  initRowSelectedAll: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(SchemaTypeConfig)
