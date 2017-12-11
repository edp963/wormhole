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
import Modal from 'antd/lib/modal'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button
const FormItem = Form.Item
import Select from 'antd/lib/select'
const { Option, OptGroup } = Select

export class SchemaTypeConfig extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      currentUmsTableData: [],
      tupleForm: '',
      currentKey: 0,
      delimiterValue: '',
      sizeValue: 0,
      umsTsSelect: '',
      umsopVisible: false,
      umsopRecord: '',
      umsopForm: '',
      tupleNum: 0
    }
  }

  componentWillReceiveProps (props) {
    if (props.umsTableDataSource.length !== 0) {
      this.setState({
        currentUmsTableData: props.umsTableDataSource.filter(s => !s.forbidden)
      }, () => {
        const temp = this.state.currentUmsTableData.find(i => i.fieldType.indexOf('##') > -1)
        if (temp) {
          this.setState({
            tupleForm: 'text',
            tupleNum: 1
          }, () => {
            const tupleArr = temp.fieldType.split('##')
            this.setState({
              delimiterValue: tupleArr[1],
              sizeValue: Number(tupleArr[2])
            })
          })
        } else {
          this.setState({
            tupleNum: 0
          })
        }
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
    this.props.initSelectUmsIdTs(record, 'ums_id_')
  }

  onChangeUmsTs = (record) => (e) => {
    this.props.initSelectUmsIdTs(record, 'ums_ts_')
  }

  onRowSelectAll = () => {
    this.props.initRowSelectedAll()
  }

  handleChangeType = (record) => (afterType) => {
    const originType = record.fieldType
    const currentType = originType.indexOf('##') > -1 ? 'tuple' : originType

    let tupleTypeTemp = ''
    if (currentType !== 'tuple' && afterType !== 'tuple') { // other to other
      tupleTypeTemp = ''
      this.props.umsFieldTypeSelectOk(record.key, afterType)
    } else if (currentType === 'tuple' && afterType !== 'tuple') {    // tuple to other
      tupleTypeTemp = ''
      this.props.umsFieldTypeSelectOk(record.key, afterType)
    } else if (currentType !== 'tuple' && afterType === 'tuple') {     // other to tuple
      if (this.state.tupleNum === 1) {
        message.warning('最多有一个 Tuple！', 3)
      } else {
        tupleTypeTemp = 'edit'
        this.setState({
          tupleNum: 1,
          currentKey: record.key
        })
      }
    }

    this.setState({
      tupleForm: tupleTypeTemp
    })
  }

  initcheckFieldType = (record, values) => {
    this.setState({
      tupleForm: 'text',
      delimiterValue: values.delimiterValue,
      sizeValue: values.sizeValue
    }, () => {
      this.props.initUmsopOther2Tuple(record, this.state.delimiterValue, this.state.sizeValue)
    })
  }

  initeditFieldType = () => {
    this.setState({
      tupleForm: 'edit'
    }, () => {
      this.editableCell.setFieldsValue({
        delimiterValue: this.state.delimiterValue,
        sizeValue: this.state.sizeValue
      })
    })
  }

  handleChangeRename = (record) => (e) => {
    const val = e.target.value
    this.props.initEditRename(record.key, val)
  }

  onChangeUmsOp = (record) => (e) => {
    this.setState({
      umsopVisible: true,
      umsopRecord: record
    })
  }

  onHandleUmsop = (record) => (e) => {
    this.setState({
      umsopVisible: true
    }, () => {
      const arr = record.split(',')
      const insertArr = arr[0].split(':')
      const updateArr = arr[1].split(':')
      const deleteArr = arr[2].split(':')
      this.editUmsOp.setFieldsValue({
        insert: insertArr[1],
        update: updateArr[1],
        delete: deleteArr[1]
      })
    })
  }

  hideUmsopForm = () => {
    this.setState({
      umsopVisible: false
    })
  }

  resetUmsopModal = () => {
    this.editUmsOp.resetFields()
  }

  onUmsopModalOk = () => {
    const { umsopRecord } = this.state

    this.editUmsOp.validateFieldsAndScroll((err, values) => { // eslint-disable-line
      if (values.insert && values.update && values.delete) {
        const textVal = `i:${values.insert},u:${values.update},d:${values.delete}`
        this.props.initCheckUmsOp(umsopRecord, 'ums_op_', textVal)
        this.hideUmsopForm()
      }
    })
  }

  render () {
    const { form } = this.props
    const { getFieldDecorator } = form
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
      width: '21%',
      render: (text, record) => <span className={record.forbidden ? 'type-text-class' : ''}>{text}</span>
    }, {
      title: 'Rename',
      dataIndex: 'rename',
      key: 'rename',
      width: '15%',
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
      width: '24%',
      render: (text, record) => {
        const { currentKey, tupleForm } = this.state

        let initType = ''
        if (currentKey === record.key && tupleForm === 'edit') {
          initType = 'tuple'
        } else if (record.fieldType.indexOf('##') > -1) {
          initType = 'tuple'
        } else {
          initType = record.fieldType
        }

        return (
          <div className="ums_field_type_class">
            <Select
              value={initType}
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

            { currentKey === record.key
              ? <EditableCell
                recordVal={record}
                tupleForm={this.state.tupleForm}
                currentKey={this.state.currentKey}
                delimiterValue={this.state.delimiterValue}
                sizeValue={this.state.sizeValue}
                initcheckFieldType={this.initcheckFieldType}
                initeditFieldType={this.initeditFieldType}
                ref={(f) => { this.editableCell = f }}
              />
              : ''
            }
          </div>
        )
      }
    }, {
      title: 'ums_id_',
      dataIndex: 'umsId',
      key: 'umsId',
      width: '8%',
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
      width: '8%',
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
        const { umsopVisible, umsopRecord } = this.state
        let umsopHtml = ''
        if (record.ums_op_ === '') {
          umsopHtml = (record.fieldType === 'string' || record.fieldType === 'long' ||
          record.fieldType === 'int' || record.fieldType === 'array')
            ? (
              <span className="ant-checkbox-wrapper">
                <span className={`ant-checkbox ${(umsopVisible && umsopRecord.key === record.key) ? 'ant-checkbox-checked' : ''}`}>
                  <input type="checkbox" className="ant-checkbox-input" value="on" onChange={this.onChangeUmsOp(record)} />
                  <span className="ant-checkbox-inner"></span>
                </span>
              </span>
            )
            : ''
        } else {
          umsopHtml = (
            <span>
              {record.ums_op_}
              <Icon
                type="edit"
                className="umsop-edit-icon-class"
                onClick={this.onHandleUmsop(record.ums_op_)}
              />
            </span>
          )
        }

        return (
          <div className="umsop-modal-class">
            {umsopHtml}
            <Modal
              title="ums_op_"
              okText="保存"
              visible={this.state.umsopVisible}
              onCancel={this.hideUmsopForm}
              afterClose={this.resetUmsopModal}
              mask={false}
              footer={[
                <Button
                  key="cancel"
                  size="large"
                  type="ghost"
                  onClick={this.hideUmsopForm}
                >
                  取消
                </Button>,
                <Button
                  key="submit"
                  size="large"
                  type="primary"
                  onClick={this.onUmsopModalOk}
                >
                  保存
                </Button>
              ]}
            >
              <EditUmsOp
                ref={(f) => { this.editUmsOp = f }}
              />
            </Modal>
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
  initUmsopOther2Tuple: React.PropTypes.func,
  initEditRename: React.PropTypes.func,
  initSelectUmsIdTs: React.PropTypes.func,
  initCheckUmsOp: React.PropTypes.func,
  // initCancelUmsOp: React.PropTypes.func,
  // umsTableDataSource: React.PropTypes.array,
  repeatRenameArr: React.PropTypes.array,
  umsTypeSeleted: React.PropTypes.string,
  selectAllState: React.PropTypes.string,
  initRowSelectedAll: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(SchemaTypeConfig)
