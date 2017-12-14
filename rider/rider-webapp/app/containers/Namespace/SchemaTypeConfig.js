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
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Radio from 'antd/lib/radio'
import Icon from 'antd/lib/icon'
import message from 'antd/lib/message'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
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
      currentKey: -1,
      delimiterValue: '',
      sizeValue: 0,
      umsTsSelect: '',
      tupleNum: 0,
      selectTypeVal: '',

      insertValue: '',
      updateValue: '',
      deleteValue: '',

      tupleSizeValue: -1
    }
  }

  componentWillReceiveProps (props) {
    if (props.umsTableDataSource.length !== 0) {
      this.setState({
        currentUmsTableData: props.umsTableDataSource.filter(s => !s.forbidden),
        selectTypeVal: ''
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
    } else {
      this.setState({
        currentUmsTableData: []
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

  handleChangeFieldType = (record) => (afterType) => {
    const originType = record.fieldType
    const currentType = originType.indexOf('##') > -1 ? 'tuple' : originType

    if (this.state.tupleForm === 'edit') {
      message.error('Tuple 配置失败！', 3)
    } else {
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
          tupleTypeTemp = 'text'
        } else {
          const { currentUmsTableData } = this.state
          tupleTypeTemp = 'edit'
          this.setState({
            tupleNum: 1,
            currentKey: record.key
          }, () => {
            const tumsArr = currentUmsTableData.map(s => {
              let tempObject = {}
              tempObject = (this.state.currentKey === s.key)
                ? tempObject = {
                  fieldName: s.fieldName,
                  fieldType: 'tuple',
                  forbidden: s.forbidden,
                  key: s.key,
                  rename: s.rename,
                  selected: s.selected,
                  ums_id_: s.ums_id_,
                  ums_op_: s.ums_op_,
                  ums_ts_: s.ums_ts_
                }
                : s
              return tempObject
            })
            this.setState({
              currentUmsTableData: tumsArr
            })
          })
        }
      }

      this.setState({
        tupleForm: tupleTypeTemp,
        selectTypeVal: afterType
      })
    }
  }

  onChangeSizeValue = (value) => {
    this.setState({
      tupleSizeValue: value
    })
  }

  checkFieldType = (record) => (e) => {
    const sepTemp = document.getElementById('sep')

    if (sepTemp) {
      const sepValue = sepTemp.value
      const { tupleSizeValue } = this.state

      const reg = /^[0-9]*$/
      if (!sepValue) {
        message.error('请填写分隔符！', 3)
      } else if (!tupleSizeValue) {
        message.error('请填写长度！', 3)
      } else if (!reg.test(tupleSizeValue)) {
        message.error('长度应为数字！', 3)
      } else {
        this.setState({
          tupleForm: 'text'
        }, () => {
          this.props.initUmsopOther2Tuple(record, sepValue, tupleSizeValue)
        })
      }
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

  onChangeUmsOp = (key) => (e) => {
    const { currentUmsTableData } = this.state
    const umsopSelect = currentUmsTableData.find(s => s.ums_op_ !== '')
    if (umsopSelect) {
      if (key === umsopSelect.key) {
        // 自我取消
        this.props.initCancelUmsopSelf(key)
      } else {
        this.props.initSelectUmsop(key)
      }
    } else {
      this.props.initSelectUmsop(key)
    }
  }

  render () {
    const { form } = this.props
    const { getFieldDecorator } = form
    const { selectAllState } = this.props

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

    const fieldTypeMsg = (
      <span>
        FieldType
        <Tooltip title="帮助">
          <Popover
            placement="top"
            content={<div style={{ width: '411px', height: '30px' }}>
              <p>{`**array 举例说明：字段 classid 为 intarray，数据格式：{"classid":[1,4,7,9]}`}</p>
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
      width: '24%',
      render: (text, record) => {
        const { repeatRenameArr } = this.props
        const repeatKey = repeatRenameArr.length === 0 ? undefined : repeatRenameArr.find(i => i === record.key)

        return (
          <Row className={repeatKey === record.key ? 'rename-text-class' : ''}>
            <Input
              value={record.rename}
              onChange={this.handleChangeRename(record)}
            />
          </Row>
        )
      }
    }, {
      title: fieldTypeMsg,
      dataIndex: 'fieldType',
      key: 'fieldType',
      width: '17%',
      render: (text, record) => {
        const { currentKey, tupleForm } = this.state

        let fieldTypeHtml = ''
        if (record.fieldType.indexOf('tuple') > -1) {
          const tupleVals = record.fieldType.split('##')

          const textHtml = (
            <div>
              <Col span={9}><span >{`Sep: ${tupleVals[1]}`}</span></Col>
              <Col span={11}><span >{`Size: ${tupleVals[2]}`}</span></Col>
              <Col span={4}>
                <Icon
                  type="edit"
                  onClick={this.editFieldType}
                />
              </Col>
            </div>
          )

          const inputhtml = (
            <div>
              <Input
                id="sep"
                defaultValue={tupleVals[1]}
                style={{ width: '40%' }}
                placeholder="Sep"
              />
              <InputNumber
                defaultValue={tupleVals[2]}
                style={{ width: '40%' }}
                placeholder="Size"
                min={1}
                step={1}
                onChange={this.onChangeSizeValue}
              />
              <Icon
                type="check"
                onClick={this.checkFieldType(record)}
              />
            </div>
          )

          if (currentKey < 0) {
            if (tupleForm === 'text') {
              fieldTypeHtml = textHtml
            } else if (tupleForm === 'edit') {
              fieldTypeHtml = inputhtml
            }
          } else {
            if (currentKey === record.key) {
              if (tupleForm === 'text') {
                fieldTypeHtml = textHtml
              } else if (tupleForm === 'edit') {
                fieldTypeHtml = inputhtml
              }
            } else {
              console.log('')
            }
          }
        } else {
          fieldTypeHtml = ''
        }

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
              <OptGroup label="Tuple Type">
                <Option value="tuple">tuple</Option>
              </OptGroup>
            </Select>

            {fieldTypeHtml}
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
        const tempHtml = (record.fieldType === 'int' || record.fieldType === 'long' ||
        record.fieldType === 'intarray' || record.fieldType === 'longarray')
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
        const tempHtml = (record.fieldType === 'long' || record.fieldType === 'datetime' ||
        record.fieldType === 'longarray' || record.fieldType === 'datetimearray')
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
        let iVal = ''
        let uVal = ''
        let dVal = ''
        if (record.ums_op_ !== '') {
          const arr = record.ums_op_.split(',')
          const insertArr = arr[0].split(':')
          const updateArr = arr[1].split(':')
          const deleteArr = arr[2].split(':')
          iVal = insertArr[1]
          uVal = updateArr[1]
          dVal = deleteArr[1]
        } else {
          iVal = ''
          uVal = ''
          dVal = ''
        }

        const selectedHtml = (
          <span>
            <span className="ant-checkbox-wrapper">
              <span className="ant-checkbox ant-checkbox-checked">
                <input type="checkbox" className="ant-checkbox-input" value="on" onChange={this.onChangeUmsOp(record.key)} />
                <span className="ant-checkbox-inner"></span>
              </span>
            </span>
            <div>
              <Input
                style={{ width: '33%' }}
                defaultValue={iVal || ''}
                id="insert"
                placeholder="Insert"
              />
              <Input
                style={{ width: '33%' }}
                defaultValue={uVal || ''}
                id="update"
                placeholder="Update"
              />
              <Input
                style={{ width: '33%' }}
                defaultValue={dVal || ''}
                id="delete"
                placeholder="Delete"
              />
            </div>
          </span>
        )

        const noSelectHtml = (
          <span className="ant-checkbox-wrapper">
            <span className="ant-checkbox">
              <input type="checkbox" className="ant-checkbox-input" value="on" onChange={this.onChangeUmsOp(record.key)} />
              <span className="ant-checkbox-inner"></span>
            </span>
          </span>
        )

        const { umsopRecordValue } = this.props

        let umsopHtml = ''
        if (record.fieldType === 'int' || record.fieldType === 'long' || record.fieldType === 'string' ||
          record.fieldType === 'intarray' || record.fieldType === 'longarray' || record.fieldType === 'stringarray') {
          if (umsopRecordValue < 0) {
            if (record.ums_op_ !== '') {
              umsopHtml = selectedHtml
            } else {
              umsopHtml = noSelectHtml
            }
          } else {
            if (record.key === umsopRecordValue) {
              umsopHtml = selectedHtml
            } else {
              umsopHtml = noSelectHtml
            }
          }
        } else {
          umsopHtml = ''
        }

        return (
          <div className="umsop-modal-class">
            {umsopHtml}
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
  umsTypeSeleted: React.PropTypes.string,
  selectAllState: React.PropTypes.string,
  initRowSelectedAll: React.PropTypes.func,
  initSelectUmsop: React.PropTypes.func,
  initCancelUmsopSelf: React.PropTypes.func,
  umsopRecordValue: React.PropTypes.number,
  repeatRenameArr: React.PropTypes.array
}

export default Form.create({wrappedComponentRef: true})(SchemaTypeConfig)
