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

      tupleSizeValue: -1,
      umsopable: false,
      umsopKey: -1
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

  onChangeRowSelect = (record) => (e) => this.props.initChangeSelected(record)

  onChangeUmsType = (e) => this.props.initChangeUmsType(e.target.value)

  onChangeUmsId = (record) => (e) => this.props.initSelectUmsIdTs(record, 'ums_id_')

  onChangeUmsTs = (record) => (e) => this.props.initSelectUmsIdTs(record, 'ums_ts_')

  onRowSelectAll = () => this.props.initRowSelectedAll()

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
          message.error('最多有一个 Tuple！', 3)
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

  onChangeSizeValue = (value) => this.setState({ tupleSizeValue: value })

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

  editFieldType = () => this.setState({ tupleForm: 'edit' })

  handleChangeRename = (record) => (e) => {
    const val = e.target.value
    this.props.initEditRename(record.key, val)
  }

  onChangeUmsOp = (key) => (e) => {
    const { umsopKey } = this.state

    this.props.initSelectUmsop(key)

    if (umsopKey === -1) { // ums_op_不为空，回显后取消自己
      this.setState({
        umsopKey: key,
        umsopable: false
      })
    } else if (umsopKey === key) { // 选择自己和取消自己
      this.setState({
        umsopable: !this.state.umsopable
      })
    } else if (umsopKey !== key) { // 选择其他
      this.setState({
        umsopKey: key,
        umsopable: true
      })
    }
  }

  isDisabledLoad () {
    const { namespaceClassHide } = this.props

    let isDisabled = ''
    if (localStorage.getItem('loginRoleType') === 'admin') {
      isDisabled = namespaceClassHide === 'hide'
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      isDisabled = true
    }
    return isDisabled
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
      <div className="ums-select-class">
        <span className={this.isDisabledLoad() ? '' : 'ant-checkbox-wrapper'}>
          <span className={`ant-checkbox ${this.isDisabledLoad() ? 'ant-checkbox-disabled' : ''} ${finalClass}`}>
            <input
              type="checkbox"
              className="ant-checkbox-input"
              value="on"
              disabled={this.isDisabledLoad()}
              onChange={this.onRowSelectAll}
            />
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
            content={<div style={{ width: '450px', height: '185px' }}>
              <p className="schema-msg-class">{`1. **array 举例说明：字段 classid 为 intarray，数据格式：{"classid":[1,4,7,9]}；`}</p>
              <p className="schema-msg-class">{`2. jsonobject 举例说明：字段address为jsonobject类型，数据格式：{"province": "北京", "city": "北京", "area": "朝阳区"}；`}</p>
              <p className="schema-msg-class">{`3. jsonarray 举例说明：字段contracts为jsonarray类型，数据格式："contracts": [{"name": "Jack", "phone": "18012345423"}, {"name": "Tom", "phone": "18012346423"}]；`}</p>
              <p>{`4. tuple 举例说明：字段message为tuple类型，数据格式为： {"message": "2017-06-27 14:14:04,557|INFO"}，数据中有特殊字符作为分隔符，且需要分别处理分隔之后的字符串。`}</p>
            </div>}
            title={<h3>帮助</h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const umstsMsg = (
      <span>
        ums_ts_
        <Tooltip title="帮助">
          <Popover
            placement="top"
            content={<div style={{ width: '420px', height: '85px' }}>
              <p className="schema-msg-class">ums_ts_的类型限制: long，datetime，longarray，datetimearray。</p>
              <p>若为 datatime 或 datatimearray 类型，数据格式须为: yyyy/MM/dd HH:mm:ss[.SSS000/SSS] 或 yyyy-MM-dd HH:mm:ss[.SSS000/SSS] 或 yyyyMMddHHmmss[SSS000/SSS]。</p>
            </div>}
            title={<h3>帮助</h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const umsidMsg = (
      <span>
        ums_id_
        <Tooltip title="帮助">
          <Popover
            placement="top"
            content={<div style={{ width: '420px', height: '65px' }}>
              <p className="schema-msg-class">ums_id_ 的类型限制: int，long，intarray，longarray。</p>
              <p>若向 sink 端写数据时，只有插入操作，可不配置 ums_id_ 字段，若有增删改操作，必须配置。</p>
            </div>}
            title={<h3>帮助</h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const umsopMsg = (
      <span>
        ums_op_
        <Tooltip title="帮助">
          <Popover
            placement="top"
            content={<div style={{ width: '450px', height: '90px' }}>
              <p className="schema-msg-class">ums_op_ 的类型限制：int，long，string，intarray，longarray，stringarray。</p>
              <p className="schema-msg-class">配置 insert 操作，update 操作，delete 操作对应的值。</p>
              <p>若向 sink 端写数据时，只有插入操作，可不配置 ums_op_ 字段；若有增删改操作，必须配置。</p>
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
              <div className="table-ums-class ums-select-class">
                <span className={this.isDisabledLoad() ? '' : 'ant-checkbox-wrapper'}>
                  <span className={`ant-checkbox ${this.isDisabledLoad() ? 'ant-checkbox-disabled' : ''} ${record.selected ? 'ant-checkbox-checked' : ''}`}>
                    <input type="checkbox" className="ant-checkbox-input" value="on" disabled={this.isDisabledLoad()} />
                    <span className="ant-checkbox-inner"></span>
                  </span>
                </span>
              </div>
            )
            : (
              <div>
                <span className={this.isDisabledLoad() ? '' : 'ant-checkbox-wrapper'}>
                  <span className={`ant-checkbox ${this.isDisabledLoad() ? 'ant-checkbox-disabled' : ''} ${record.selected ? 'ant-checkbox-checked' : ''}`}>
                    <input
                      type="checkbox"
                      className="ant-checkbox-input"
                      value="on"
                      disabled={this.isDisabledLoad()}
                      onChange={this.onChangeRowSelect(record)}
                    />
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
      width: '20%'
    }, {
      title: 'Rename',
      dataIndex: 'rename',
      key: 'rename',
      width: '19%',
      render: (text, record) => {
        const { repeatRenameArr } = this.props
        const repeatKey = repeatRenameArr.length === 0 ? undefined : repeatRenameArr.find(i => i === record.key)

        return (
          <Row className={repeatKey === record.key ? 'rename-text-class' : ''}>
            <Input
              disabled={this.isDisabledLoad()}
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
        const { repeatArrayArr } = this.props

        const repeatKey = repeatArrayArr.length === 0 ? undefined : repeatArrayArr.find(i => i === record.key)

        let fieldTypeHtml = ''
        if (record.fieldType.indexOf('tuple') > -1) {
          const tupleVals = record.fieldType.split('##')

          const textHtml = (
            <div style={{ marginTop: '4px' }}>
              <Col span={10}><span >{`Sep: ${tupleVals[1]}`}</span></Col>
              <Col span={11}><span >{`Size: ${tupleVals[2]}`}</span></Col>
              <Col span={3}>
                <Icon
                  type="edit"
                  onClick={this.editFieldType}
                />
              </Col>
            </div>
          )

          const inputhtml = (
            <div style={{ marginTop: '4px' }}>
              <Input
                id="sep"
                defaultValue={tupleVals[1]}
                style={{ width: '40%' }}
                placeholder="Sep"
              />
              <InputNumber
                defaultValue={tupleVals[2]}
                style={{ width: '43%' }}
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
          <div className={repeatKey === record.key ? 'repeat-array-class' : ''}>
            <Select
              disabled={this.isDisabledLoad()}
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
      title: umstsMsg,
      dataIndex: 'umsTs',
      key: 'umsTs',
      width: '10%',
      className: 'text-align-center',
      render: (text, record) => {
        const tempHtml = (record.fieldType === 'long' || record.fieldType === 'datetime' ||
        record.fieldType === 'longarray' || record.fieldType === 'datetimearray')
          ? (
            <span className={this.isDisabledLoad() ? '' : 'ant-radio-wrapper'}>
              <span className={`ant-radio ${this.isDisabledLoad() ? 'ant-radio-disabled' : ''} ${record.ums_ts_ ? 'ant-radio-checked' : ''}`}>
                <input
                  type="radio"
                  className="ant-radio-input"
                  disabled={this.isDisabledLoad()}
                  onClick={this.onChangeUmsTs(record)}
                />
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
      title: umsidMsg,
      dataIndex: 'umsId',
      key: 'umsId',
      width: '10%',
      className: 'text-align-center',
      render: (text, record) => {
        const tempHtml = (record.fieldType === 'int' || record.fieldType === 'long' ||
        record.fieldType === 'intarray' || record.fieldType === 'longarray')
          ? (
            <span className={this.isDisabledLoad() ? '' : 'ant-checkbox-wrapper'}>
              <span className={`ant-checkbox ${this.isDisabledLoad() ? 'ant-checkbox-disabled' : ''} ${record.ums_id_ ? 'ant-checkbox-checked' : ''}`}>
                <input
                  type="checkbox"
                  className="ant-checkbox-input"
                  value="on"
                  disabled={this.isDisabledLoad()}
                  onChange={this.onChangeUmsId(record)}
                />
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
      title: umsopMsg,
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
            <span className={this.isDisabledLoad() ? '' : 'ant-checkbox-wrapper'}>
              <span className={`ant-checkbox ant-checkbox-checked ${this.isDisabledLoad() ? 'ant-checkbox-disabled' : ''}`}>
                <input
                  type="checkbox"
                  className="ant-checkbox-input"
                  value="on"
                  disabled={this.isDisabledLoad()}
                  onChange={this.onChangeUmsOp(record.key)}
                />
                <span className="ant-checkbox-inner"></span>
              </span>
            </span>
            <div style={{ marginTop: '4px' }}>
              <Input
                disabled={this.isDisabledLoad()}
                style={{ width: '33%' }}
                defaultValue={iVal || ''}
                id="insert"
                placeholder="Ins"
              />
              <Input
                disabled={this.isDisabledLoad()}
                style={{ width: '33%' }}
                defaultValue={uVal || ''}
                id="update"
                placeholder="Upd"
              />
              <Input
                disabled={this.isDisabledLoad()}
                style={{ width: '33%' }}
                defaultValue={dVal || ''}
                id="delete"
                placeholder="Del"
              />
            </div>
          </span>
        )

        const noSelectHtml = (
          <span className={this.isDisabledLoad() ? '' : 'ant-checkbox-wrapper'}>
            <span className={`ant-checkbox ${this.isDisabledLoad() ? 'ant-checkbox-disabled' : ''}`}>
              <input
                type="checkbox"
                className="ant-checkbox-input"
                value="on"
                disabled={this.isDisabledLoad()}
                onChange={this.onChangeUmsOp(record.key)}
              />
              <span className="ant-checkbox-inner"></span>
            </span>
          </span>
        )

        const { umsopKey, umsopable } = this.state

        let umsopHtml = ''
        if (record.fieldType === 'int' || record.fieldType === 'long' || record.fieldType === 'string' ||
          record.fieldType === 'intarray' || record.fieldType === 'longarray' || record.fieldType === 'stringarray') {
          if (umsopKey < 0) {
            umsopHtml = record.ums_op_ !== '' ? selectedHtml : noSelectHtml
          } else {
            if (record.key === umsopKey) {
              umsopHtml = umsopable ? selectedHtml : noSelectHtml
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
          <Col span={24} className="ums-type">
            <FormItem label="UMS Type" {...itemStyle}>
              {getFieldDecorator('umsType', {
                rules: [{
                  required: true,
                  message: '请选择 UMS Type'
                }]
              })(
                <RadioGroup className="radio-group-style" size="default" disabled={this.isDisabledLoad()} onChange={this.onChangeUmsType}>
                  <RadioButton value="ums" className="radio-btn-style radio-btn-extra">UMS</RadioButton>
                  <RadioButton value="ums_extension" className="ums-extension">UMS_Extension</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
        </Row>
        <Row className={umsTypeSeleted === 'ums_extension' ? '' : 'hide'}>
          <Col span={7} className="schema-json-title"><span>Source JSON Sample：</span></Col>
          <Col span={1}></Col>
          <Col span={16} className="schema-table-title">
            <span>Source Schema Table [注: 只能包含一个 array 类型 (intarray等或jsonarray)，且须将其或某子字段设置为主键之一]：
            </span></Col>
        </Row>
        <Row className={umsTypeSeleted === 'ums_extension' ? '' : 'hide'}>
          <Col span={7}>
            <textarea
              id="jsonSampleTextarea"
              placeholder="Paste your JSON Sample here."
            />
          </Col>
          <Col span={1} className="change-btn">
            <Button type="primary" onClick={this.props.onChangeJsonToTable} disabled={this.isDisabledLoad()}>
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
  namespaceClassHide: React.PropTypes.string,
  initRowSelectedAll: React.PropTypes.func,
  initSelectUmsop: React.PropTypes.func,
  repeatRenameArr: React.PropTypes.array,
  repeatArrayArr: React.PropTypes.array
}

export default Form.create({wrappedComponentRef: true})(SchemaTypeConfig)
