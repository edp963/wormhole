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

import { selectRoleType } from '../App/selectors'
import { selectLocale } from '../LanguageProvider/selectors'

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
        const temp = this.state.currentUmsTableData.find(i => i.fieldType.includes('##'))
        if (temp) {
          this.setState({
            tupleForm: 'text'
          }, () => {
            const tupleArr = temp.fieldType.split('##')
            this.setState({
              delimiterValue: tupleArr[1],
              sizeValue: Number(tupleArr[2])
            })
          })
        }
      })
    } else {
      this.setState({ currentUmsTableData: [] })
    }
    this.setState({umsopable: props.umsopable, umsopKey: props.umsopKey})
  }

  handleChangeFieldType = (record) => (afterType) => {
    const { locale } = this.props

    const originType = record.fieldType
    const currentType = originType.includes('##') ? 'tuple' : originType

    if (this.state.tupleForm === 'edit') {
      message.error(locale === 'en' ? 'Tuple configuration has error!' : 'Tuple 配置失败！', 3)
    } else {
      let tupleTypeTemp = ''
      if (currentType !== 'tuple' && afterType !== 'tuple') { // other to other
        tupleTypeTemp = ''
        this.props.umsFieldTypeSelectOk(record.key, afterType)
      } else if (currentType === 'tuple' && afterType !== 'tuple') {    // tuple to other
        tupleTypeTemp = ''
        this.props.umsFieldTypeSelectOk(record.key, afterType)
      } else if (currentType !== 'tuple' && afterType === 'tuple') {     // other to tuple
        const { currentUmsTableData } = this.state
        tupleTypeTemp = 'edit'
        this.setState({
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
          this.setState({ currentUmsTableData: tumsArr })
        })
      }

      this.setState({
        tupleForm: tupleTypeTemp,
        selectTypeVal: afterType
      })
    }
  }

  onChangeSizeValue = (value) => this.setState({ tupleSizeValue: value })

  checkFieldType = (record) => (e) => {
    const { locale } = this.props
    const sepTemp = document.getElementById('sep')

    if (sepTemp) {
      const sepValue = sepTemp.value
      const { tupleSizeValue } = this.state

      const reg = /^[0-9]*$/
      if (!sepValue) {
        message.error(locale === 'en' ? 'Please fill in separator!' : '请填写分隔符！', 3)
      } else if (!tupleSizeValue) {
        message.error(locale === 'en' ? 'Please fill in length!' : '请填写长度！', 3)
      } else if (!reg.test(tupleSizeValue)) {
        message.error(locale === 'en' ? 'Length should be figures!' : '长度应为数字！', 3)
      } else {
        this.setState({
          tupleForm: 'text'
        }, () => {
          this.props.initUmsopOther2Tuple(record, sepValue, tupleSizeValue)
        })
      }
    }
  }

  editFieldType = (record) => (e) => {
    const typeArr = record.fieldType.split('##')
    this.setState({
      tupleForm: 'edit',
      currentKey: record.key,
      tupleSizeValue: typeArr[2]
    })
  }

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
      }, () => {
        this.props.emitUmSopable(this.state.umsopable)
      })
    } else if (umsopKey === key) { // 选择自己和取消自己
      this.setState({
        umsopable: !this.state.umsopable
      }, () => {
        this.props.emitUmSopable(this.state.umsopable)
      })
    } else if (umsopKey !== key) { // 选择其他
      this.setState({
        umsopKey: key,
        umsopable: true
      }, () => {
        this.props.emitUmSopable(this.state.umsopable, this.state.umsopKey)
      })
    }
  }

  isDisabledLoad () {
    const { namespaceClassHide, roleType } = this.props

    let isDisabled = ''
    if (roleType === 'admin') {
      isDisabled = namespaceClassHide === 'hide'
    } else if (roleType === 'user') {
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
              onChange={() => this.props.initRowSelectedAll()}
            />
            <span className="ant-checkbox-inner"></span>
          </span>
        </span>
      </div>
    )

    const fieldTypeMsg = (
      <span>
        FieldType
        <Tooltip title={<FormattedMessage {...messages.nsHelp} />}>
          <Popover
            placement="top"
            content={
              <div style={{ width: '450px', height: '185px' }}>
                <p className="schema-msg-class"><FormattedMessage {...messages.nsShemaFieldTypeMsg1} /></p>
                <p className="schema-msg-class"><FormattedMessage {...messages.nsShemaFieldTypeMsg2} /></p>
                <p className="schema-msg-class"><FormattedMessage {...messages.nsShemaFieldTypeMsg3} /></p>
                <p><FormattedMessage {...messages.nsShemaFieldTypeMsg4} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.nsHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const umstsMsg = (
      <span>
        ums_ts_
        <Tooltip title={<FormattedMessage {...messages.nsHelp} />}>
          <Popover
            placement="top"
            content={
              <div style={{ width: '420px', height: '85px' }}>
                <p className="schema-msg-class"><FormattedMessage {...messages.nsShemaUmsTsMsg1} /></p>
                <p><FormattedMessage {...messages.nsShemaUmsTsMsg2} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.nsHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const umsidMsg = (
      <span>
        ums_id_
        <Tooltip title={<FormattedMessage {...messages.nsHelp} />}>
          <Popover
            placement="top"
            content={
              <div style={{ width: '420px', height: '65px' }}>
                <p className="schema-msg-class"><FormattedMessage {...messages.nsShemaUmsIdMsg1} /></p>
                <p><FormattedMessage {...messages.nsShemaUmsIdMsg2} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.nsHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const umsopMsg = (
      <span>
        ums_op_
        <Tooltip title={<FormattedMessage {...messages.nsHelp} />}>
          <Popover
            placement="top"
            content={
              <div style={{ width: '450px', height: '90px' }}>
                <p className="schema-msg-class"><FormattedMessage {...messages.nsShemaUmsOpMsg1} /></p>
                <p className="schema-msg-class"><FormattedMessage {...messages.nsShemaUmsOpMsg2} /></p>
                <p><FormattedMessage {...messages.nsShemaUmsOpMsg3} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.nsHelp} /></h3>}
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
                      onChange={() => this.props.initChangeSelected(record)}
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
        if (record.fieldType.includes('tuple')) {
          const tupleVals = record.fieldType.split('##')

          const textHtml = (
            <div style={{ marginTop: '4px' }}>
              <Col span={10}><span >{`Sep: ${tupleVals[1]}`}</span></Col>
              <Col span={11}><span >{`Size: ${tupleVals[2]}`}</span></Col>
              <Col span={3} className={this.isDisabledLoad() ? 'hide' : ''}>
                <Icon
                  type="edit"
                  onClick={this.editFieldType(record)}
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

          if (record.fieldType === 'tuple') {
            fieldTypeHtml = inputhtml
          } else {
            if (currentKey === record.key) {
              if (tupleForm === 'text') {
                fieldTypeHtml = textHtml
              } else if (tupleForm === 'edit') {
                fieldTypeHtml = inputhtml
              }
            } else {
              fieldTypeHtml = textHtml
            }
          }
        } else {
          fieldTypeHtml = ''
        }

        let initType = ''
        if (currentKey === record.key && tupleForm === 'edit') {
          initType = 'tuple'
        } else if (record.fieldType.includes('##')) {
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
                  onClick={() => this.props.initSelectUmsIdTs(record, 'ums_ts_')}
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
                  onChange={() => this.props.initSelectUmsIdTs(record, 'ums_id_')}
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
                <RadioGroup className="radio-group-style" size="default" disabled={this.isDisabledLoad()} onChange={(e) => this.props.initChangeUmsType(e.target.value)}>
                  <RadioButton value="ums" className="radio-btn-style radio-btn-extra">UMS</RadioButton>
                  <RadioButton value="ums_extension" className="ums-extension">UMS_Extension</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
        </Row>
        <Row className={umsTypeSeleted === 'ums_extension' ? '' : 'hide'}>
          <Col span={7} className="schema-json-title"><span>Source JSON Sample</span></Col>
          <Col span={1}></Col>
          <Col span={16} className="schema-table-title">
            <span>Source Schema Table [<FormattedMessage {...messages.nsShemaTableTitle} />]
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
  form: PropTypes.any,
  initChangeSelected: PropTypes.func,
  initChangeUmsType: PropTypes.func,
  onChangeJsonToTable: PropTypes.func,
  umsFieldTypeSelectOk: PropTypes.func,
  initUmsopOther2Tuple: PropTypes.func,
  initEditRename: PropTypes.func,
  initSelectUmsIdTs: PropTypes.func,
  umsTypeSeleted: PropTypes.string,
  selectAllState: PropTypes.string,
  namespaceClassHide: PropTypes.string,
  initRowSelectedAll: PropTypes.func,
  initSelectUmsop: PropTypes.func,
  repeatRenameArr: PropTypes.array,
  repeatArrayArr: PropTypes.array,
  roleType: PropTypes.string,
  locale: PropTypes.string,
  emitUmSopable: PropTypes.func
}

const mapStateToProps = createStructuredSelector({
  roleType: selectRoleType(),
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, null)(SchemaTypeConfig))
