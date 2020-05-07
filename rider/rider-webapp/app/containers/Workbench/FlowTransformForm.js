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
import { FormattedMessage } from 'react-intl'
import messages from './messages'
import { cloneDeep } from 'lodash'
import Form from 'antd/lib/form'
const FormItem = Form.Item
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Popover from 'antd/lib/popover'
import Tooltip from 'antd/lib/tooltip'
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
import Select from 'antd/lib/select'
import Cascader from 'antd/lib/cascader'
import Radio from 'antd/lib/radio'
import { Table, Card, Button, Modal } from 'antd'
const RadioButton = Radio.Button
const RadioGroup = Radio.Group

import { forceCheckNum, operateLanguageSelect, operateLanguageFillIn } from '../../utils/util'
import DataSystemSelector from '../../components/DataSystemSelector'
import { flowTransformationDadaHide, flowTransformationDadaShow } from '../../components/DataSystemSelector/dataSystemFunction'
import FilterComponent from './components/FilterComponent'

export class FlowTransformForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      dsHideOrNot: '',
      selectValue: '',
      cepDataSource: [],
      operatorBtnInitVal: '',
      outputType: '',
      outputFilteredRowSelect: '',
      outputFilteredRowFieldName: '',
      patternModalShow: false,
      quartifierTimesBtnToInput: false,
      quartifierTimesOrMoreBtnToInput: false,
      patternSourceDataConditions: null,
      editRow: -1,
      quantifierInputValidate: '',
      quantifierInputValidateTxt: '',
      outputFieldList: []
    }
  }
  componentDidMount () {
    const { cepPropData, outputType, outputFieldList } = this.props
    this.formatCepOutputData({ outputType, outputFieldList })
    this.formatCepPatternData(cepPropData)
  }
  componentWillReceiveProps (props) {
    if (this.props.transformModalVisible === props.transformModalVisible) return
    if (props.transformModalVisible === false) {
      this.setState({
        outputType: '',
        outputFilteredRowSelect: '',
        outputFilteredRowFieldName: '',
        outputFieldList: []
      })
      return
    }
    const { outputType, outputFieldList } = props
    this.formatCepOutputData({ outputType, outputFieldList })
    if (props.cepPropData) {
      this.formatCepPatternData(props.cepPropData)
    }
  }
  formatCepOutputData = ({ outputType, outputFieldList }) => {
    if (outputType === 'agg') {
      this.setState({ outputFieldList })
    } else if (outputType === 'filteredRow') {
      this.setState({outputFilteredRowSelect: outputFieldList[0].function_type, outputFilteredRowFieldName: outputFieldList[0].field_name})
    }
    this.setState({outputType})
  }
  formatCepPatternData = (data) => {
    if (!data.tranConfigInfoSql) {
      this.setState({cepDataSource: []})
      return
    }
    if (data.transformType === 'cep') {
      let cepDataSource = []
      let tranConfigInfoSql = typeof data.tranConfigInfoSql === 'string' && JSON.parse(data.tranConfigInfoSql.split(';')[0])
      cepDataSource = tranConfigInfoSql.pattern_seq.map(v => {
        if (v.quantifier === '{}') {
          v.quantifier = ''
        } else {
          v.quantifier = JSON.stringify(v.quantifier)
        }
        return v
      })
      this.setState({
        cepDataSource
      }, () => {
        this.props.emitCepSourceData(cepDataSource)
      })
    }
  }
  onTransformTypeSelect = (e) => {
    this.setState({
      selectValue: e.target.value
    })
    this.props.onInitTransformValue(e.target.value)
  }

  onLookupSqlTypeItemSelect = (val) => this.setState({ dsHideOrNot: val === 'union' ? 'hide' : '' })

  // 通过不同的Transformation里的 Sink Data System 显示不同的 Sink Namespace 的内容
  onTransformSinkDataSystemItemSelect = (val) => {
    this.props.form.setFieldsValue({ transformSinkNamespace: undefined })
    this.props.onInitTransformSinkTypeNamespace(this.props.projectIdGeted, val, 'transType')
  }

  changeOutput = (e) => {
    let outputType = e.target.value
    this.setState({outputType})
  }
  selectOutputFilteredRow = (value, option) => {
    this.setState({outputFilteredRowSelect: value})
  }
  doFilterQuery = (conditions) => {
    const { cepDataSource, editRow, operatorBtnInitVal } = this.state
    let patternValue = {}
    let quartifierObj = {}
    let patternOperHiddenQuartifier = false
    let validateStr = []
    if (operatorBtnInitVal === 'notnext' || operatorBtnInitVal === 'notfollowedby') patternOperHiddenQuartifier = true
    if (patternOperHiddenQuartifier) {
      validateStr = ['operator', 'conditions']
    } else {
      validateStr = ['operator', 'quantifier', 'conditions']
    }
    this.props.form.validateFieldsAndScroll(validateStr, (err, values) => {
      let { operator, quantifier } = values
      if (err && err.quantifier) {
        this.setState({
          quantifierInputValidate: 'error',
          quantifierInputValidateTxt: '请填写quantifier'
        })
        return
      }
      if (!patternOperHiddenQuartifier) {
        quartifierObj.type = values.quantifier
        switch (quantifier) {
          case 'oneormore':
            quartifierObj.value = 1
            break
          case 'times':
            let timesInput = this.quartifierTimesInput.value
            quartifierObj.value = timesInput
            break
          case 'timesormore':
            let timesOrMoreInput = this.quartifierTimesOrMoreInput.value
            quartifierObj.value = timesOrMoreInput
            break
          default:
            quartifierObj = ''
        }
        quartifierObj = quartifierObj === '' ? '' : JSON.stringify(quartifierObj)
        patternValue = {
          pattern_type: operator,
          quantifier: quartifierObj,
          conditions: JSON.stringify(conditions)
        }
      } else {
        patternValue = {
          pattern_type: operator,
          conditions: JSON.stringify(conditions)
        }
      }
      if (!err) {
        let newCepTableDataSource = cepDataSource.slice()
        if (editRow > -1) {
          newCepTableDataSource[editRow] = patternValue
        } else {
          newCepTableDataSource.push(patternValue)
        }
        this.setState({
          cepDataSource: newCepTableDataSource,
          editRow: -1,
          quantifierInputValidate: '',
          quantifierInputValidateTxt: ''
        }, () => {
          this.props.emitCepSourceData(newCepTableDataSource)
          this.clearPatterModalData(() => {
            this.setState({
              patternModalShow: false
            })
          })
        })
      }
    })
  }

  changeQuartifier = (event) => {
    let value = event.target.value
    if (value === 'times') {
      this.setState({
        quartifierTimesBtnToInput: true,
        quartifierTimesOrMoreBtnToInput: false
      }, () => {
        this.quartifierTimesInput.value = ''
        this.quartifierTimesOrMoreInput.value = ''
      })
    } else if (value === 'timesormore') {
      this.setState({
        quartifierTimesBtnToInput: false,
        quartifierTimesOrMoreBtnToInput: true
      }, () => {
        this.quartifierTimesInput.value = ''
        this.quartifierTimesOrMoreInput.value = ''
      })
    } else {
      this.setState({
        quartifierTimesOrMoreBtnToInput: false,
        quartifierTimesBtnToInput: false,
        quantifierInputValidate: '',
        quantifierInputValidateTxt: ''
      }, () => {
        this.quartifierTimesInput.value = ''
        this.quartifierTimesOrMoreInput.value = ''
      })
    }
  }
  quantifierInputChange = (e) => {
    let value = e.target.value
    if (value === '') {
      if (!this.state.quantifierInputValidate) {
        this.setState({
          quantifierInputValidate: 'error',
          quantifierInputValidateTxt: '请填写quantifier'
        })
      }
    } else {
      if (this.state.quantifierInputValidate) {
        this.setState({
          quantifierInputValidate: '',
          quantifierInputValidateTxt: ''
        })
      }
    }
  }
  addOrEditPattern = () => {
    let quantifierBtn = this.props.form.getFieldValue('quantifier')
    let operatorBtn = this.props.form.getFieldValue('operator')
    if (operatorBtn === 'begin' || operatorBtn === 'next' || operatorBtn === 'followedby') {
      if (quantifierBtn === 'times') {
        if (this.quartifierTimesInput.value === '') {
          this.setState({
            quantifierInputValidate: 'error',
            quantifierInputValidateTxt: '请填写quantifier'
          })
          return
        }
      } else if (quantifierBtn === 'timesormore') {
        if (this.quartifierTimesOrMoreInput.value === '') {
          this.setState({
            quantifierInputValidate: 'error',
            quantifierInputValidateTxt: '请填写quantifier'
          })
          return
        }
      }
    }
    this.filterComponent.doQuery()
  }
  onEditPattern = (record, index) => (e) => {
    let patternSourceDataConditions = record.conditions
    let quartifierObj = record.quantifier && JSON.parse(record.quantifier)
    if (quartifierObj) {
      this.props.form.setFieldsValue({
        operator: record.pattern_type,
        quantifier: quartifierObj.type
      })
      switch (quartifierObj.type) {
        case 'times':
          this.setState({
            quartifierTimesBtnToInput: true,
            quartifierTimesOrMoreBtnToInput: false
          }, () => {
            this.quartifierTimesInput.value = quartifierObj.value
          })
          break
        case 'timesormore':
          this.setState({
            quartifierTimesBtnToInput: false,
            quartifierTimesOrMoreBtnToInput: true
          }, () => {
            this.quartifierTimesOrMoreInput.value = quartifierObj.value
          })
          break
      }
    } else {
      this.props.form.setFieldsValue({
        operator: record.pattern_type
      })
    }

    this.setState({
      operatorBtnInitVal: record.pattern_type,
      patternSourceDataConditions,
      patternModalShow: true,
      editRow: index
    }, () => {
      this.props.form.setFieldsValue({conditions: JSON.parse(patternSourceDataConditions)})
    })
  }
  onDeletePattern = (index) => () => {
    let cepDataSource = this.state.cepDataSource.slice()
    cepDataSource.splice(index, 1)
    this.setState({cepDataSource})
    this.props.emitCepSourceData(cepDataSource)
  }
  addPatternModal = () => {
    const { cepDataSource } = this.state
    this.clearPatterModalData(() => {
      if (cepDataSource.length === 0) {
        this.setState({operatorBtnInitVal: 'begin'})
        this.props.form.setFieldsValue({operator: 'begin'})
      } else {
        this.setState({operatorBtnInitVal: ''})
      }
      this.setState({
        patternModalShow: true
      })
    })
  }
  offPatternModal = () => {
    this.clearPatterModalData(() => {
      this.setState({
        patternModalShow: false
      })
    })
  }

  clearPatterModalData = (fn) => {
    this.props.form.setFieldsValue({
      operator: '',
      quantifier: '',
      conditions: ''
    })
    this.setState({
      operatorBtnInitVal: '',
      quartifierTimesBtnToInput: false,
      quartifierTimesOrMoreBtnToInput: false,
      patternSourceDataConditions: null,
      editRow: -1,
      quantifierInputValidate: '',
      quantifierInputValidateTxt: ''
    }, () => {
      if (this.quartifierTimesInput) {
        this.quartifierTimesInput.value = ''
      }
      if (this.quartifierTimesOrMoreInput) {
        this.quartifierTimesOrMoreInput.value = ''
      }
      if (this.filterComponent) {
        this.filterComponent.resetTree()
      }
      if (fn) fn()
    })
  }

  setQuartifierTimesInputRef = el => {
    this.quartifierTimesInput = el
  }

  setQuartifierTimesOrMoreInputRef = el => {
    this.quartifierTimesOrMoreInput = el
  }
  operatorChange = (event) => {
    let operatorBtnInitVal = event.target.value
    this.setState({operatorBtnInitVal})
  }
  changeCepOutputAgg = (action, index) => {
    this.setState({outputFilteredRowSelect: ''})
    let outputFieldList = cloneDeep(this.state.outputFieldList)
    if (action === 'add') {
      outputFieldList.push({
        function_type: '',
        field_name: '',
        alias_name: '',
        _id: Date.now()
      })
    } else if (action === 'remove') {
      outputFieldList.splice(index, 1)
    }
    this.setState({outputFieldList})
  }
  render () {
    const { form, transformValue, transformSinkTypeNamespaceData, flowTransNsData, step2SourceNamespace, step2SinkNamespace, flowSubPanelKey } = this.props
    const { dsHideOrNot, selectValue, cepDataSource, outputType, outputFilteredRowSelect, outputFilteredRowFieldName, patternModalShow, operatorBtnInitVal, quartifierTimesBtnToInput, quartifierTimesOrMoreBtnToInput, patternSourceDataConditions, quantifierInputValidate, quantifierInputValidateTxt } = this.state
    const { getFieldDecorator } = form

    const itemStyle = {
      labelCol: { span: 4 },
      wrapperCol: { span: 12 }
    }
    const patternItemStyle = {
      labelCol: { span: 4 },
      wrapperCol: { span: 20 }
    }
    const diffType = [
      flowSubPanelKey === 'spark' ? '' : 'hide',
      flowSubPanelKey === 'flink' ? '' : 'hide'
    ]
  // ----- spark -------
    const transformTypeClassNames = [
      transformValue === 'lookupSql' ? '' : 'hide',
      transformValue === 'sparkSql' ? '' : 'hide',
      transformValue === 'streamJoinSql' ? '' : 'hide',
      transformValue === 'transformClassName' ? '' : 'hide'
    ]

    const transformTypeHiddens = [
      transformValue !== 'lookupSql',
      transformValue !== 'sparkSql',
      transformValue !== 'streamJoinSql',
      transformValue !== 'transformClassName'
    ]

    const flowLookupSqlType = [
      { value: 'leftJoin', text: 'Left Join' },
      { value: 'union', text: 'Union' }
    ]

    const flowStreamJoinSqlType = [
      { value: 'leftJoin', text: 'Left Join' },
      { value: 'innerJoin', text: 'Inner Join' }
    ]
  // --------------------
  // ------ flink -------
    const flinkTransformTypeClassNames = [
      transformValue === 'lookupSql' ? '' : 'hide',
      transformValue === 'flinkSql' ? '' : 'hide',
      transformValue === 'cep' ? '' : 'hide'
    ]

    const flinkTransformTypeHiddens = [
      transformValue !== 'lookupSql',
      transformValue !== 'flinkSql',
      transformValue !== 'cep'
    ]

    const outputHiddens = [
      outputType === 'detail',
      outputType === 'agg',
      outputType === 'filteredRow',
      outputType === 'timeout',
      outputFilteredRowSelect === 'min',
      outputFilteredRowSelect === 'max'
    ]
    const patternOperatorAboutHiddens = [
      operatorBtnInitVal === 'next',
      operatorBtnInitVal === 'followedby',
      operatorBtnInitVal === 'notnext',
      operatorBtnInitVal === 'notfollowedby'
    ]

    const flinkFlowLookupSqlType = [
      { value: 'leftJoin', text: 'Left Join' }
    ]
  // ----------------------
    const sinkDataSystemData = dsHideOrNot
      ? flowTransformationDadaHide()
      : flowTransformationDadaShow()

    const nsChildren = flowTransNsData.map(i => {
      const temp = [i.nsSys, i.nsInstance, i.nsDatabase, i.nsTable].join('.')
      return (
        <Select.Option key={i.id} value={temp}>
          {temp}
        </Select.Option>
      )
    })

    let sqlMsg = ''
    if (selectValue === 'lookupSql') {
      sqlMsg = <FormattedMessage {...messages.workbenchTransLookup} />
    } else if (selectValue === 'sparkSql' || selectValue === 'flinkSql') {
      sqlMsg = <FormattedMessage {...messages.workbenchTransSpark} />
    }

    const sqlHtml = (
      <span>
        SQL
        <Tooltip title={<FormattedMessage {...messages.workbenchHelp} />}>
          <Popover
            placement="top"
            content={
              <div style={{ width: '400px', height: '90px' }}>
                <p>{sqlMsg}</p>
              </div>
            }
            title={<h3><FormattedMessage {...messages.workbenchHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const windowTimeHelp = (
      <span>
        Windowtime
        <Tooltip title={<FormattedMessage {...messages.workbenchHelp} />} placement="bottom">
          <Popover
            placement="top"
            content={
              <div style={{ width: '280px', height: '35px' }}>
                <p><FormattedMessage {...messages.workbenchFlowTransCepWindowtime} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.workbenchHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )
    const strategyHelp = (
      <span>
        Strategy
        <Tooltip title={<FormattedMessage {...messages.workbenchHelp} />} placement="bottom">
          <Popover
            placement="top"
            content={
              <div style={{ width: '280px', height: '80px' }}>
                <p><FormattedMessage {...messages.workbenchFlowTransCepStrategy} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.workbenchHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const keyByHelp = (
      <span>
        KeyBy
        <Tooltip title={<FormattedMessage {...messages.workbenchHelp} />} placement="bottom">
          <Popover
            placement="top"
            content={
              <div style={{ width: '280px', height: '110px' }}>
                <p><FormattedMessage {...messages.workbenchFlowTransCepKeyby} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.workbenchHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const outputHelp = (
      <span>
        Output
        <Tooltip title={<FormattedMessage {...messages.workbenchHelp} />} placement="bottom">
          <Popover
            placement="top"
            content={
              <div style={{ width: '280px', height: '100px' }}>
                <p><FormattedMessage {...messages.workbenchFlowTransCepOutput} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.workbenchHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const operatorHelp = (
      <span>
        Operator
        <Tooltip title={<FormattedMessage {...messages.workbenchHelp} />} placement="bottom">
          <Popover
            placement="top"
            content={
              <div style={{ width: '280px', height: '100px' }}>
                <p><FormattedMessage {...messages.workbenchFlowTransCepOperator} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.workbenchHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const quartifierHelp = (
      <span>
        Quantifier
        <Tooltip title={<FormattedMessage {...messages.workbenchHelp} />} placement="bottom">
          <Popover
            placement="top"
            content={
              <div style={{ width: '280px', height: '60px' }}>
                <p><FormattedMessage {...messages.workbenchFlowTransCepQuartifier} /></p>
              </div>}
            title={<h3><FormattedMessage {...messages.workbenchHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )
    const columnsCEP = [
      {
        title: 'Operator',
        dataIndex: 'pattern_type',
        key: 'pattern_type',
        width: '10%',
        className: 'text-align-center'
      },
      {
        title: 'Conditions',
        dataIndex: 'conditions',
        key: 'conditions',
        width: '40%',
        className: 'text-align-center'
      },
      {
        title: 'Quantifier',
        dataIndex: 'quantifier',
        key: 'quantifier',
        width: '30%',
        className: 'text-align-center'
      },
      {
        title: 'Action',
        dataIndex: '',
        key: 'action',
        width: '20%',
        className: 'text-align-center',
        render: (value, record, index) => {
          const modifyFormat = <FormattedMessage {...messages.workbenchTransModify} />
          const deleteFormat = <FormattedMessage {...messages.workbenchTransDelete} />
          return (
            <span className="ant-table-action-column">
              <Tooltip title={modifyFormat}>
                <Button icon="edit" shape="circle" type="ghost" onClick={this.onEditPattern(record, index)}></Button>
              </Tooltip>
              <Tooltip title={deleteFormat}>
                <Button shape="circle" type="ghost" onClick={this.onDeletePattern(index)}>
                  <i className="anticon anticon-delete"></i>
                </Button>
              </Tooltip>
            </span>
          )
        }
      }
    ]
    // const pagination = {
    //   defaultPageSize: 5,
    //   pageSizeOptions: ['5', '10', '15'],
    //   showSizeChanger: true,
    //   onShowSizeChange: (current, pageSize) => {
    //     this.setState({
    //       pageIndex: current,
    //       pageSize: pageSize
    //     })
    //   },
    //   onChange: (current) => {
    //     this.setState({ pageIndex: current })
    //   }
    // }
    const aggOptions = this.state.outputFieldList.map((v, i) => (
      <Col span={24} key={v._id}>
        <Col span={5} offset={1} className={`${flinkTransformTypeClassNames[2]} ${!outputHiddens[1] ? 'hide' : ''}`}>
          <FormItem label="Agg" {...{ labelCol: { span: 16 }, wrapperCol: { span: 8 } }}>
            {getFieldDecorator(`outputAggSelect_${v._id}`, {
              rules: [{
                required: true,
                message: operateLanguageSelect('agg select', 'Agg Select')
              }],
              hidden: !outputHiddens[1] || flinkTransformTypeHiddens[2],
              initialValue: v.function_type
            })(
              <Select>
                <Select.Option value="avg">Avg</Select.Option>
                <Select.Option value="count">Count</Select.Option>
                <Select.Option value="min">Min</Select.Option>
                <Select.Option value="max">Max</Select.Option>
                <Select.Option value="sum">Sum</Select.Option>
              </Select>
            )}
          </FormItem>
        </Col>
        <Col span={7} offset={1} className={`${flinkTransformTypeClassNames[2]} ${!outputHiddens[1] ? 'hide' : ''}`}>
          <FormItem label="Field Name" {...{ labelCol: { span: 8 }, wrapperCol: { span: 10 } }}>
            {getFieldDecorator(`outputAggFieldName_${v._id}`, {
              required: true,
              hidden: !outputHiddens[1] || flinkTransformTypeHiddens[2],
              initialValue: v.field_name
            })(
              <Input />
            )}
          </FormItem>
        </Col>
        <Col span={8} className={`${flinkTransformTypeClassNames[2]} ${!outputHiddens[1] ? 'hide' : ''}`}>
          <FormItem label="Rename" {...{ labelCol: { span: 4 }, wrapperCol: { span: 8 } }}>
            {getFieldDecorator(`outputAggRename_${v._id}`, {
              rules: [{
                validator: (rule, value, callback) => {
                  const formValues = this.props.form.getFieldsValue()
                  const fieldNameArr = []
                  const hasRepeatValue = Object.keys(formValues).some(v => {
                    if (v.includes('outputAggFieldName')) {
                      const value = formValues[v]
                      if (value === '') return false
                      const hasRepeatValue = fieldNameArr.some(m => m === value)
                      if (hasRepeatValue) {
                        return hasRepeatValue
                      } else {
                        fieldNameArr.push(formValues[v])
                      }
                    }
                  })
                  if (hasRepeatValue) {
                    if (value !== '') {
                      callback()
                    }
                    const errMsg = '存在重复的Field Name,需填写此项'
                    rule.message = errMsg
                    callback(errMsg)
                  } else {
                    callback()
                  }
                }
              }],
              hidden: !outputHiddens[1] || flinkTransformTypeHiddens[2],
              initialValue: v.alias_name
            })(
              <Input />
            )}
          </FormItem>
        </Col>
        <Col span={1} className={`${flinkTransformTypeClassNames[2]} ${!outputHiddens[1] ? 'hide' : ''}`}>
          <Button shape="circle" type="danger" onClick={() => this.changeCepOutputAgg('remove', i)}>
            <Icon type="minus" />
          </Button>
        </Col>
      </Col>
      ))
    return (
      <Form className="transform-modal-style">
        <Row>
          <Col span={24}>
            <FormItem label="Source Namespace" {...itemStyle}>
              {getFieldDecorator('step2SourceNamespace', {})(
                <strong className="value-font-style">{step2SourceNamespace}</strong>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Sink Namespace" {...itemStyle}>
              {getFieldDecorator('step2SinkNamespace', {})(
                <strong className="value-font-style">{step2SinkNamespace}</strong>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem className="hide">
              {getFieldDecorator('editTransformId', {})(
                <Input />
              )}
            </FormItem>
            <FormItem label="Transformation" {...itemStyle}>
              {getFieldDecorator('transformation', {
                rules: [{
                  required: true,
                  message: operateLanguageSelect('transformation', 'Transformation')
                }]
              })(
                <RadioGroup onChange={this.onTransformTypeSelect}>
                  <RadioButton value="lookupSql">Lookup SQL</RadioButton>

                  <RadioButton value="sparkSql" className={diffType[0]}>Spark SQL</RadioButton>
                  <RadioButton value="streamJoinSql" className={diffType[0]}>Stream Join SQL</RadioButton>
                  <RadioButton value="transformClassName" className={diffType[0]}>ClassName</RadioButton>

                  <RadioButton value="flinkSql" className={diffType[1]}>Flink SQL</RadioButton>
                  <RadioButton value="cep" className={diffType[1]}>CEP</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>

          {/* 设置 Lookup Sql */}
          <Col span={24} className={`${transformTypeClassNames[0] || flinkTransformTypeClassNames[0]}`}>
            <FormItem label="Type" {...itemStyle} style={{lineHeight: '36px'}}>
              {getFieldDecorator('lookupSqlType', {
                rules: [{
                  required: true,
                  message: operateLanguageSelect('type', 'Type')
                }],
                initialValue: 'leftJoin',
                hidden: transformTypeHiddens[0]
              })(
                <DataSystemSelector
                  data={flowSubPanelKey === 'spark' ? flowLookupSqlType : flowSubPanelKey === 'flink' ? flinkFlowLookupSqlType : []}
                  onItemSelect={this.onLookupSqlTypeItemSelect}
                />
              )}
            </FormItem>
          </Col>
          <Col span={24} className={transformTypeClassNames[0] || flinkTransformTypeClassNames[0]}>
            <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
              {getFieldDecorator('transformSinkDataSystem', {
                rules: [{
                  required: true,
                  message: operateLanguageSelect('data system', 'Data System')
                }],
                hidden: transformTypeHiddens[0] || flinkTransformTypeClassNames[0]
              })(
                <DataSystemSelector
                  data={sinkDataSystemData}
                  onItemSelect={this.onTransformSinkDataSystemItemSelect}
                />
              )}
            </FormItem>
          </Col>
          <Col span={24} className={transformTypeClassNames[0]}>
            <FormItem label="Database" {...itemStyle}>
              {getFieldDecorator('transformSinkNamespace', {
                rules: [{
                  required: true,
                  message: operateLanguageSelect('Database', 'Database')
                }],
                hidden: transformTypeHiddens[0]
              })(
                <Cascader
                  placeholder="Select a Database"
                  popupClassName="ri-workbench-select-dropdown"
                  options={transformSinkTypeNamespaceData}
                  expandTrigger="hover"
                  displayRender={(labels) => labels.join('.')}
                />
              )}
            </FormItem>
          </Col>
          <Col span={4} className={transformTypeClassNames[0]}>
            <FormItem label={sqlHtml} className="tran-sql-label">
              {getFieldDecorator('lookupSql', {
                hidden: transformTypeHiddens[0]
              })(
                <Input className="hide" />
              )}
            </FormItem>
          </Col>
          <Col span={17} className={`${transformTypeClassNames[0]} cm-sql-textarea`}>
            <textarea
              id="lookupSqlTextarea"
              placeholder="Lookup SQL"
            />
          </Col>

          {/* 设置 Spark/Flink Sql */}
          {/* {flowSubPanelKey === 'spark' ? ( */}
          <Col span={4} className={transformTypeClassNames[1]}>
            <FormItem label={sqlHtml} className="tran-sql-label">
              {getFieldDecorator('sparkSql', {
                hidden: transformTypeHiddens[1]
              })(
                <Input className="hide" />
              )}
            </FormItem>
          </Col>
          {/* ) : flowSubPanelKey === 'flink' ? ( */}
          <Col span={4} className={flinkTransformTypeClassNames[1]}>
            <FormItem label={sqlHtml} className="tran-sql-label">
              {getFieldDecorator('flinkSql', {
                hidden: flinkTransformTypeHiddens[1]
              })(
                <Input className="hide" />
              )}
            </FormItem>
          </Col>
          {/* ) : ''} */}
          {/* <Col span={17} className={`${transformValue === 'sparkSql' || transformValue === 'flinkSql' ? '' : 'hide'} cm-sql-textarea`}>
            <textarea
              id="sparkOrFlinkSqlTextarea"
              placeholder={flowSubPanelKey === 'spark' ? 'Spark SQL' : flowSubPanelKey === 'flink' ? 'Flink SQL' : ''}
            />
          </Col> */}
          {/* {flowSubPanelKey === 'spark' ? ( */}
          <Col span={17} className={`${transformTypeClassNames[1]} cm-sql-textarea`}>
            <textarea
              id="sparkSqlTextarea"
              placeholder={'Spark SQL'}
            />
          </Col>
          {/* ) : flowSubPanelKey === 'flink' ? ( */}
          <Col span={17} className={`${flinkTransformTypeClassNames[1]} cm-sql-textarea`}>
            <textarea
              id="flinkSqlTextarea"
              placeholder={'Flink SQL'}
              // className={`ant-input ant-input-extra`}
            />
          </Col>
          {/* ) : ''} */}

          {/* 设置 Stream Join Sql */}
          {flowSubPanelKey === 'spark' ? (
            <Col span={24} className={transformTypeClassNames[2]}>
              <FormItem label="Type" {...itemStyle} style={{lineHeight: '36px'}}>
                {getFieldDecorator('streamJoinSqlType', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('type', 'Type')
                  }],
                  hidden: transformTypeHiddens[2]
                })(
                  <DataSystemSelector
                    data={flowStreamJoinSqlType}
                    onItemSelect={this.onStreamJoinSqlTypeItemSelect}
                  />
                )}
              </FormItem>
            </Col>
          ) : '' }
          {flowSubPanelKey === 'spark' ? (
            <Col span={24} className={transformTypeClassNames[2]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('streamJoinSqlNs', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('namespace', 'Namespace')
                  }],
                  hidden: transformTypeHiddens[2]
                })(
                  <Select
                    mode="multiple"
                    placeholder="Select namespaces"
                  >
                    {nsChildren}
                  </Select>
                )}
              </FormItem>
            </Col>
          ) : '' }
          {flowSubPanelKey === 'spark' ? (
            <Col span={24} className={transformTypeClassNames[2]}>
              <FormItem label="Retention time (Sec)" {...itemStyle}>
                {getFieldDecorator('timeout', {
                  rules: [{
                    required: true,
                    message: operateLanguageFillIn('retention time', 'Retention Time')
                  }, {
                    validator: forceCheckNum
                  }],
                  hidden: transformTypeHiddens[2]
                })(
                  <InputNumber min={10} max={1800} step={1} placeholder="Time" />
                )}
              </FormItem>
            </Col>
          ) : '' }
          {flowSubPanelKey === 'spark' ? (
            <Col span={4} className={transformTypeClassNames[2]}>
              <FormItem label="SQL" className="tran-sql-label">
                {getFieldDecorator('streamJoinSql', {
                  hidden: transformTypeHiddens[2]
                })(
                  <Input className="hide" />
                )}
              </FormItem>
            </Col>
          ) : '' }
          {flowSubPanelKey === 'spark' ? (
            <Col span={17} className={`${transformTypeClassNames[2]} cm-sql-textarea`}>
              <textarea
                id="streamJoinSqlTextarea"
                placeholder="Stream Join SQL"
              />
            </Col>
          ) : '' }

          {/* 设置 ClassName */}
          {flowSubPanelKey === 'spark' ? (
            <Col span={24} className={transformTypeClassNames[3]}>
              <FormItem label="ClassName" {...itemStyle}>
                {getFieldDecorator('transformClassName', {
                  rules: [{
                    required: true,
                    message: operateLanguageFillIn('className', 'ClassName')
                  }],
                  hidden: transformTypeHiddens[3]
                })(
                  <Input type="textarea" placeholder="ClassName" autosize={{ minRows: 5, maxRows: 8 }} />
                )}
              </FormItem>
            </Col>
          ) : '' }

          {/* 设置 Flink CEP  */}
          {flowSubPanelKey === 'flink' ? (
            <Col span={24} className={flinkTransformTypeClassNames[2]}>
              <FormItem label={windowTimeHelp} {...itemStyle}>
                {getFieldDecorator('windowTime', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('windowTime', 'Windowtime')
                  }],
                  hidden: flinkTransformTypeHiddens[2]
                })(
                  <InputNumber step={1} min={0} placeholder="seconds" />
                )}
              </FormItem>
            </Col>
          ) : '' }
          {flowSubPanelKey === 'flink' ? (
            <Col span={24} className={flinkTransformTypeClassNames[2]}>
              <FormItem label={strategyHelp} {...itemStyle}>
                {getFieldDecorator('strategy', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('strategy', 'Strategy')
                  }],
                  hidden: flinkTransformTypeHiddens[2]
                })(
                  <Select
                    dropdownClassName="ri-workbench-select-dropdown"
                    placeholder="Select a strategy"
                    // disabled={flowDisabledOrNot}
                  >
                    <Select.Option key="NO_SKIP" value="no_skip">NO_SKIP</Select.Option>
                    <Select.Option key="SKIP_PAST_LAST_EVENT" value="skip_past_last_event">SKIP_PAST_LAST_EVENT</Select.Option>
                    {/* <Select.Option key="SKIP_TO_FIRST" value="skip_to_first">SKIP_TO_FIRST</Select.Option>
                    <Select.Option key="SKIP_TO_LAST" value="skip_to_last">SKIP_TO_LAST</Select.Option> */}
                  </Select>
                )}
              </FormItem>
            </Col>
          ) : '' }
          {flowSubPanelKey === 'flink' ? (
            <Col span={24} className={flinkTransformTypeClassNames[2]}>
              <FormItem label={keyByHelp} {...itemStyle}>
                {getFieldDecorator('keyBy', {
                  rules: [{
                    required: true,
                    message: operateLanguageFillIn('keyBy', 'Keyby')
                  }],
                  hidden: flinkTransformTypeHiddens[2]
                })(
                  <Input />
                )}
              </FormItem>
            </Col>
          ) : '' }
          {flowSubPanelKey === 'flink' ? (
            <Col span={12} offset={2} className={flinkTransformTypeClassNames[2]}>
              <FormItem label={outputHelp} {...itemStyle}>
                {getFieldDecorator('output', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('output', 'Output')
                  }],
                  initialValue: this.props.outputType,
                  hidden: flinkTransformTypeHiddens[2]
                })(
                  <RadioGroup onChange={this.changeOutput}>
                    <RadioButton value="detail">Detail</RadioButton>
                    <RadioButton value="agg">Agg</RadioButton>
                    <RadioButton value="filteredRow">FilteredRow</RadioButton>
                    <RadioButton value="timeout">Timeout</RadioButton>
                  </RadioGroup>
                )}
              </FormItem>
            </Col>
          ) : '' }
          {/* agg */}
          {flowSubPanelKey === 'flink' ? (
            <Col span={2} pull={5} className={`${flinkTransformTypeClassNames[2]} ${!outputHiddens[1] ? 'hide' : ''}`}>
              <Button shape="circle" type="default" onClick={() => this.changeCepOutputAgg('add')}>
                <Icon type="plus" />
              </Button>
            </Col>
          ) : ''}
          {flowSubPanelKey === 'flink' ? (
            aggOptions
          ) : ''}
          {/* filteredRow */}
          {flowSubPanelKey === 'flink' ? (
            <Col span={24}>
              <Col span={5} offset={1} className={`${flinkTransformTypeClassNames[2]} ${!outputHiddens[2] ? 'hide' : ''}`}>
                <FormItem label="FilteredRow" {...{ labelCol: { span: 16 }, wrapperCol: { span: 8 } }}>
                  {getFieldDecorator('outputFilteredRowSelect', {
                    rules: [{
                      required: true,
                      message: operateLanguageSelect('filteredRow output', 'FilteredRow Output')
                    }],
                    hidden: !outputHiddens[2] || flinkTransformTypeHiddens[2],
                    initialValue: outputFilteredRowSelect
                  })(
                    <Select onChange={this.selectOutputFilteredRow}>
                      <Select.Option value="head">Head</Select.Option>
                      <Select.Option value="last">Last</Select.Option>
                      <Select.Option value="min">Min</Select.Option>
                      <Select.Option value="max">Max</Select.Option>
                    </Select>
                  )}
                </FormItem>
              </Col>
              <Col span={8} offset={2} className={`${flinkTransformTypeClassNames[2]} ${!outputHiddens[2] || !outputHiddens[3] && !outputHiddens[4] ? 'hide' : ''}`}>
                <FormItem label="Field Name" {...{ labelCol: { span: 6 }, wrapperCol: { span: 8 } }}>
                  {getFieldDecorator('outputFilteredRowSelectFieldName', {
                    rules: [{
                      required: true,
                      message: operateLanguageSelect('field name', 'Field Name')
                    }],
                    hidden: !outputHiddens[2] || !outputHiddens[3] && !outputHiddens[4] || flinkTransformTypeHiddens[2],
                    initialValue: outputFilteredRowFieldName
                  })(
                    <Input />
                  )}
                </FormItem>
              </Col>
            </Col>
          ) : ''}
          {/* {flowSubPanelKey === 'flink' ? (
            <Col span={4} pull={4} className={`${flinkTransformTypeClassNames[2]} ${outputHiddens[0] ? 'hide' : ''}`}>
              <FormItem>
                {getFieldDecorator('outputText', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('output', 'Output')
                  }],
                  hidden: outputHiddens[0] || flinkTransformTypeHiddens[2]
                })(
                  <Input />
                )}
              </FormItem>
            </Col>
          ) : ''} */}
          {flowSubPanelKey === 'flink' ? (
            <Col span={24} className={`${flinkTransformTypeClassNames[2]}`}>
              <FormItem
                label="Pattern"
                {...itemStyle}
                validateStatus={cepDataSource.length > 0 ? 'success' : 'error'}
                help={operateLanguageFillIn('patternBtn', 'Pattern')}
              >
                <Button onClick={this.addPatternModal}>添加Pattern</Button>
              </FormItem>
            </Col>
          ) : ''}
          {flowSubPanelKey === 'flink' ? (
            <Col span={20} offset={4} className={flinkTransformTypeClassNames[2]}>
              <Table
                dataSource={cepDataSource}
                columns={columnsCEP}
                pagination={false}
                bordered
                rowKey="conditions"
              />
            </Col>
          ) : ''}
          {flowSubPanelKey === 'flink' ? (
            <Modal
              title="Pattern"
              okText="确定"
              visible={patternModalShow}
              wrapClassName="transform-form-style-sub"
              onOk={this.addOrEditPattern}
              onCancel={this.offPatternModal}
            >
              <Card className={`${flinkTransformTypeClassNames[2]}`}>
                <Col span={24}>
                  <FormItem label={operatorHelp} {...patternItemStyle}>
                    {getFieldDecorator('operator', {
                      rules: [{
                        required: true,
                        message: operateLanguageSelect('operator', 'Operator')
                      }],
                      initialValue: operatorBtnInitVal,
                      hidden: flinkTransformTypeHiddens[2] || !patternModalShow
                    })(
                      <RadioGroup size="default" onChange={this.operatorChange}>
                        {operatorBtnInitVal === 'begin' ? (
                          <RadioButton value="begin" className={`radio-btn-style`}>Begin</RadioButton>
                        ) : ''}
                        {operatorBtnInitVal !== 'begin' ? (
                          <RadioButton value="next" className={`radio-btn-style`}>Next</RadioButton>
                        ) : ''}
                        {operatorBtnInitVal !== 'begin' ? (
                          <RadioButton value="followedby" className={`radio-btn-style`}>FollowedBy</RadioButton>
                        ) : ''}
                        {operatorBtnInitVal !== 'begin' ? (
                          <RadioButton value="notnext" className={`radio-btn-style`}>NotNext</RadioButton>
                        ) : ''}
                        {operatorBtnInitVal !== 'begin' ? (
                          <RadioButton value="notfollowedby" className={`radio-btn-style`}>NotFollowedBy</RadioButton>
                        ) : ''}
                      </RadioGroup>
                    )}
                  </FormItem>
                </Col>
                <Col span={24} className={patternOperatorAboutHiddens[2] || patternOperatorAboutHiddens[3] ? 'hide' : ''}>
                  <FormItem label={quartifierHelp} {...patternItemStyle} validateStatus={quantifierInputValidate} help={quantifierInputValidateTxt}>
                    {getFieldDecorator('quantifier', {
                      rules: [{
                        required: true,
                        message: operateLanguageSelect('quantifier', 'Quantifier')
                      }],
                      hidden: flinkTransformTypeHiddens[2] || !patternModalShow || patternOperatorAboutHiddens[2] || patternOperatorAboutHiddens[3]
                    })(
                      <RadioGroup size="default" onChange={this.changeQuartifier}>
                        <RadioButton value="oneormore" className="radio-btn-style">OneOrMore</RadioButton>
                        <RadioButton value="times" className={`radio-btn-style ${quartifierTimesBtnToInput ? 'hide' : ''}`}>Times</RadioButton>
                        <input onChange={this.quantifierInputChange} ref={this.setQuartifierTimesInputRef} className={`${quartifierTimesBtnToInput ? '' : 'hide'}`} />
                        <RadioButton value="timesormore" className={`radio-btn-style ${quartifierTimesOrMoreBtnToInput ? 'hide' : ''}`}>TimesOrMore</RadioButton>
                        <input onChange={this.quantifierInputChange} ref={this.setQuartifierTimesOrMoreInputRef} className={`${quartifierTimesOrMoreBtnToInput ? '' : 'hide'}`} />
                      </RadioGroup>
                    )}
                  </FormItem>
                </Col>
                <Col span={24}>
                  <FormItem label="Conditions" {...patternItemStyle}>
                    {getFieldDecorator('conditions', {
                      rules: [{
                        required: true,
                        message: operateLanguageSelect('conditions', 'Conditions')
                      }],
                      hidden: flinkTransformTypeHiddens[2] || !patternModalShow
                    })(
                      <FilterComponent
                        sourceData={patternSourceDataConditions}
                        onQuery={this.doFilterQuery}
                        isOpen={patternModalShow}
                        onRef={ref => { this.filterComponent = ref }}
                      ></FilterComponent>
                    )}
                  </FormItem>
                </Col>
              </Card>
            </Modal>
          ) : ''}
        </Row>
      </Form>
    )
  }
}

FlowTransformForm.propTypes = {
  form: PropTypes.any,
  transformSinkTypeNamespaceData: PropTypes.array,
  projectIdGeted: PropTypes.string,
  transformValue: PropTypes.string,
  step2SinkNamespace: PropTypes.string,
  step2SourceNamespace: PropTypes.string,
  flowTransNsData: PropTypes.array,
  onInitTransformValue: PropTypes.func,
  onInitTransformSinkTypeNamespace: PropTypes.func,
  flowSubPanelKey: PropTypes.string,
  emitCepSourceData: PropTypes.func,
  transformModalVisible: PropTypes.bool,
  cepPropData: PropTypes.object,
  outputType: PropTypes.string,
  outputFieldList: PropTypes.array
}

export default Form.create({wrappedComponentRef: true})(FlowTransformForm)
