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

import DataSystemSelector from '../../components/DataSystemSelector'
import Form from 'antd/lib/form'
const FormItem = Form.Item
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
import Cascader from 'antd/lib/cascader'
import Popconfirm from 'antd/lib/popconfirm'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
import Button from 'antd/lib/button'
import Tag from 'antd/lib/tag'
import Icon from 'antd/lib/icon'
import Table from 'antd/lib/table'
import Card from 'antd/lib/card'
import Radio from 'antd/lib/radio'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button
import DatePicker from 'antd/lib/date-picker'
// const { RangePicker } = DatePicker

import { prettyShownText, uuid } from '../../utils/util'

export class WorkbenchJobForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      sinkConfigClass: ''
    }
  }

  componentWillReceiveProps (props) {
    if (props.jobTransTableSource) {
      props.jobTransTableSource.map(s => {
        s.key = uuid()
        // s.visible = false
        return s
      })
    }
  }

  onNameInputChange = (e) => this.props.onInitJobNameValue(e.target.value)

  onAllOrNotSelect = (e) => {
    this.props.initResultFieldClass(e)
  }

  onShowDataFrame = (e) => this.props.initDataShowClass(e)

  forceCheckNum = (rule, value, callback) => {
    const reg = /^[0-9]+$/
    if (reg.test(value)) {
      callback()
    } else {
      callback('必须是数字')
    }
  }

  // 通过不同的 Source Data System 显示不同的 Source Namespace 的内容
  onSourceDataSystemItemSelect = (val) => {
    this.props.onInitJobSourceNs(this.props.projectIdGeted, val, 'sourceType')
  }

  // 通过不同的 Sink Data System 显示不同的 Sink Namespace 的内容
  onSinkDataSystemItemSelect = (val) => {
    this.props.onInitJobSinkNs(this.props.projectIdGeted, val, 'sinkType')
    this.setState({
      sinkConfigClass: (val === 'hbase' || val === 'mysql' || val === 'oracle' || val === 'postgresql') ? 'sink-config-class' : ''
    })
  }

  onChangeStartTs = (value, dateString) => {
    this.props.initStartTS(dateString)
  }

  onChangeEndTs = (value, dateString) => {
    this.props.initEndTS(dateString)
  }

  render () {
    const { step, form, jobMode, fieldSelected, jobTranTableConfirmValue } = this.props
    const { getFieldDecorator } = form
    const { onShowJobTransModal, onShowJobSinkConfigModal } = this.props
    const { jobTransTableSource, onDeleteSingleTransform, onAddTransform, onEditTransform, onUpTransform, onDownTransform } = this.props
    const { jobStepSourceNs, jobStepSinkNs, jobTranTagClassName, jobTranTableClassName } = this.props
    const { sourceTypeNamespaceData, sinkTypeNamespaceData } = this.props
    const { sinkConfigClass } = this.state

    const stepClassNames = [
      step === 0 ? '' : 'hide',
      step === 1 ? '' : 'hide',
      step === 2 ? '' : 'hide'
    ]

    const stepHiddens = [
      false,
      step <= 0,
      step <= 1
    ]

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 17 }
    }

    const sourceDataSystemData = [
      { value: 'kafka', icon: 'icon-kafka', style: {fontSize: '35px'} },
      { value: 'log', text: 'Log' },
      { value: 'file', text: 'File' },
      { value: 'app', text: 'App' },
      { value: 'presto', text: 'Presto' },
      { value: 'mysql', icon: 'icon-mysql' },
      { value: 'oracle', icon: 'icon-amy-db-oracle', style: {lineHeight: '40px'} },
      { value: 'mongodb', icon: 'icon-mongodb', style: {fontSize: '26px'} }
    ]

    const sinkDataSystemData = [
      { value: 'oracle', icon: 'icon-amy-db-oracle', style: {lineHeight: '40px'} },
      { value: 'mysql', icon: 'icon-mysql' },
      { value: 'es', icon: 'icon-elastic', style: {fontSize: '24px'} },
      { value: 'hbase', icon: 'icon-hbase1' },
      { value: 'phoenix', text: 'Phoenix' },
      { value: 'kafka', icon: 'icon-kafka', style: {fontSize: '35px'} },
      { value: 'postgresql', icon: 'icon-postgresql', style: {fontSize: '31px'} },
      { value: 'cassandra', icon: 'icon-cass', style: {fontSize: '52px', lineHeight: '60px'} },
      { value: 'mongodb', icon: 'icon-mongodb', style: {fontSize: '26px'} }
    ]

    let formValues = ''
    formValues = this.props.form.getFieldsValue([
      'jobName',
      'type',
      // Spark Configs
      // 'eventStartTs',
      // 'eventEndTs',
      'sourceDataSystem',
      'sourceNamespace',
      'sinkDataSystem',
      'sinkNamespace',
      'maxRecordPerPartitionProcessed'
      // 'sinkConfig'
    ])

    const step3ConfirmDSNS = Object.keys(formValues).map(key => (
      <Col span={24} key={key}>
        <div className="ant-row ant-form-item">
          <Row>
            <Col span={8} className="ant-form-item-label">
              <label htmlFor="#">{prettyShownText(key)}</label>
            </Col>
            <Col span={15} className="value-font-style">
              <div className="ant-form-item-control" style={{font: 'bolder'}}>
                <p>
                  {Object.prototype.toString.call(formValues[key]) === '[object Array]'
                    ? formValues[key].join('.')
                    : formValues[key]}
                </p>
              </div>
            </Col>
          </Row>
        </div>
      </Col>
    ))

    const sinkConfigTag = form.getFieldValue('sinkConfig')
      ? (
        <Tag color="#7CB342" onClick={onShowJobSinkConfigModal}>
          <Icon type="check-circle-o" /> 点击修改
        </Tag>
      )
      : (
        <Tag onClick={onShowJobSinkConfigModal}>
          <Icon type="minus-circle-o" /> 点击修改
        </Tag>
      )

    const columns = [{
      title: 'Num',
      dataIndex: 'order',
      key: 'order',
      width: '12%'
    }, {
      title: 'Config Info',
      dataIndex: 'transformConfigInfo',
      key: 'transformConfigInfo',
      width: '65%'
    }, {
      title: 'Transform Config Info Request',
      dataIndex: 'transformConfigInfoRequest',
      key: 'transformConfigInfoRequest',
      className: 'hide'
    }, {
      title: 'Pushdown Connection',
      dataIndex: 'pushdownConnection',
      key: 'pushdownConnection',
      className: 'hide'
    }, {
      title: 'Action',
      key: 'action',
      render: (text, record) => {
        const transformUpHide = record.order === 1 ? 'hide' : ''
        const transformDownHide = record.order === jobTransTableSource.length ? 'hide' : ''

        return (
          <span className="ant-table-action-column">
            <Tooltip title="编辑">
              <Button icon="edit" shape="circle" type="ghost" onClick={onEditTransform(record)}></Button>
            </Tooltip>

            <Tooltip title="添加">
              <Button shape="circle" type="ghost" onClick={onAddTransform(record)}>
                <i className="iconfont icon-jia"></i>
              </Button>
            </Tooltip>

            <Popconfirm placement="bottom" title="确定删除吗？" okText="Yes" cancelText="No" onConfirm={onDeleteSingleTransform(record)}>
              <Tooltip title="删除">
                <Button shape="circle" type="ghost">
                  <i className="iconfont icon-jian"></i>
                </Button>
              </Tooltip>
            </Popconfirm>

            <Tooltip title="向上">
              <Button shape="circle" type="ghost" onClick={onUpTransform(record)} className={transformUpHide}>
                <i className="iconfont icon-up"></i>
              </Button>
            </Tooltip>

            <Tooltip title="向下">
              <Button shape="circle" type="ghost" onClick={onDownTransform(record)} className={transformDownHide}>
                <i className="iconfont icon-down"></i>
              </Button>
            </Tooltip>
          </span>
        )
      }
    }]

    const pagination = {
      defaultPageSize: 5,
      pageSizeOptions: ['5', '10', '15'],
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

    const { jobResultFieldsValue, sparkConfigCheck, onShowSparkConfigModal } = this.props

    const jobConfigTag = sparkConfigCheck === true
      ? (
        <Tag color="#7CB342" onClick={onShowSparkConfigModal}>
          <Icon type="check-circle-o" /> 点击修改
        </Tag>
      )
      : (
        <Tag onClick={onShowSparkConfigModal}>
          <Icon type="minus-circle-o" /> 点击修改
        </Tag>
      )

    const warningMsg = (
      <span>
        Spark Configs
        <Tooltip title="帮助" placement="bottom">
          <Popover
            placement="top"
            content={<div style={{ width: '200px', height: '25px' }}>
              <p>Dirver / Execotor 资源配置</p>
            </div>}
            title={<h3>帮助</h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    return (
      <Form className="ri-workbench-form workbench-flow-form">
        {/* Step 1 */}
        <Row gutter={8} className={stepClassNames[0]}>
          <Card title="" className="ri-workbench-form-card-style stream-card">
            <Col span={24}>
              <FormItem label="Name" {...itemStyle}>
                {getFieldDecorator('jobName', {
                  rules: [{
                    required: true,
                    message: 'Name 不能为空'
                  }, {
                    validator: this.forceCheckSave
                  }]
                })(
                  <Input placeholder="Name" onChange={this.onNameInputChange} disabled={jobMode === 'edit'} />
                )}
              </FormItem>
            </Col>

            <Col span={24}>
              <FormItem label="Type" {...itemStyle}>
                {getFieldDecorator('type', {
                  rules: [{
                    required: true,
                    message: '请选择 Type'
                  }]
                })(
                  <RadioGroup className="radio-group-style" size="default" disabled={jobMode === 'edit'}>
                    <RadioButton value="hdfs_txt" className="radio-btn-style radio-btn-extra">Hdfslog</RadioButton>
                  </RadioGroup>
                )}
              </FormItem>
            </Col>

            <Col span={24}>
              <div className="ant-col-6 ant-form-item-label">
                <label htmlFor="#" className="sink-config-class">{warningMsg}</label>
              </div>
              <div className="ant-col-17">
                <div className="ant-form-item-control">
                  {jobConfigTag}
                </div>
              </div>
            </Col>
            <Col span={24} className="hide">
              <FormItem>
                {getFieldDecorator('config', {})(<Input />)}
              </FormItem>
            </Col>
          </Card>
          <Card title="Time Range" className="ri-workbench-form-card-style stream-card">
            <Col span={24}>
              <FormItem label="Event Start Ts" {...itemStyle}>
                {getFieldDecorator('eventStartTs', {})(
                  <DatePicker
                    showTime
                    format="YYYY-MM-DD HH:mm:ss"
                    placeholder="Select Start Time"
                    onChange={this.onChangeStartTs}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={24}>
              <FormItem label="Event End Ts" {...itemStyle}>
                {getFieldDecorator('eventEndTs', {})(
                  <DatePicker
                    showTime
                    format="YYYY-MM-DD HH:mm:ss"
                    placeholder="Select End Time"
                    onChange={this.onChangeEndTs}
                  />
                )}
              </FormItem>
            </Col>

          </Card>
          <Card title="Source" className="ri-workbench-form-card-style source-card">
            <Col span={24}>
              <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
                {getFieldDecorator('sourceDataSystem', {
                  rules: [{
                    required: true,
                    message: '请选择 Data System'
                  }]
                })(
                  <DataSystemSelector
                    data={sourceDataSystemData}
                    onItemSelect={this.onSourceDataSystemItemSelect}
                    dataSystemDisabled={jobMode === 'edit'}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={24}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('sourceNamespace', {
                  rules: [{
                    required: true,
                    message: '请选择 Namespace'
                  }]
                })(
                  <Cascader
                    disabled={jobMode === 'edit'}
                    placeholder="Select a Source Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={sourceTypeNamespaceData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                  />
                )}
              </FormItem>
            </Col>
          </Card>

          <Card title="Sink" className="ri-workbench-form-card-style sink-card">
            <Col span={24}>
              <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
                {getFieldDecorator('sinkDataSystem', {
                  rules: [{
                    required: true,
                    message: '请选择 Data System'
                  }]
                })(
                  <DataSystemSelector
                    data={sinkDataSystemData}
                    onItemSelect={this.onSinkDataSystemItemSelect}
                    dataSystemDisabled={jobMode === 'edit'}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={24}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('sinkNamespace', {
                  rules: [{
                    required: true,
                    message: '请选择 Namespace'
                  }]
                })(
                  <Cascader
                    disabled={jobMode === 'edit'}
                    placeholder="Select a Sink Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={sinkTypeNamespaceData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                  />
                )}
              </FormItem>
            </Col>

            <Col span={24} className="result-field-class">
              <FormItem label="Result Fields" {...itemStyle}>
                {getFieldDecorator('resultFields', {
                  rules: [{
                    required: true,
                    message: '请选择 Result Fields'
                  }],
                  hidden: stepHiddens[1]
                })(
                  <RadioGroup className="radio-group-style" onChange={this.onAllOrNotSelect} size="default">
                    <RadioButton value="all" className="radio-btn-style fradio-btn-extra">All</RadioButton>
                    <RadioButton value="selected" className="radio-btn-style radio-btn-extra">Selected</RadioButton>
                  </RadioGroup>
                )}
              </FormItem>
            </Col>
            <Col span={6}></Col>
            <Col span={17} className={`${fieldSelected}`}>
              <FormItem>
                {getFieldDecorator('resultFieldsSelected', {
                  hidden: stepHiddens[1]
                })(
                  <Input type="textarea" placeholder="Result Fields 多条时以英文逗号分隔" autosize={{ minRows: 2, maxRows: 6 }} />
                )}
              </FormItem>
            </Col>

            <Col span={24}>
              <FormItem label="Batch Record Num" {...itemStyle}>
                {getFieldDecorator('maxRecordPerPartitionProcessed', {
                  rules: [{
                    required: true,
                    message: '请填写 Batch Record Num'
                  }, {
                    validator: this.forceCheckNum
                  }],
                  initialValue: 5000,
                  hidden: stepHiddens[1]
                })(
                  <InputNumber step={10} className="max-record-class" />
                )}
              </FormItem>
            </Col>

            <Col span={24} style={{marginBottom: '8px'}}>
              <div className="ant-col-6 ant-form-item-label">
                <label htmlFor="#" className={sinkConfigClass}>Sink Config</label>
              </div>
              <div className="ant-col-17">
                <div className="ant-form-item-control">
                  {sinkConfigTag}
                </div>
              </div>
            </Col>
            <Col span={24} className="hide">
              <FormItem>
                {getFieldDecorator('sinkConfig', {
                })(<Input />)}
              </FormItem>
            </Col>
          </Card>
        </Row>
        {/* Step 2 */}
        <Row gutter={8} className={`${stepClassNames[1]}`}>
          <Col span={24}>
            <FormItem label="Source Namespace" {...itemStyle}>
              {getFieldDecorator('jobStepSourceNs', {
                hidden: stepHiddens[1]
              })(
                <p className="value-font-style">{jobStepSourceNs}</p>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Sink Namespace" {...itemStyle}>
              {getFieldDecorator('jobStepSinkNs', {
                hidden: stepHiddens[1]
              })(
                <p className="value-font-style">{jobStepSinkNs}</p>
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Transformation" {...itemStyle}>
              {getFieldDecorator('transformation', {
                hidden: stepHiddens[1]
              })(
                <Tag className={jobTranTagClassName} onClick={onShowJobTransModal}>
                  <Icon type="minus-circle-o" /> 点击修改
                </Tag>
              )}
            </FormItem>
          </Col>

          <Col span={6}></Col>
          <Col span={18} className={jobTranTableClassName}>
            <Table
              // rowKey={jobTransTableSource.order}
              dataSource={jobTransTableSource}
              columns={columns}
              pagination={pagination}
              bordered
              className="tran-table-style"
            />
          </Col>

          {/* <Col span={24}>
            <FormItem label="Specific Config" {...itemStyle}>
              {getFieldDecorator('specialConfig', {
                hidden: stepHiddens[1]
              })(
                <Input type="textarea" placeholder="Specific Config" />
              )}
            </FormItem>
          </Col> */}
        </Row>
        {/* Step 3 */}
        <Row gutter={8} className={`ri-workbench-confirm-step ${stepClassNames[2]}`}>
          {step3ConfirmDSNS}
          <Col span={24}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Result Fields</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{jobResultFieldsValue}</p>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Transformation</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{jobTranTableConfirmValue}</p>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
        </Row>
      </Form>
    )
  }
}

WorkbenchJobForm.propTypes = {
  step: React.PropTypes.number,
  jobTransTableSource: React.PropTypes.array,
  form: React.PropTypes.any,
  projectIdGeted: React.PropTypes.string,
  jobMode: React.PropTypes.string,
  sparkConfigCheck: React.PropTypes.bool,
  jobStepSourceNs: React.PropTypes.string,
  jobStepSinkNs: React.PropTypes.string,
  onShowSparkConfigModal: React.PropTypes.func,
  onInitJobNameValue: React.PropTypes.func,
  jobTranTagClassName: React.PropTypes.string,
  jobTranTableClassName: React.PropTypes.string,

  onShowJobTransModal: React.PropTypes.func,
  onShowJobSinkConfigModal: React.PropTypes.func,
  onDeleteSingleTransform: React.PropTypes.func,
  onAddTransform: React.PropTypes.func,
  onEditTransform: React.PropTypes.func,
  onUpTransform: React.PropTypes.func,
  onDownTransform: React.PropTypes.func,
  sourceTypeNamespaceData: React.PropTypes.array,
  sinkTypeNamespaceData: React.PropTypes.array,
  onInitJobSourceNs: React.PropTypes.func,
  onInitJobSinkNs: React.PropTypes.func,
  jobResultFieldsValue: React.PropTypes.string,
  jobTranTableConfirmValue: React.PropTypes.string,
  fieldSelected: React.PropTypes.string,
  initResultFieldClass: React.PropTypes.func,
  initDataShowClass: React.PropTypes.func,
  initStartTS: React.PropTypes.func,
  initEndTS: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(WorkbenchJobForm)
