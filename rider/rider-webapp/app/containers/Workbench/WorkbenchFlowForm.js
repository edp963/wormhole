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
import Select from 'antd/lib/select'
const Option = Select.Option
import Cascader from 'antd/lib/cascader'
import Popconfirm from 'antd/lib/popconfirm'
import Tooltip from 'antd/lib/tooltip'
import Button from 'antd/lib/button'
import Tag from 'antd/lib/tag'
import Icon from 'antd/lib/icon'
import Table from 'antd/lib/table'
import Card from 'antd/lib/card'
import Radio from 'antd/lib/radio'
const RadioGroup = Radio.Group
const RadioButton = Radio.Button

import { prettyShownText } from '../../utils/util'

export class WorkbenchFlowForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      flowMode: '',
      sinkConfigClass: ''
    }
  }

  componentWillReceiveProps (props) {
    this.setState({ flowMode: props.flowMode })
  }

  onAllOrNotSelect = (e) => this.props.initResultFieldClass(e)

  onShowDataFrame = (e) => this.props.initDataShowClass(e)

  forceCheckDataframeNum = (rule, value, callback) => {
    const reg = /^[0-9]+$/
    if (reg.test(value)) {
      callback()
    } else {
      callback('必须是数字')
    }
  }

  // 通过不同的 Source Data System 显示不同的 Source Namespace 的内容
  onSourceDataSystemItemSelect = (val) => {
    const { streamDiffType, flowMode } = this.props

    if (streamDiffType === 'default') {
      this.props.onInitSourceTypeNamespace(this.props.projectIdGeted, val, 'sourceType')
    } else if (streamDiffType === 'hdfslog') {
      // placeholder 和单条数据回显
      if (flowMode === 'add' || flowMode === 'copy') {
        this.props.form.setFieldsValue({
          hdfslogNamespace: undefined
        })
      }
      this.props.onInitHdfslogNamespace(this.props.projectIdGeted, val, 'sourceType')
    }
  }

  // 通过不同的 Sink Data System 显示不同的 Sink Namespace 的内容
  onSinkDataSystemItemSelect = (val) => {
    this.props.onInitSinkTypeNamespace(this.props.projectIdGeted, val, 'sinkType')
    this.setState({
      sinkConfigClass: (val === 'hbase' || val === 'mysql' || val === 'oracle' || val === 'postgresql') ? 'sink-config-class' : ''
    })
  }

  onStreamJoinSqlConfigTypeSelect = (val) => this.props.onStreamJoinSqlConfigTypeSelect(val)

  onHandleChangeStreamType = (e) => this.props.onInitStreamTypeSelect(e.target.value)

  onHandleChangeStreamName = (val) => this.props.onInitStreamNameSelect(val)

  onHandleHdfslogCascader = (value) => this.props.initialHdfslogCascader(value)

  render () {
    const { step, form, fieldSelected, dataframeShowSelected, streamDiffType, hdfslogSinkDataSysValue, hdfslogSinkNsValue, transformTableConfirmValue, flowKafkaTopicValue } = this.props
    const { getFieldDecorator } = form
    const { onShowTransformModal, onShowEtpStrategyModal, onShowSinkConfigModal } = this.props
    const { transformTableSource, onDeleteSingleTransform, onAddTransform, onEditTransform, onUpTransform, onDownTransform } = this.props
    const { step2SourceNamespace, step2SinkNamespace, etpStrategyCheck, transformTagClassName, transformTableClassName, transConnectClass } = this.props
    const { selectStreamKafkaTopicValue, sourceTypeNamespaceData, hdfslogNsData, sinkTypeNamespaceData } = this.props
    const { flowMode, sinkConfigClass } = this.state

    // edit 时，不能修改部分元素
    let flowDisabledOrNot = false
    if (flowMode === 'add' || flowMode === 'copy') {
      flowDisabledOrNot = false
    } else if (flowMode === 'edit') {
      flowDisabledOrNot = true
    }

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

    const streamTypeClass = [
      streamDiffType === 'default' ? '' : 'hide',
      streamDiffType === 'hdfslog' ? '' : 'hide'
    ]

    const streamTypeHiddens = [
      streamDiffType !== 'default',
      streamDiffType !== 'hdfslog'
    ]

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 17 }
    }

    const itemStyleDFS = {
      labelCol: { span: 9 },
      wrapperCol: { span: 9 }
    }

    const itemStyleDFSN = {
      labelCol: { span: 14 },
      wrapperCol: { span: 10 }
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
    if (streamDiffType === 'default') {
      formValues = this.props.form.getFieldsValue([
        'streamName',
        'streamType'
      ])
    } else if (streamDiffType === 'hdfslog') {
      formValues = this.props.form.getFieldsValue([
        'streamName',
        'streamType'
      ])
    }

    const step3ConfirmContent = Object.keys(formValues).map(key => (
      <Col span={24} key={key}>
        <div className="ant-row ant-form-item">
          <Row>
            <Col span={8} className="ant-form-item-label">
              <label htmlFor="#">{prettyShownText(key)}</label>
            </Col>
            <Col span={15} className="value-font-style">
              <div className="ant-form-item-control">
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

    let formDSNSValues = ''
    if (streamDiffType === 'default') {
      formValues = this.props.form.getFieldsValue([
        'sourceDataSystem',
        'sourceNamespace',
        'sinkDataSystem',
        'sinkNamespace',
        'sinkConfig'
      ])
    }

    const step3ConfirmDSNS = Object.keys(formDSNSValues).map(key => (
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
        <Tag color="#7CB342" onClick={onShowSinkConfigModal}>
          <Icon type="check-circle-o" /> 点击修改
        </Tag>
      )
      : (
        <Tag onClick={onShowSinkConfigModal}>
          <Icon type="minus-circle-o" /> 点击修改
        </Tag>
      )

    const etpStrategyTag = etpStrategyCheck === true
      ? (
        <Tag color="#7CB342" onClick={onShowEtpStrategyModal}>
          <Icon type="check-circle-o" /> 点击修改
        </Tag>
      )
      : (
        <Tag onClick={onShowEtpStrategyModal}>
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
      // width: '23%',
      render: (text, record) => {
        const transformUpHide = record.order === 1 ? 'hide' : ''
        const transformDownHide = record.order === transformTableSource.length ? 'hide' : ''

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

    const streamNameOptions = selectStreamKafkaTopicValue.length === 0
      ? undefined
      : selectStreamKafkaTopicValue.map(s => (<Option key={s.id} value={`${s.id}`}>{s.name}</Option>))

    const { etpStrategyConfirmValue, resultFieldsValue, flowKafkaInstanceValue } = this.props

    return (
      <Form className="ri-workbench-form workbench-flow-form">
        {/* Step 1 */}
        <Row gutter={8} className={stepClassNames[0]}>
          <Card title="Stream" className="ri-workbench-form-card-style stream-card">
            <Row>
              <Col span={24}>
                <FormItem label="Stream Type" {...itemStyle}>
                  {getFieldDecorator('streamType', {
                    rules: [{
                      required: true,
                      message: '请选择 Stream Type'
                    }],
                    initialValue: 'default'
                  })(
                    <RadioGroup className="radio-group-style" onChange={this.onHandleChangeStreamType} size="default">
                      <RadioButton value="default" className="radio-btn-style radio-btn-extra" disabled={flowDisabledOrNot}>Default</RadioButton>
                      <RadioButton value="hdfslog" className="radio-btn-style radio-btn-extra" disabled={flowDisabledOrNot}>Hdfslog</RadioButton>
                      <RadioButton value="routing" className="radio-btn-style" disabled>Routing</RadioButton>
                    </RadioGroup>
                  )}
                </FormItem>
              </Col>
              <Col span={24} className="hide">
                <FormItem label="">
                  {getFieldDecorator('flowStreamId', {})(
                    <Input />
                  )}
                </FormItem>
              </Col>
              <Col span={24}>
                <FormItem label="Stream Name" {...itemStyle}>
                  {getFieldDecorator('streamName', {
                    rules: [{
                      required: true,
                      message: '请选择 Stream Name'
                    }]
                  })(
                    <Select
                      dropdownClassName="ri-workbench-select-dropdown"
                      onSelect={this.onHandleChangeStreamName}
                      placeholder="Select a Stream Name"
                      disabled={flowDisabledOrNot}
                    >
                      {streamNameOptions}
                    </Select>
                  )}
                </FormItem>
              </Col>
            </Row>
            <Col span={24} className="ri-input-text">
              <FormItem label="Kafka Instance" {...itemStyle} >
                {getFieldDecorator('kafkaInstance', {})(
                  <p className="value-font-style">{flowKafkaInstanceValue}</p>
                )}
              </FormItem>
            </Col>
            <Col span={24} className="ri-input-text">
              <FormItem label="Kafka Topics" {...itemStyle}>
                {getFieldDecorator('kafkaTopic', {})(
                  <p className="value-font-style">{flowKafkaTopicValue}</p>
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
                    flowMode={flowMode}
                    data={sourceDataSystemData}
                    onItemSelect={this.onSourceDataSystemItemSelect}
                    dataSystemDisabled={flowDisabledOrNot}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={24} className={streamTypeClass[0]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('sourceNamespace', {
                  rules: [{
                    required: true,
                    message: '请选择 Namespace'
                  }],
                  hidden: streamTypeHiddens[0]
                })(
                  <Cascader
                    disabled={flowDisabledOrNot}
                    placeholder="Select a Source Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={sourceTypeNamespaceData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                    onChange={(labels, options) => {
                      // const sourceNsId = sourceTypeNamespaceData.find(i => i.value === options[options.length - 1].value)
                      // console.log('sourceNsId', sourceNsId)
                      // this.props.form.setFieldsValue({
                      //   sourceNamespaceId: sourceNsId
                      // })
                    }}
                  />
                )}
              </FormItem>
            </Col>

            <Col span={24} className={streamTypeClass[1]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('hdfslogNamespace', {
                  rules: [{
                    required: true,
                    message: '请选择 Namespace'
                  }],
                  hidden: streamTypeHiddens[1]
                })(
                  <Cascader
                    disabled={flowDisabledOrNot}
                    placeholder="Select a Source Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={hdfslogNsData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                    onChange={this.onHandleHdfslogCascader}
                  />
                )}
              </FormItem>
            </Col>

            <Col span={24} className={streamTypeClass[0]}>
              <FormItem label="Protocol" {...itemStyle}>
                {getFieldDecorator('protocol', {
                  rules: [{
                    required: true,
                    message: '请选择 Protocol'
                  }],
                  hidden: streamTypeHiddens[0]
                })(
                  <RadioGroup className="radio-group-style" size="default">
                    <RadioButton value="all" className="radio-btn-style radio-btn-extra">All</RadioButton>
                    <RadioButton value="increment" className="radio-btn-style radio-btn-extra">Increment</RadioButton>
                    <RadioButton value="initial" className="radio-btn-style radio-btn-extra">Initial</RadioButton>
                  </RadioGroup>
                )}
              </FormItem>
            </Col>
          </Card>

          <Card title="Sink" className="ri-workbench-form-card-style sink-card">
            <Col span={24} className={streamTypeClass[0]}>
              <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
                {getFieldDecorator('sinkDataSystem', {
                  rules: [{
                    required: true,
                    message: '请选择 Data System'
                  }],
                  hidden: streamTypeHiddens[0]
                })(
                  <DataSystemSelector
                    flowMode={flowMode}
                    data={sinkDataSystemData}
                    onItemSelect={this.onSinkDataSystemItemSelect}
                    dataSystemDisabled={flowDisabledOrNot}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={24} className={streamTypeClass[0]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('sinkNamespace', {
                  rules: [{
                    required: true,
                    message: '请选择 Namespace'
                  }],
                  hidden: streamTypeHiddens[0]
                })(
                  <Cascader
                    disabled={flowDisabledOrNot}
                    placeholder="Select a Sink Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={sinkTypeNamespaceData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                    // onChange={(labels, options) => {
                    //   const { topicName, topicId, type } = options[options.length - 1]
                    //   this.props.form.setFieldsValue({
                    //     sourceTopicName: topicName,
                    //     sourceTopicId: topicId,
                    //     sourceType: type
                    //   })
                    // }}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={24} className={streamTypeClass[0]} style={{marginBottom: '8px'}}>
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
                  hidden: streamTypeHiddens[0]
                })(<Input />)}
              </FormItem>
            </Col>

            <Col span={24} className={`ri-input-text ${streamTypeClass[1]}`}>
              <FormItem label="Date System" {...itemStyle}>
                {getFieldDecorator('hdfslogDataSys', {
                  hidden: streamTypeHiddens[1]
                })(
                  <p className="value-font-style">{hdfslogSinkDataSysValue}</p>
                )}
              </FormItem>
            </Col>
            <Col span={24} className={`ri-input-text ${streamTypeClass[1]}`}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('hdfslogSinkNs', {
                  hidden: streamTypeHiddens[1]
                })(
                  <p className="value-font-style">{hdfslogSinkNsValue}</p>
                )}
              </FormItem>
            </Col>

          </Card>
        </Row>
        {/* Step 2 */}
        <Row gutter={8} className={`${stepClassNames[1]} ${streamTypeClass[0]}`}>
          <Col span={24}>
            <FormItem label="Source Namespace" {...itemStyle}>
              {getFieldDecorator('step2SourceNamespace', {
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <p className="value-font-style">{step2SourceNamespace}</p>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Sink Namespace" {...itemStyle}>
              {getFieldDecorator('step2SinkNamespace', {
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <p className="value-font-style">{step2SinkNamespace}</p>
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
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <RadioGroup className="radio-group-style" onChange={this.onAllOrNotSelect} size="default">
                  <RadioButton value="all" className="radio-btn-style fradio-btn-extra">All</RadioButton>
                  <RadioButton value="selected" className="radio-btn-style radio-btn-extra">Selected</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
          <Col span={6}></Col>
          <Col span={17} className={fieldSelected}>
            <FormItem>
              {getFieldDecorator('resultFieldsSelected', {
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <Input type="textarea" placeholder="Result Fields 多条时以英文逗号分隔" autosize={{ minRows: 5, maxRows: 8 }} />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Transformation" {...itemStyle}>
              {getFieldDecorator('transformation', {
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <Tag className={transformTagClassName} onClick={onShowTransformModal}>
                  <Icon type="minus-circle-o" /> 点击修改
                </Tag>
              )}
            </FormItem>
          </Col>

          <Col span={6}></Col>
          <Col span={18} className={transformTableClassName}>
            <Table
              // rowKey={transformTableSource.order}
              dataSource={transformTableSource}
              columns={columns}
              pagination={pagination}
              bordered
              className="tran-table-style"
            />
          </Col>

          <Col span={24} className={transConnectClass}>
            <div className="ant-col-6 ant-form-item-label">
              <label htmlFor="#">Event Time Processing</label>
            </div>
            <div className="ant-col-17">
              <div className="ant-form-item-control">
                {etpStrategyTag}
              </div>
            </div>
          </Col>
          <Col span={24} className="hide">
            <FormItem>
              {getFieldDecorator('etpStrategy', {})(<Input />)}
            </FormItem>
          </Col>

          <Col span={16} className={`ds-class ${transConnectClass}`}>
            <FormItem label="Sample Show" {...itemStyleDFS}>
              {getFieldDecorator('dataframeShow', {
                rules: [{
                  required: true,
                  message: '请选择 Dataframe Show'
                }],
                hidden: stepHiddens[1] || transformTableClassName || streamTypeHiddens[0]
              })(
                <RadioGroup className="radio-group-style" onChange={this.onShowDataFrame} size="default">
                  <RadioButton value="false" className="radio-btn-style radio-btn-extra">False</RadioButton>
                  <RadioButton value="true" className="radio-btn-style">True</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </Col>
          <Col span={7} className={`ds-class ${dataframeShowSelected}`}>
            <FormItem label="Number" {...itemStyleDFSN}>
              {getFieldDecorator('dataframeShowNum', {
                rules: [{
                  required: true,
                  message: '请填写 Number'
                }, {
                  validator: this.forceCheckDataframeNum
                }],
                initialValue: 10,
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <InputNumber min={10} step={10} />
              )}
            </FormItem>
          </Col>

          <Col span={24} className="hide">
            <FormItem label="Swifts Specific Config" {...itemStyle}>
              {getFieldDecorator('swiftsSpecificConfig', {
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <Input placeholder="Swifts Specific Config" />
              )}
            </FormItem>
          </Col>
        </Row>
        {/* Step 3 */}
        <Row gutter={8} className={`ri-workbench-confirm-step ${stepClassNames[2]}`}>
          {step3ConfirmContent}
          <Col span={24}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Kafka Instance</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{flowKafkaInstanceValue}</p>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Kafka Topics</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{flowKafkaTopicValue}</p>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          {step3ConfirmDSNS}
          <Col span={24} className={streamTypeClass[0]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Result Fields</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{resultFieldsValue}</p>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[0]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Transformation</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{transformTableConfirmValue}</p>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={`${transConnectClass} ${streamTypeClass[0]}`}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Event Time Processing</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{etpStrategyConfirmValue}</p>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={`${transConnectClass} ${streamTypeClass[0]}`}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Dataframe Show</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{this.props.dataframeShowNumValue}</p>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>

          <Col span={24} className={streamTypeClass[1]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Source Data System</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{hdfslogSinkDataSysValue}</p>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[1]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Source Namespace</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{hdfslogSinkNsValue}</p>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[1]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Sink Data System</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{hdfslogSinkDataSysValue}</p>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[1]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Sink Namespace</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <p className="value-font-style">{hdfslogSinkNsValue}</p>
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

WorkbenchFlowForm.propTypes = {
  step: React.PropTypes.number,
  transformTableSource: React.PropTypes.array,
  form: React.PropTypes.any,
  projectIdGeted: React.PropTypes.string,
  flowMode: React.PropTypes.string,
  onStreamJoinSqlConfigTypeSelect: React.PropTypes.func,
  onShowTransformModal: React.PropTypes.func,
  onShowEtpStrategyModal: React.PropTypes.func,
  onShowSinkConfigModal: React.PropTypes.func,
  onDeleteSingleTransform: React.PropTypes.func,
  onAddTransform: React.PropTypes.func,
  onEditTransform: React.PropTypes.func,
  onUpTransform: React.PropTypes.func,
  onDownTransform: React.PropTypes.func,
  step2SourceNamespace: React.PropTypes.string,
  step2SinkNamespace: React.PropTypes.string,
  etpStrategyCheck: React.PropTypes.bool,
  transformTagClassName: React.PropTypes.string,
  transformTableClassName: React.PropTypes.string,
  transConnectClass: React.PropTypes.string,
  selectStreamKafkaTopicValue: React.PropTypes.array,
  sourceTypeNamespaceData: React.PropTypes.array,
  hdfslogNsData: React.PropTypes.array,
  sinkTypeNamespaceData: React.PropTypes.array,
  onInitSourceTypeNamespace: React.PropTypes.func,
  onInitHdfslogNamespace: React.PropTypes.func,
  onInitSinkTypeNamespace: React.PropTypes.func,
  onInitStreamNameSelect: React.PropTypes.func,
  resultFieldsValue: React.PropTypes.string,
  dataframeShowNumValue: React.PropTypes.string,
  etpStrategyConfirmValue: React.PropTypes.string,
  transformTableConfirmValue: React.PropTypes.string,
  fieldSelected: React.PropTypes.string,
  dataframeShowSelected: React.PropTypes.string,
  streamDiffType: React.PropTypes.string,
  hdfslogSinkDataSysValue: React.PropTypes.string,
  hdfslogSinkNsValue: React.PropTypes.string,
  initResultFieldClass: React.PropTypes.func,
  initDataShowClass: React.PropTypes.func,
  onInitStreamTypeSelect: React.PropTypes.func,
  initialHdfslogCascader: React.PropTypes.func,
  flowKafkaTopicValue: React.PropTypes.string,
  flowKafkaInstanceValue: React.PropTypes.string
}

export default Form.create({wrappedComponentRef: true})(WorkbenchFlowForm)
