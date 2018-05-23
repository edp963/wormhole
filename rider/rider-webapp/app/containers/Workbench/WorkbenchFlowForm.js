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

import DataSystemSelector from '../../components/DataSystemSelector'

import {
  prettyShownText, uuid, forceCheckNum, operateLanguageSelect, operateLanguageFillIn
} from '../../utils/util'
import { sourceDataSystemData, sinkDataSystemData } from '../../components/DataSystemSelector/dataSystemFunction'

export class WorkbenchFlowForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      flowMode: '',
      sinkConfigClass: ''
    }
  }

  componentWillReceiveProps (props) {
    this.setState({
      flowMode: props.flowMode
    })
    if (props.transformTableSource) {
      props.transformTableSource.map(s => {
        s.key = uuid()
        return s
      })
    }
  }

  // 通过不同的 Source Data System 显示不同的 Source Namespace 的内容
  onSourceDataSystemItemSelect = (val) => {
    const { streamDiffType, flowMode, projectIdGeted } = this.props
    if (val) {
      switch (streamDiffType) {
        case 'default':
          this.props.onInitSourceTypeNamespace(projectIdGeted, val, 'sourceType')
          break
        case 'hdfslog':
          // placeholder 和单条数据回显
          if (flowMode === 'add' || flowMode === 'copy') {
            this.props.form.setFieldsValue({ hdfslogNamespace: undefined })
          }
          this.props.onInitHdfslogNamespace(projectIdGeted, val, 'sourceType')
          break
        case 'routing':
          this.props.onInitRoutingNamespace(projectIdGeted, val, 'sourceType')
          break
      }
    }
  }

  // 通过不同的 Sink Data System 显示不同的 Sink Namespace 的内容
  onSinkDataSystemItemSelect = (val) => {
    if (val) {
      this.props.onInitSinkTypeNamespace(this.props.projectIdGeted, val, 'sinkType')
    }
    this.setState({
      sinkConfigClass: val === 'hbase' ? 'sink-config-class' : ''
    })
    if (this.state.flowMode !== 'edit') {
      this.props.form.setFieldsValue({ sinkConfig: '' })
    }
  }

  render () {
    const {
      step, form, fieldSelected, dataframeShowSelected, streamDiffType,
      hdfslogSinkDataSysValue, hdfslogSinkNsValue, routingSourceNsValue,
      routingSinkNsValue, transformTableConfirmValue, flowKafkaTopicValue,
      onShowTransformModal, onShowEtpStrategyModal, onShowSinkConfigModal, onShowSpecialConfigModal,
      transformTableSource, onDeleteSingleTransform, onAddTransform, onEditTransform, onUpTransform, onDownTransform,
      step2SourceNamespace, step2SinkNamespace, etpStrategyCheck, transformTagClassName, transformTableClassName, transConnectClass,
      selectStreamKafkaTopicValue, sourceTypeNamespaceData, hdfslogNsData, routingNsData, sinkTypeNamespaceData, routingSinkTypeNsData,
      initResultFieldClass, initDataShowClass, onInitStreamTypeSelect, onInitStreamNameSelect,
      initialHdfslogCascader, initialRoutingCascader, initialRoutingSinkCascader
    } = this.props

    const { getFieldDecorator } = form

    const { flowMode, sinkConfigClass } = this.state

    // edit 时，不能修改部分元素
    const flowDisabledOrNot = flowMode === 'edit'

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
      streamDiffType === 'hdfslog' ? '' : 'hide',
      streamDiffType === 'routing' ? '' : 'hide'
    ]

    const streamTypeHiddens = [
      streamDiffType !== 'default',
      streamDiffType !== 'hdfslog',
      streamDiffType !== 'routing'
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

    let formValues = this.props.form.getFieldsValue([
      'streamName',
      'streamType'
    ])

    const step3ConfirmContent = Object.keys(formValues).map(key => (
      <Col span={24} key={key}>
        <div className="ant-row ant-form-item">
          <Row>
            <Col span={8} className="ant-form-item-label">
              <label htmlFor="#">{prettyShownText(key)}</label>
            </Col>
            <Col span={15} className="value-font-style">
              <div className="ant-form-item-control">
                <strong>
                  {Object.prototype.toString.call(formValues[key]) === '[object Array]'
                    ? formValues[key].join('.')
                    : formValues[key]}
                </strong>
              </div>
            </Col>
          </Row>
        </div>
      </Col>
    ))

    let formDSNSValues = ''
    if (streamDiffType === 'default') {
      formDSNSValues = this.props.form.getFieldsValue([
        'sourceDataSystem',
        'sourceNamespace',
        'sinkDataSystem',
        'sinkNamespace',
        'sinkConfig'
      ])
    } else if (streamDiffType === 'routing') {
      formDSNSValues = this.props.form.getFieldsValue([
        'sourceDataSystem'
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
                <strong>
                  {Object.prototype.toString.call(formDSNSValues[key]) === '[object Array]'
                    ? formDSNSValues[key].join('.')
                    : formDSNSValues[key]}
                </strong>
              </div>
            </Col>
          </Row>
        </div>
      </Col>
    ))

    const sinkConfigColor = (
      <Tag color="#7CB342" onClick={onShowSinkConfigModal}>
        <Icon type="check-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
      </Tag>
    )
    const sinkConfigNoColor = (
      <Tag onClick={onShowSinkConfigModal}>
        <Icon type="minus-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
      </Tag>
    )

    const sinkConfigTag = flowMode === 'copy'
      ? this.props.sinkConfigCopy ? sinkConfigColor : sinkConfigNoColor
      : form.getFieldValue('sinkConfig') ? sinkConfigColor : sinkConfigNoColor

    const flowSpecialConfigTag = form.getFieldValue('flowSpecialConfig')
      ? (
        <Tag color="#7CB342" onClick={onShowSpecialConfigModal}>
          <Icon type="check-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
        </Tag>
      )
      : (
        <Tag onClick={onShowSpecialConfigModal}>
          <Icon type="minus-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
        </Tag>
      )

    const etpStrategyTag = etpStrategyCheck
      ? (
        <Tag color="#7CB342" onClick={onShowEtpStrategyModal}>
          <Icon type="check-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
        </Tag>
      )
      : (
        <Tag onClick={onShowEtpStrategyModal}>
          <Icon type="minus-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
        </Tag>
      )

    const columns = [{
      title: 'Num',
      dataIndex: 'order',
      key: 'order',
      width: '12%'
    }, {
      title: 'Config Info',
      dataIndex: 'tranConfigInfoSql',
      key: 'tranConfigInfoSql',
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
        const transformDownHide = record.order === transformTableSource.length ? 'hide' : ''
        const addFormat = <FormattedMessage {...messages.workbenchTransAdd} />
        const modifyFormat = <FormattedMessage {...messages.workbenchTransModify} />
        const deleteFormat = <FormattedMessage {...messages.workbenchTransDelete} />
        const sureDeleteFormat = <FormattedMessage {...messages.workbenchTransSureDelete} />
        const upFormat = <FormattedMessage {...messages.workbenchTransUp} />
        const downFormat = <FormattedMessage {...messages.workbenchTransDown} />

        return (
          <span className="ant-table-action-column">
            <Tooltip title={modifyFormat}>
              <Button icon="edit" shape="circle" type="ghost" onClick={onEditTransform(record)}></Button>
            </Tooltip>

            <Tooltip title={addFormat}>
              <Button shape="circle" type="ghost" onClick={onAddTransform(record)}>
                <i className="iconfont icon-jia"></i>
              </Button>
            </Tooltip>

            <Popconfirm placement="bottom" title={sureDeleteFormat} okText="Yes" cancelText="No" onConfirm={onDeleteSingleTransform(record)}>
              <Tooltip title={deleteFormat}>
                <Button shape="circle" type="ghost">
                  <i className="iconfont icon-jian"></i>
                </Button>
              </Tooltip>
            </Popconfirm>

            <Tooltip title={upFormat}>
              <Button shape="circle" type="ghost" onClick={onUpTransform(record)} className={transformUpHide}>
                <i className="iconfont icon-up"></i>
              </Button>
            </Tooltip>

            <Tooltip title={downFormat}>
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
        this.setState({ pageIndex: current })
      }
    }

    const streamNameOptions = selectStreamKafkaTopicValue.length === 0
      ? undefined
      : selectStreamKafkaTopicValue.map(s => (<Option key={s.id} value={`${s.name}`}>{s.name}</Option>))

    const { etpStrategyConfirmValue, transConfigConfirmValue, resultFieldsValue, flowKafkaInstanceValue } = this.props

    return (
      <Form className="ri-workbench-form workbench-flow-form">
        {/* Step 1 */}
        <Row gutter={8} className={stepClassNames[0]}>
          <Card title="Stream" className="ri-workbench-form-card-style stream-card">
            <Col span={24}>
              <FormItem label="Stream Type" {...itemStyle}>
                {getFieldDecorator('streamType', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('stream type', 'Stream Type')
                  }],
                  initialValue: 'default'
                })(
                  <RadioGroup className="radio-group-style" onChange={(e) => onInitStreamTypeSelect(e.target.value)} size="default">
                    <RadioButton value="default" className="radio-btn-style radio-btn-extra" disabled={flowDisabledOrNot}>Default</RadioButton>
                    <RadioButton value="hdfslog" className="radio-btn-style radio-btn-extra" disabled={flowDisabledOrNot}>Hdfslog</RadioButton>
                    <RadioButton value="routing" className="radio-btn-style" disabled={flowDisabledOrNot}>Routing</RadioButton>
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
                    message: operateLanguageSelect('stream name', 'Stream Name')
                  }]
                })(
                  <Select
                    dropdownClassName="ri-workbench-select-dropdown"
                    onChange={(e) => onInitStreamNameSelect(e)}
                    placeholder="Select a Stream Name"
                    disabled={flowDisabledOrNot}
                  >
                    {streamNameOptions}
                  </Select>
                )}
              </FormItem>
            </Col>
            <Col span={24} className="ri-input-text">
              <FormItem label="Kafka Instance" {...itemStyle} >
                {getFieldDecorator('kafkaInstance', {})(
                  <strong className="value-font-style">{flowKafkaInstanceValue}</strong>
                )}
              </FormItem>
            </Col>
            <Col span={24} className="ri-input-text">
              <FormItem label="Exist Kafka Topics" {...itemStyle}>
                {getFieldDecorator('kafkaTopic', {})(
                  <strong className="value-font-style">{flowKafkaTopicValue}</strong>
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
                    message: operateLanguageSelect('data system', 'Data System')
                  }]
                })(
                  <DataSystemSelector
                    flowMode={flowMode}
                    data={sourceDataSystemData()}
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
                    message: operateLanguageSelect('namespace', 'Namespace')
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
                  />
                )}
              </FormItem>
            </Col>

            <Col span={24} className={streamTypeClass[1]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('hdfslogNamespace', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('namespace', 'Namespace')
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
                    onChange={(e) => initialHdfslogCascader(e)}
                  />
                )}
              </FormItem>
            </Col>

            <Col span={24} className={streamTypeClass[2]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('routingNamespace', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('namespace', 'Namespace')
                  }],
                  hidden: streamTypeHiddens[2]
                })(
                  <Cascader
                    disabled={flowDisabledOrNot}
                    placeholder="Select a Source Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={routingNsData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                    onChange={(e) => initialRoutingCascader(e)}
                  />
                )}
              </FormItem>
            </Col>

            <Col span={24} className={streamTypeClass[0]}>
              <FormItem label="Protocol" {...itemStyle}>
                {getFieldDecorator('protocol', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('protocol', 'Protocol')
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
                    message: operateLanguageSelect('data system', 'Data System')
                  }],
                  hidden: streamTypeHiddens[0]
                })(
                  <DataSystemSelector
                    flowMode={flowMode}
                    data={sinkDataSystemData()}
                    onItemSelect={this.onSinkDataSystemItemSelect}
                    dataSystemDisabled={flowDisabledOrNot}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={24} className={streamTypeClass[2]}>
              <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
                {getFieldDecorator('routingSinkDataSystem', {
                  hidden: streamTypeHiddens[2]
                })(
                  <p>kafka</p>
                )}
              </FormItem>
            </Col>
            <Col span={24} className={streamTypeClass[0]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('sinkNamespace', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('namespace', 'Namespace')
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
                  />
                )}
              </FormItem>
            </Col>
            <Col span={24} className={streamTypeClass[2]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('routingSinkNs', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('namespace', 'Namespace')
                  }],
                  hidden: streamTypeHiddens[2]
                })(
                  <Cascader
                    disabled={flowDisabledOrNot}
                    placeholder="Select a Sink Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={routingSinkTypeNsData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                    onChange={(e) => initialRoutingSinkCascader(e)}
                  />
                )}
              </FormItem>
            </Col>

            <Col span={24} className={`result-field-class ${streamDiffType === 'default' ? '' : 'hide'}`}>
              <FormItem label="Result Fields" {...itemStyle}>
                {getFieldDecorator('resultFields', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('result fields', 'Result Fields')
                  }],
                  hidden: stepHiddens[1] || streamTypeHiddens[0]
                })(
                  <RadioGroup className="radio-group-style" onChange={(e) => initResultFieldClass(e.target.value)} size="default">
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
                  hidden: stepHiddens[1] || streamTypeHiddens[0]
                })(
                  <Input type="textarea" placeholder="Result Fields 多条时以英文逗号分隔" autosize={{ minRows: 2, maxRows: 6 }} />
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
              <FormItem label="Data System" {...itemStyle}>
                {getFieldDecorator('hdfslogDataSys', {
                  hidden: streamTypeHiddens[1]
                })(
                  <strong className="value-font-style">{hdfslogSinkDataSysValue}</strong>
                )}
              </FormItem>
            </Col>
            <Col span={24} className={`ri-input-text ${streamTypeClass[1]}`}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('hdfslogSinkNs', {
                  hidden: streamTypeHiddens[1]
                })(
                  <strong className="value-font-style">{hdfslogSinkNsValue}</strong>
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
                <strong className="value-font-style">{step2SourceNamespace}</strong>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Sink Namespace" {...itemStyle}>
              {getFieldDecorator('step2SinkNamespace', {
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <strong className="value-font-style">{step2SinkNamespace}</strong>
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Transformation" {...itemStyle}>
              {getFieldDecorator('transformation', {
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <Tag className={transformTagClassName} onClick={onShowTransformModal}>
                  <Icon type="minus-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
                </Tag>
              )}
            </FormItem>
          </Col>

          <Col span={6}></Col>
          <Col span={18} className={transformTableClassName}>
            <Table
              dataSource={transformTableSource}
              columns={columns}
              pagination={pagination}
              bordered
              className="tran-table-style"
            />
          </Col>

          <Col span={24} className={transConnectClass} style={{marginBottom: '8px'}}>
            <div className="ant-col-6 ant-form-item-label">
              <label htmlFor="#">Transformation Config</label>
            </div>
            <div className="ant-col-17">
              <div className="ant-form-item-control">
                {flowSpecialConfigTag}
              </div>
            </div>
          </Col>
          <Col span={24} className="hide">
            <FormItem>
              {getFieldDecorator('flowSpecialConfig', {
                hidden: streamTypeHiddens[0]
              })(<Input />)}
            </FormItem>
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
                  message: operateLanguageSelect('sample show', 'Sample Show')
                }],
                hidden: stepHiddens[1] || transformTableClassName || streamTypeHiddens[0]
              })(
                <RadioGroup className="radio-group-style" onChange={(e) => initDataShowClass(e.target.value)} size="default">
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
                  message: operateLanguageFillIn('number', 'Number')
                }, {
                  validator: forceCheckNum
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
                    <strong className="value-font-style">{flowKafkaInstanceValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Exist Kafka Topics</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{flowKafkaTopicValue}</strong>
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
                    <strong className="value-font-style">{resultFieldsValue}</strong>
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
                    <strong className="value-font-style">{transformTableConfirmValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[0]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Transformation Config</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{transConfigConfirmValue}</strong>
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
                    <strong className="value-font-style">{etpStrategyConfirmValue}</strong>
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
                    <strong className="value-font-style">{this.props.dataframeShowNumValue}</strong>
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
                    <strong className="value-font-style">{hdfslogSinkDataSysValue}</strong>
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
                    <strong className="value-font-style">{hdfslogSinkNsValue}</strong>
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
                    <strong className="value-font-style">{hdfslogSinkDataSysValue}</strong>
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
                    <strong className="value-font-style">{hdfslogSinkNsValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[2]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Source Namespace</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{routingSourceNsValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[2]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Sink Data System</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">kafka</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[2]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Sink Namespace</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{routingSinkNsValue}</strong>
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
  step: PropTypes.number,
  transformTableSource: PropTypes.array,
  form: PropTypes.any,
  projectIdGeted: PropTypes.string,
  flowMode: PropTypes.string,
  onShowTransformModal: PropTypes.func,
  onShowEtpStrategyModal: PropTypes.func,
  onShowSinkConfigModal: PropTypes.func,
  onShowSpecialConfigModal: PropTypes.func,
  onDeleteSingleTransform: PropTypes.func,
  onAddTransform: PropTypes.func,
  onEditTransform: PropTypes.func,
  onUpTransform: PropTypes.func,
  onDownTransform: PropTypes.func,
  step2SourceNamespace: PropTypes.string,
  step2SinkNamespace: PropTypes.string,
  etpStrategyCheck: PropTypes.bool,
  transformTagClassName: PropTypes.string,
  transformTableClassName: PropTypes.string,
  transConnectClass: PropTypes.string,
  selectStreamKafkaTopicValue: PropTypes.array,
  sourceTypeNamespaceData: PropTypes.array,
  hdfslogNsData: PropTypes.array,
  routingNsData: PropTypes.array,
  sinkTypeNamespaceData: PropTypes.array,
  routingSinkTypeNsData: PropTypes.array,
  onInitSourceTypeNamespace: PropTypes.func,
  onInitHdfslogNamespace: PropTypes.func,
  onInitRoutingNamespace: PropTypes.func,
  onInitSinkTypeNamespace: PropTypes.func,
  onInitStreamNameSelect: PropTypes.func,
  resultFieldsValue: PropTypes.string,
  dataframeShowNumValue: PropTypes.string,
  etpStrategyConfirmValue: PropTypes.string,
  transConfigConfirmValue: PropTypes.string,
  transformTableConfirmValue: PropTypes.string,
  fieldSelected: PropTypes.string,
  dataframeShowSelected: PropTypes.string,
  streamDiffType: PropTypes.string,
  hdfslogSinkDataSysValue: PropTypes.string,
  hdfslogSinkNsValue: PropTypes.string,
  routingSourceNsValue: PropTypes.string,
  routingSinkNsValue: PropTypes.string,
  initResultFieldClass: PropTypes.func,
  initDataShowClass: PropTypes.func,
  onInitStreamTypeSelect: PropTypes.func,
  initialHdfslogCascader: PropTypes.func,
  initialRoutingSinkCascader: PropTypes.func,
  initialRoutingCascader: PropTypes.func,
  flowKafkaTopicValue: PropTypes.string,
  flowKafkaInstanceValue: PropTypes.string,
  sinkConfigCopy: PropTypes.string
}

export default Form.create({wrappedComponentRef: true})(WorkbenchFlowForm)
