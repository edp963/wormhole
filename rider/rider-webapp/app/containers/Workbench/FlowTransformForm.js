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
import Popover from 'antd/lib/popover'
import Tooltip from 'antd/lib/tooltip'
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
import Select from 'antd/lib/select'
import Cascader from 'antd/lib/cascader'
import Radio from 'antd/lib/radio'
const RadioButton = Radio.Button
const RadioGroup = Radio.Group

import { forceCheckNum, operateLanguageSelect, operateLanguageFillIn } from '../../utils/util'
import DataSystemSelector from '../../components/DataSystemSelector'
import { flowTransformationDadaHide, flowTransformationDadaShow } from '../../components/DataSystemSelector/dataSystemFunction'

export class FlowTransformForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      dsHideOrNot: '',
      selectValue: ''
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

  render () {
    const { form, transformValue, transformSinkTypeNamespaceData, flowTransNsData, step2SourceNamespace, step2SinkNamespace, flowSubPanelKey } = this.props
    const { dsHideOrNot, selectValue } = this.state
    const { getFieldDecorator } = form

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 17 }
    }

    const itemStyleTimeout = {
      labelCol: { span: 6 },
      wrapperCol: { span: 18 }
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
      transformValue === 'sparkSql' ? '' : 'hide',
      transformValue === 'cep' ? '' : 'hide'
    ]

    const flinkTransformTypeHiddens = [
      transformValue !== 'lookupSql',
      transformValue !== 'sparkSql',
      transformValue !== 'cep'
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
    } else if (selectValue === 'sparkSql') {
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
          <Col span={24} className={transformTypeClassNames[0]}>
            <FormItem label="Type" {...itemStyle} style={{lineHeight: '36px'}}>
              {getFieldDecorator('lookupSqlType', {
                rules: [{
                  required: true,
                  message: operateLanguageSelect('type', 'Type')
                }],
                hidden: transformTypeHiddens[0]
              })(
                <DataSystemSelector
                  data={flowSubPanelKey === 'spark' ? flowLookupSqlType : flowSubPanelKey === 'flink' ? flinkFlowLookupSqlType : []}
                  onItemSelect={this.onLookupSqlTypeItemSelect}
                />
              )}
            </FormItem>
          </Col>
          <Col span={24} className={transformTypeClassNames[0]}>
            <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
              {getFieldDecorator('transformSinkDataSystem', {
                rules: [{
                  required: true,
                  message: operateLanguageSelect('data system', 'Data System')
                }],
                hidden: transformTypeHiddens[0]
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
          <Col span={6} className={transformTypeClassNames[0]}>
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

          {/* 设置 Spark/ Flink Sql */}
          {flowSubPanelKey === 'spark' ? (
            <Col span={6} className={transformTypeClassNames[1]}>
              <FormItem label={sqlHtml} className="tran-sql-label">
                {getFieldDecorator('sparkSql', {
                  hidden: transformTypeHiddens[1]
                })(
                  <Input className="hide" />
                )}
              </FormItem>
            </Col>
          ) : flowSubPanelKey === 'flink' ? (
            <Col span={6} className={transformTypeClassNames[1]}>
              <FormItem label={sqlHtml} className="tran-sql-label">
                {getFieldDecorator('flinkSql', {
                  hidden: flinkTransformTypeHiddens[1]
                })(
                  <Input className="hide" />
                )}
              </FormItem>
            </Col>
          ) : ''}

          <Col span={17} className={`${transformTypeClassNames[1]} cm-sql-textarea`}>
            <textarea
              id="sparkSqlTextarea"
              placeholder={flowSubPanelKey === 'spark' ? 'Spark SQL' : flowSubPanelKey === 'flink' ? 'Flink SQL' : ''}
            />
          </Col>

          {/* 设置 Stream Join Sql */}
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
          <Col span={24} className={transformTypeClassNames[2]}>
            <FormItem label="Retention time (Sec)" {...itemStyleTimeout}>
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

          <Col span={6} className={transformTypeClassNames[2]}>
            <FormItem label="SQL" className="tran-sql-label">
              {getFieldDecorator('streamJoinSql', {
                hidden: transformTypeHiddens[2]
              })(
                <Input className="hide" />
              )}
            </FormItem>
          </Col>
          <Col span={17} className={`${transformTypeClassNames[2]} cm-sql-textarea`}>
            <textarea
              id="streamJoinSqlTextarea"
              placeholder="Stream Join SQL"
            />
          </Col>

          {/* 设置 ClassName */}
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
  flowSubPanelKey: PropTypes.string
}

export default Form.create({wrappedComponentRef: true})(FlowTransformForm)
