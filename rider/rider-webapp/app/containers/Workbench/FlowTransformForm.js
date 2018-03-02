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
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import { forceCheckNum, operateLanguageSelect, operateLanguageFillIn } from '../../utils/util'
import DataSystemSelector from '../../components/DataSystemSelector'
import Form from 'antd/lib/form'
const FormItem = Form.Item
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Popover from 'antd/lib/popover'
import Tooltip from 'antd/lib/tooltip'
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
import Cascader from 'antd/lib/cascader'
import Radio from 'antd/lib/radio'
const RadioButton = Radio.Button
const RadioGroup = Radio.Group

export class FlowTransformForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = { dsHideOrNot: '' }
  }

  onTransformTypeSelect = (e) => this.props.onInitTransformValue(e.target.value)

  onLookupSqlTypeItemSelect = (val) => this.setState({ dsHideOrNot: val === 'union' ? 'hide' : '' })

  // 通过不同的Transformation里的 Sink Data System 显示不同的 Sink Namespace 的内容
  onTransformSinkDataSystemItemSelect = (val) => {
    this.props.form.setFieldsValue({ transformSinkNamespace: undefined })
    this.props.onInitTransformSinkTypeNamespace(this.props.projectIdGeted, val, 'transType')
  }

  render () {
    const { form } = this.props
    const { transformValue, transformSinkTypeNamespaceData } = this.props
    const { dsHideOrNot } = this.state
    const { getFieldDecorator } = form

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 17 }
    }

    const itemStyleNs = {
      labelCol: { span: 9 },
      wrapperCol: { span: 14 }
    }

    const itemStyleTimeout = {
      labelCol: { span: 14 },
      wrapperCol: { span: 7 }
    }

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

    const sinkDataSystemData = dsHideOrNot
      ? [
        { value: 'mysql', icon: 'icon-mysql' },
        { value: 'oracle', icon: 'icon-amy-db-oracle' },
        { value: 'postgresql', icon: 'icon-postgresql', style: {fontSize: '31px'} },
        { value: 'cassandra', icon: 'icon-cass', style: {fontSize: '52px', lineHeight: '60px'} },
        { value: 'mongodb', icon: 'icon-mongodb', style: {fontSize: '26px'} },
        { value: 'phoenix', text: 'Phoenix' },
        { value: 'es', icon: 'icon-elastic', style: {fontSize: '24px'} }
      ]
      : [
        { value: 'mysql', icon: 'icon-mysql' },
        { value: 'oracle', icon: 'icon-amy-db-oracle' },
        { value: 'postgresql', icon: 'icon-postgresql', style: {fontSize: '31px'} },
        { value: 'cassandra', icon: 'icon-cass', style: {fontSize: '52px', lineHeight: '60px'} },
        { value: 'mongodb', icon: 'icon-mongodb', style: {fontSize: '26px'} },
        { value: 'phoenix', text: 'Phoenix' },
        { value: 'hbase', icon: 'icon-hbase1' },
        { value: 'es', icon: 'icon-elastic', style: {fontSize: '24px'} },
        { value: 'redis', icon: 'icon-redis', style: {fontSize: '31px'} }
      ]

    const lookUpSqlMsg = (
      <span>
        SQL
        <Tooltip title={<FormattedMessage {...messages.workbenchHelp} />}>
          <Popover
            placement="top"
            content={<div style={{ width: '400px', height: '90px' }}>
              <p><FormattedMessage {...messages.workbenchTransLookup} /></p>
            </div>}
            title={<h3><FormattedMessage {...messages.workbenchHelp} /></h3>}
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    const sparkSqlMsg = (
      <span>
        SQL
        <Tooltip title={<FormattedMessage {...messages.workbenchHelp} />}>
          <Popover
            placement="top"
            content={<div style={{ width: '400px', height: '90px' }}>
              <p><FormattedMessage {...messages.workbenchTransSpark} /></p>
            </div>}
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
                <strong className="value-font-style">{this.props.step2SourceNamespace}</strong>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Sink Namespace" {...itemStyle}>
              {getFieldDecorator('step2SinkNamespace', {})(
                <strong className="value-font-style">{this.props.step2SinkNamespace}</strong>
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
                  <RadioButton value="sparkSql">Spark SQL</RadioButton>
                  <RadioButton value="streamJoinSql">Stream Join SQL</RadioButton>
                  <RadioButton value="transformClassName">ClassName</RadioButton>
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
                  data={flowLookupSqlType}
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
            <FormItem label={lookUpSqlMsg} className="tran-sql-label">
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

          {/* 设置 Spark Sql */}
          <Col span={6} className={transformTypeClassNames[1]}>
            <FormItem label={sparkSqlMsg} className="tran-sql-label">
              {getFieldDecorator('sparkSql', {
                hidden: transformTypeHiddens[1]
              })(
                <Input className="hide" />
              )}
            </FormItem>

          </Col>
          <Col span={17} className={`${transformTypeClassNames[1]} cm-sql-textarea`}>
            <textarea
              id="sparkSqlTextarea"
              placeholder="Spark SQL"
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

          <Col span={16} className={transformTypeClassNames[2]}>
            <FormItem label="Namespace" {...itemStyleNs}>
              {getFieldDecorator('streamJoinSqlNs', {
                rules: [{
                  required: true,
                  message: operateLanguageSelect('namespace', 'Namespace')
                }],
                hidden: transformTypeHiddens[2]
              })(
                <Cascader
                  placeholder="Select a Namespace"
                  popupClassName="ri-workbench-select-dropdown"
                  // options={flowTransNsData}
                  expandTrigger="hover"
                  displayRender={(labels) => labels.join('.')}
                />
              )}
            </FormItem>
          </Col>

          <Col span={7} className={transformTypeClassNames[2]}>
            <FormItem label="Timeout (Sec)" {...itemStyleTimeout}>
              {getFieldDecorator('timeout', {
                rules: [{
                  required: true,
                  message: operateLanguageFillIn('timeout', 'Timeout')
                }, {
                  validator: forceCheckNum
                }],
                hidden: transformTypeHiddens[2]
              })(
                <InputNumber min={10} max={1800} step={1} placeholder="Timeout" />
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
  form: React.PropTypes.any,
  transformSinkTypeNamespaceData: React.PropTypes.array,
  projectIdGeted: React.PropTypes.string,
  transformValue: React.PropTypes.string,
  step2SinkNamespace: React.PropTypes.string,
  step2SourceNamespace: React.PropTypes.string,
  // flowTransNsData: React.PropTypes.array,
  onInitTransformValue: React.PropTypes.func,
  onInitTransformSinkTypeNamespace: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(FlowTransformForm)
