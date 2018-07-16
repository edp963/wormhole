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
      outputType: 'agg',
      patternModalShow: false
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

  changeStrategy = (e) => {
    console.log(e)
  }
  changeOutput = (e) => {
    let outputType = e.target.value
    this.setState({outputType})
  }
  doFilterQuery = (sql) => {
    console.log('__sql__: ', sql)
  }

  addOrEditPattern = () => {
    console.log('addOrEditPattern')
  }
  onEditPattern = () => {
    console.log('onEditPattern')
  }
  onPatternModal = () => {
    const { cepDataSource } = this.state
    if (cepDataSource.length === 0) {
      this.setState({operatorBtnInitVal: 'begin'})
    }
    this.setState({
      patternModalShow: true
    })
    console.log('onPatternModal')
  }
  closePatternModal = () => {
    this.setState({
      patternModalShow: false
    })
    console.log('closePatternModal')
  }
  render () {
    const { form, transformValue, transformSinkTypeNamespaceData, flowTransNsData, step2SourceNamespace, step2SinkNamespace, flowSubPanelKey } = this.props
    const { dsHideOrNot, selectValue, cepDataSource, outputType, patternModalShow, operatorBtnInitVal } = this.state
    const { getFieldDecorator } = form

    const itemStyle = {
      labelCol: { span: 4 },
      wrapperCol: { span: 10 }
    }

    // const itemStyleTimeout = {
    //   labelCol: { span: 6 },
    //   wrapperCol: { span: 18 }
    // }
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
      outputType === 'detail'
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
    const columnsCEP = [
      {
        title: 'Operator',
        dataIndex: 'operator',
        key: 'operator',
        width: '20%',
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
        title: 'Quartifier',
        dataIndex: 'quartifier',
        key: 'quartifier',
        width: '20%',
        className: 'text-align-center'
      },
      {
        title: 'Action',
        dataIndex: 'action',
        key: 'action',
        width: '20%',
        className: 'text-align-center',
        render: (text, record) => {
          <Button icon="edit" shape="circle" type="ghost" onClick={this.onEditPattern(record)}></Button>
        }
      }
    ]
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
              <FormItem label="Windowtime" {...itemStyle}>
                {getFieldDecorator('windowTime', {
                  rules: [{
                    required: true,
                    message: operateLanguageFillIn('windowtime', 'Windowtime')
                  }],
                  hidden: flinkTransformTypeHiddens[2]
                })(
                  <InputNumber step={1} placeholder="seconds" />
                )}
              </FormItem>
            </Col>
          ) : '' }
          {flowSubPanelKey === 'flink' ? (
            <Col span={24} className={flinkTransformTypeClassNames[2]}>
              <FormItem label="Strategy" {...itemStyle}>
                {getFieldDecorator('strategy', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('strategy', 'Strategy')
                  }]
                })(
                  <Select
                    dropdownClassName="ri-workbench-select-dropdown"
                    onChange={(e) => this.changeStrategy(e)}
                    placeholder="Select a strategy"
                    // disabled={flowDisabledOrNot}
                  >
                    {/* NOTE: 待添加 */}
                    <Select.Option key={1} value={'1'}>1</Select.Option>
                  </Select>
                )}
              </FormItem>
            </Col>
          ) : '' }
          {flowSubPanelKey === 'flink' ? (
            <Col span={24} className={flinkTransformTypeClassNames[2]}>
              <FormItem label="KeyBy" {...itemStyle}>
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
              <FormItem label="Output" labelCol={{span: 4}} wrapperCol={{span: 10}}>
                {getFieldDecorator('output', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('output', 'Output')
                  }],
                  initialValue: 'agg'
                })(
                  <RadioGroup onChange={this.changeOutput}>
                    <RadioButton value="agg">Agg</RadioButton>
                    <RadioButton value="detail">Detail</RadioButton>
                    <RadioButton value="filteredRow">FilteredRow</RadioButton>
                  </RadioGroup>
                )}
              </FormItem>
            </Col>
          ) : '' }
          {flowSubPanelKey === 'flink' ? (
            <Col span={4} pull={4} className={`${flinkTransformTypeClassNames[2]} ${outputHiddens[0] ? 'hide' : ''}`}>
              <FormItem>
                {getFieldDecorator('outputText', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('output', 'Output')
                  }],
                  hidden: outputHiddens[0]
                })(
                  <Input />
                )}
              </FormItem>
            </Col>
          ) : ''}
          {flowSubPanelKey === 'flink' ? (
            <Col span={24} className={`${flinkTransformTypeClassNames[2]}`}>
              <FormItem label="Pattern" {...itemStyle}>
                {getFieldDecorator('patternBtn', {
                  hidden: true
                })(
                  <Button onClick={this.onPatternModal}>添加Pattern</Button>
                )}
              </FormItem>
            </Col>
          ) : ''}
          {flowSubPanelKey === 'flink' ? (
            <Col span={20} offset={4} className={flinkTransformTypeClassNames[2]}>
              <Table
                dataSource={cepDataSource}
                columns={columnsCEP}
                pagination={pagination}
                bordered
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
              onCancel={this.closePatternModal}
            >
              <Card className={`${flinkTransformTypeClassNames[2]}`}>
                <Col span={24}>
                  <FormItem label="Operator" {...patternItemStyle}>
                    {getFieldDecorator('operator', {
                      rules: [{
                        required: true,
                        message: operateLanguageSelect('operator', 'Operator')
                      }],
                      initialValue: operatorBtnInitVal
                    })(
                      <RadioGroup size="default">
                        <RadioButton value="begin" className="radio-btn-style" disabled={cepDataSource.length > 0}>Begin</RadioButton>
                        <RadioButton value="next" className="radio-btn-style" disabled={cepDataSource.length === 0}>Next</RadioButton>
                        <RadioButton value="followBy" className="radio-btn-style" disabled={cepDataSource.length === 0}>FollowBy</RadioButton>
                        <RadioButton value="notNext" className="radio-btn-style" disabled={cepDataSource.length === 0}>NotNext</RadioButton>
                        <RadioButton value="notFollowBy" className="radio-btn-style" disabled={cepDataSource.length === 0}>NotFollowBy</RadioButton>
                      </RadioGroup>
                    )}
                  </FormItem>
                </Col>
                <Col span={24}>
                  <FormItem label="Quartifier" {...patternItemStyle}>
                    {getFieldDecorator('quartifier', {
                      rules: [{
                        required: true,
                        message: operateLanguageSelect('quartifier', 'Quartifier')
                        // hidden: flinkTransformTypeHiddens[2]
                      }]
                    })(
                      <RadioGroup size="default">
                        <RadioButton value="oneOrMore" className="radio-btn-style">OneOrMore</RadioButton>
                        <RadioButton value="times" className="radio-btn-style">Times($int$)</RadioButton>
                        <RadioButton value="timesOrMore" className="radio-btn-style">TimesOrMore($int$)</RadioButton>
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
                        // hidden: flinkTransformTypeHiddens[2]
                      }]
                    })(
                      <div style={{width: '100%', height: '260px', padding: '20px', overflow: 'scroll', border: '1px solid #ddd'}}>
                        <FilterComponent
                          loginUser={null}
                          itemId={null}
                          onQuery={this.doFilterQuery}
                          wrappedComponentRef={f => { this.filterComponent = f }}
                        ></FilterComponent>
                      </div>
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
  flowSubPanelKey: PropTypes.string
}

export default Form.create({wrappedComponentRef: true})(FlowTransformForm)
