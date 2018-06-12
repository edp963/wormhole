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
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Card from 'antd/lib/card'
import Tooltip from 'antd/lib/tooltip'
import Button from 'antd/lib/button'
import Select from 'antd/lib/select'
import InputNumber from 'antd/lib/input-number'
import { Collapse, Input, Icon } from 'antd'
const Panel = Collapse.Panel
const FormItem = Form.Item
import { forceCheckNum } from '../../utils/util'
import { selectLocale } from '../LanguageProvider/selectors'

export class StreamStartForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      data: [] || ''
    }
  }

  componentWillReceiveProps (props) {
    let dataFinal = []
    if (!props.data) {
      dataFinal = 'There is no topics now.'
    } else {
      dataFinal = props.data.map(s => {
        const conTemp = props.consumedOffsetValue.find(i => i.id === s.id)
        const conTempObject = conTemp
          ? {
            id: conTemp.id,
            name: conTemp.name,
            conOffsetVal: conTemp.partitionOffsets,
            rate: conTemp.rate,
            key: conTemp.id
          }
          : {}

        const kafTemp = props.kafkaOffsetValue.find(i => i.id === s.id)
        const kafTempObject = kafTemp
          ? {kafOffsetVal: kafTemp.partitionOffsets}
          : {}

        return Object.assign(conTempObject, kafTempObject)
      })
    }

    this.setState({ data: dataFinal })
  }

  onApplyOffset = (i, index, offset) => (e) => {
    this.props.form.setFieldsValue({
      [`${i.id}_${index}`]: offset
    })
  }

  onApplyAll = (i, type) => (e) => {
    let arr = []
    switch (type) {
      case 'consumer':
        arr = i.conOffsetVal.split(',')
        break
      case 'kafka':
        arr = i.kafOffsetVal.split(',')
        break
    }
    for (let item of arr) {
      const itemTemp = item.split(':')
      this.props.form.setFieldsValue({
        [`${i.id}_${itemTemp[0]}`]: itemTemp[1]
      })
    }
  }

  render () {
    const { form, streamActionType, startUdfValsOption, renewUdfValsOption, currentUdfVal, locale } = this.props
    const { getFieldDecorator } = form
    const { data } = this.state

    const noTopicCardTitle = (<Col span={24} style={{fontWeight: '500'}}><span className="modal-topic-name">Topic Name</span></Col>)
    const topicCardTitle = (<Col span={24} style={{fontWeight: '500'}}><span className="modal-topic-name">Topics</span></Col>)
    const autoRegisteredTopicsCardTitle = (<Col span={24} style={{fontWeight: '500'}}><span className="modal-topic-name">Auto Registered Topics</span></Col>)
    const userDefinedTopicsCardTitle = (<Col span={24} style={{fontWeight: '500'}}><span className="modal-topic-name">User Defined Topics</span></Col>)

    let cardStartItem = ''
    if (data) {
      cardStartItem = data === 'There is no topics now.'
        ? (
          <Row className="no-topic-card-class">
            <Card title={noTopicCardTitle} className="stream-start-form-card-style">
              <div className="rate-topic-info-wrapper">
                <div className="rate-class">
                  <Col span={24} className="card-content required-offset">
                    Rate (<FormattedMessage {...messages.streamModalRate} />)
                  </Col>
                </div>
                <div className="topic-info-class">
                  <Col span={3} className="card-content">Partition</Col>
                  <Col span={7} className="card-content required-offset">Offset</Col>
                  <Col span={7} className="card-content">Latest Consumed Offset</Col>
                  <Col span={7} className="card-content">Latest Kafka Offset</Col>
                </div>
              </div>
              <h3 className="no-topic-class">{data}</h3>
            </Card>
          </Row>
        )
        : data.map(i => {
          let parOffInput = ''

          if (i.conOffsetVal) {
            const partitionOffsetsArr = i.conOffsetVal.split(',')

            parOffInput = partitionOffsetsArr.map((g, index) => {
              const gKey = g.substring(0, g.indexOf(':'))
              const conOffFinal = g.substring(g.indexOf(':') + 1)

              let kafOffFinal = ''
              if (i.kafOffsetVal) {
                const kafOffArr = i.kafOffsetVal.split(',')
                const kafOffFilter = kafOffArr.filter(s => s.substring(0, s.indexOf(':')) === gKey)
                kafOffFinal = kafOffFilter[0].substring(kafOffFilter[0].indexOf(':') + 1)
              } else {
                kafOffFinal = ''
              }

              const applyFormat = <FormattedMessage {...messages.streamModalApply} />
              return (
                <Row key={`${i.id}_${index}`}>
                  <Col span={3} className="partition-content">{g.substring(0, g.indexOf(':'))}</Col>
                  <Col span={7} className="offset-content">
                    <FormItem>
                      <ol key={g}>
                        {getFieldDecorator(`${i.id}_${index}`, {
                          rules: [{
                            required: true,
                            message: locale === 'en' ? 'Please fill in offset' : '请填写 Offset'
                          }, {
                            validator: forceCheckNum
                          }],
                          initialValue: conOffFinal
                        })(
                          <InputNumber size="medium" className="conform-table-input" />
                        )}
                      </ol>
                    </FormItem>
                  </Col>
                  <Col span={7} className="stream-start-offset-class">
                    <FormItem>
                      <ol key={g}>
                        {getFieldDecorator(`consumedLatest_${i.id}_${index}`, {})(
                          <div className="stream-start-lastest-consumed-offset">
                            <span style={{ marginRight: '5px' }}>{conOffFinal}</span>
                            <Tooltip title={applyFormat}>
                              <Button shape="circle" type="ghost" onClick={this.onApplyOffset(i, index, conOffFinal)}>
                                <i className="iconfont icon-apply_icon_-copy-copy"></i>
                              </Button>
                            </Tooltip>
                          </div>
                        )}
                      </ol>
                    </FormItem>
                  </Col>
                  <Col span={7} className="stream-start-offset-class">
                    <FormItem>
                      <ol key={g}>
                        {getFieldDecorator(`kafkaLatest_${i.id}_${index}`, {})(
                          <div className="stream-start-lastest-kafka-offset">
                            <span style={{ marginRight: '5px' }}>{kafOffFinal}</span>
                            <Tooltip title={applyFormat}>
                              <Button shape="circle" type="ghost" onClick={this.onApplyOffset(i, index, kafOffFinal)}>
                                <i className="iconfont icon-apply_icon_-copy-copy"></i>
                              </Button>
                            </Tooltip>
                          </div>
                        )}
                      </ol>
                    </FormItem>
                  </Col>
                </Row>
              )
            })
          } else {
            return
          }

          const cardTitle = (
            <Row key={i.id}>
              <Col span={24} style={{fontWeight: '500'}}>
                <span className="modal-topic-name">Topic Name</span>
                {i.name}
              </Col>
            </Row>
          )

          const applyAllText = <FormattedMessage {...messages.streamModalApplyAll} />
          const cardContent = (
            <Row key={i.id} className="apply-all-btn">
              <div className="rate-topic-info-wrapper">
                <Col span={3} className="card-content card-content-extra">Partition</Col>
                <Col span={7} className="card-content required-offset card-content-extra">Offset</Col>
                <Col span={7} className="card-content">Latest Consumed Offset
                  <Tooltip title={applyAllText}>
                    <Button shape="circle" type="ghost" onClick={this.onApplyAll(i, 'consumer')}>
                      <i className="iconfont icon-apply_icon_-copy-copy"></i>
                    </Button>
                  </Tooltip>
                </Col>
                <Col span={7} className="card-content">Latest Kafka Offset
                  <Tooltip title={applyAllText}>
                    <Button shape="circle" type="ghost" onClick={this.onApplyAll(i, 'kafka')}>
                      <i className="iconfont icon-apply_icon_-copy-copy"></i>
                    </Button>
                  </Tooltip>
                </Col>
              </div>
              {parOffInput}
            </Row>
          )

          return (
            <Row key={i.id}>
              <Card title={cardTitle} className="stream-start-form-card-style">
                <div className="rate-topic-info-wrapper">
                  <div className="rate-class">
                    <Col span={24} className="card-content required-offset card-content-extra">
                      Rate (<FormattedMessage {...messages.streamModalRate} />)
                    </Col>
                    <Col span={24}>
                      <FormItem>
                        {getFieldDecorator(`${i.id}_${i.rate}_rate`, {
                          rules: [{
                            required: true,
                            message: locale === 'en' ? 'Please fill in rate' : '请填写 Rate'
                          }, {
                            validator: forceCheckNum
                          }],
                          initialValue: `${i.rate}`
                        })(
                          <InputNumber size="medium" className="rate-input" />
                        )}
                      </FormItem>
                    </Col>
                  </div>
                  <div className="topic-info-class">
                    {cardContent}
                  </div>
                </div>
              </Card>
            </Row>
          )
        })
    }

    let userDefinedTopicsCardAddItem = (
      <Row className="apply-all-btn">
        <div className="rate-topic-info-wrapper">
          <div className="rate-class">
            <Col span={24} className="card-content required-offset card-content-extra">
              Topic Name
            </Col>
            <Col span={24}>
              <FormItem>
                {getFieldDecorator(`newTopicName`, {
                  rules: [{
                    required: true,
                    message: locale === 'en' ? 'Please fill in topic name' : '请填写 topic name'
                  }]
                })(
                  <Input size="medium" className="rate-input" />
                )}
              </FormItem>
            </Col>
          </div>
          <div className="rate-class">
            <Col span={24} className="card-content required-offset card-content-extra">
              Partition Nums
            </Col>
            <Col span={24}>
              <FormItem>
                {getFieldDecorator(`partitionNum`, {
                  rules: [{
                    required: true,
                    message: locale === 'en' ? 'Please fill in partition nums' : '请填写 partition nums'
                  }, {
                    validator: forceCheckNum
                  }]
                })(
                  <InputNumber size="medium" className="rate-input" />
                )}
              </FormItem>
            </Col>
          </div>
          <div className="rate-class" style={{flexGrow: 1}}>
            <Col offset={22} className="card-content card-content-extra">
              Add
            </Col>
            <Col offset={22}>
              <Button shape="circle" type="primary">
                <Icon type="plus-circle" />
              </Button>
            </Col>
          </div>
        </div>
      </Row>
    )
    const itemStyleUdf = {
      wrapperCol: { span: 24 }
    }

    const udfChildren = streamActionType === 'start'
      ? startUdfValsOption.map(i => (<Select.Option key={i.id} value={`${i.id}`}>{i.functionName}</Select.Option>))
      : renewUdfValsOption.map(i => (<Select.Option key={i.id} value={`${i.id}`}>{i.functionName}</Select.Option>))

    const currentUdfsShow = currentUdfVal.length === 0
      ? ''
      : currentUdfVal.map(i => i.functionName).join(', ')

    return (
      <Form>
        <Row>
          <Card title="UDFs：" className={streamActionType === 'start' ? 'stream-start-form-udf-style' : 'stream-renew-form-udf-style'}>
            <div className="udf-info-wrapper">
              <div className={`${streamActionType === 'start' ? 'hide' : ''} selected-udf-class`}>Selected UDFs：{currentUdfsShow}</div>
              <Col span={24} className="stream-udf">
                <FormItem label="" {...itemStyleUdf}>
                  {getFieldDecorator('udfs', {})(
                    <Select
                      mode="multiple"
                      placeholder={streamActionType === 'start' ? 'Select UDFs' : 'Add UDFs'}
                    >
                      {udfChildren}
                    </Select>
                  )}
                </FormItem>
              </Col>
            </div>
          </Card>
          <Card title={topicCardTitle} className="stream-start-form-card-style">
            <Collapse>
              <Panel header={autoRegisteredTopicsCardTitle}>
                {cardStartItem}
              </Panel>
              <Panel header={userDefinedTopicsCardTitle}>
                {userDefinedTopicsCardAddItem}
                {cardStartItem}
              </Panel>
            </Collapse>
            {/* <Card title={autoRegisteredTopicsCardTitle} className="stream-start-form-card-style">
              {cardStartItem}
            </Card>
            <Card title={userDefinedTopicsCardTitle} className="stream-start-form-card-style">
              {cardStartItem}
            </Card> */}
          </Card>
        </Row>
      </Form>
    )
  }
}

StreamStartForm.propTypes = {
  form: PropTypes.any,
  streamActionType: PropTypes.string,
  startUdfValsOption: PropTypes.array,
  renewUdfValsOption: PropTypes.array,
  currentUdfVal: PropTypes.array,
  locale: PropTypes.string
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, null)(StreamStartForm))
