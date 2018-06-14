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
import { postUserTopic, deleteUserTopic } from './action'

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

  onApplyOffset = (i, index, offset, type) => (e) => {
    this.props.form.setFieldsValue({
      [`${i.id}_${index}_${type}`]: offset
    })
  }

  onApplyAll = (i, type, topicsType) => (e) => {
    let arr = []
    switch (type) {
      case 'consumer':
        arr = i.conOffsetVal.split(',')
        break
      case 'kafka':
        arr = i.kafOffsetVal.split(',')
        break
      case 'kafkaEar':
      // NOTE: 新变量
        arr = i.kafEarOffsetVal.split(',')
    }
    for (let item of arr) {
      const itemTemp = item.split(':')
      this.props.form.setFieldsValue({
        [`${i.id}_${itemTemp[0]}_${topicsType}`]: itemTemp[1]
      })
    }
  }
  toggleItem = (type, id) => (e) => {
    if (!type) return
    let { projectIdGeted, streamIdGeted } = this.props
    if (type === 'add') {
      this.props.form.validateFieldsAndScroll((err, values) => {
        if (!err) {
          let name = values.newTopicName
          let req = { name }
          this.props.onPostUserTopic(projectIdGeted, streamIdGeted, req, (result) => {
            console.log('onPostUserTopic', result)
          })
        }
      })
    } else if (type === 'delete') {
      this.props.form.validateFieldsAndScroll((err, values) => {
        if (!err) {
          this.props.onDeleteUserTopic(projectIdGeted, streamIdGeted, id, (result) => {
            console.log('onDeleteUserTopic', result)
          })
        }
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

    const itemFactory = (data, hasDel = false, type = 'auto') => {
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
                    <Col span={3} className="card-content required-offset">Offset</Col>
                    <Col span={7} className="card-content">Latest Consumed Offset</Col>
                    <Col span={6} className="card-content">Earliest Kafka Offset</Col>
                    <Col span={5} className="card-content">Latest Kafka Offset</Col>
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
                    <Col span={2} className="partition-content">{g.substring(0, g.indexOf(':'))}</Col>
                    <Col span={6} className="offset-content">
                      <FormItem>
                        <ol key={g}>
                          {getFieldDecorator(`${i.id}_${index}_${type}`, {
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
                    <Col span={6} className="stream-start-offset-class">
                      <FormItem>
                        <ol key={g}>
                          {getFieldDecorator(`consumedLatest_${i.id}_${index}_${type}`, {})(
                            <div className="stream-start-lastest-consumed-offset">
                              <span style={{ marginRight: '5px' }}>{conOffFinal}</span>
                              <Tooltip title={applyFormat}>
                                <Button shape="circle" type="ghost" onClick={this.onApplyOffset(i, index, conOffFinal, type)}>
                                  <i className="iconfont icon-apply_icon_-copy-copy"></i>
                                </Button>
                              </Tooltip>
                            </div>
                          )}
                        </ol>
                      </FormItem>
                    </Col>
                    <Col span={6} className="stream-start-offset-class">
                      <FormItem>
                        <ol key={g}>
                          {getFieldDecorator(`kafkaEarliest_${i.id}_${index}_${type}`, {})(
                            <div className="stream-start-lastest-kafka-offset">
                              <span style={{ marginRight: '5px' }}>{kafOffFinal}</span>
                              <Tooltip title={applyFormat}>
                                <Button shape="circle" type="ghost" onClick={this.onApplyOffset(i, index, kafOffFinal, type)}>
                                  <i className="iconfont icon-apply_icon_-copy-copy"></i>
                                </Button>
                              </Tooltip>
                            </div>
                          )}
                        </ol>
                      </FormItem>
                    </Col>
                    <Col span={2} offset={2} className="stream-start-offset-class">
                      <FormItem>
                        <ol key={g}>
                          {getFieldDecorator(`kafkaLatest_${i.id}_${index}_${type}`, {})(
                            <div className="stream-start-lastest-kafka-offset">
                              <span style={{ marginRight: '5px' }}>{kafOffFinal}</span>
                              <Tooltip title={applyFormat}>
                                <Button shape="circle" type="ghost" onClick={this.onApplyOffset(i, index, kafOffFinal, type)}>
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
                  <Col span={2} className="card-content card-content-extra">Partition</Col>
                  <Col span={4} offset={1} className="card-content required-offset card-content-extra">Offset</Col>
                  <Col span={6} className="card-content">Latest Consumed Offset
                    <Tooltip title={applyAllText}>
                      <Button shape="circle" type="ghost" onClick={this.onApplyAll(i, 'consumer', type)}>
                        <i className="iconfont icon-apply_icon_-copy-copy"></i>
                      </Button>
                    </Tooltip>
                  </Col>
                  <Col span={6} className="card-content">Earliest Kafka Offset
                    <Tooltip title={applyAllText}>
                      <Button shape="circle" type="ghost" onClick={this.onApplyAll(i, 'kafkaEar', type)}>
                        <i className="iconfont icon-apply_icon_-copy-copy"></i>
                      </Button>
                    </Tooltip>
                  </Col>
                  <Col span={5} className="card-content">Latest Kafka Offset
                    <Tooltip title={applyAllText}>
                      <Button shape="circle" type="ghost" onClick={this.onApplyAll(i, 'kafka', type)}>
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
                    {
                      hasDel ? (
                        <Button shape="circle" type="danger" style={{position: 'absolute', top: '5px', right: '5px'}} onClick={this.toggleItem('delete', i.id)}>
                          <Icon type="minus-circle" />
                        </Button>) : ''
                    }
                  </div>
                </Card>
              </Row>
            )
          })
      }
      return cardStartItem
    }

    const userItemFactory = (data) => itemFactory(data, true, 'user')

    const userDefinedTopicsCardAddItem = (
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
                  <Input className="rate-input" />
                )}
              </FormItem>
            </Col>
          </div>
          <div className="rate-class" style={{flexGrow: 1}}>
            <Col offset={22} className="card-content card-content-extra">
              Add
            </Col>
            <Col offset={22}>
              <Button shape="circle" type="default" onClick={this.toggleItem('add')}>
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
                {itemFactory(data)}
              </Panel>
              <Panel header={userDefinedTopicsCardTitle}>
                {userDefinedTopicsCardAddItem}
                {userItemFactory(data)}
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
  locale: PropTypes.string,
  projectIdGeted: PropTypes.string,
  streamIdGeted: PropTypes.number,
  onPostUserTopic: PropTypes.func,
  onDeleteUserTopic: PropTypes.func
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

function mapDispatchToProps (dispatch) {
  return {
    onPostUserTopic: (projectId, streamId, topic, resolve) => dispatch(postUserTopic(projectId, streamId, topic, resolve)),
    onDeleteUserTopic: (projectId, streamId, topicId, resolve) => dispatch(deleteUserTopic(projectId, streamId, topicId, resolve))
  }
}
export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, mapDispatchToProps)(StreamStartForm))
