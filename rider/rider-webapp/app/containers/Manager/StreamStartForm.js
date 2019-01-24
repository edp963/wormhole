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
import { Collapse, Input, Icon, message } from 'antd'
const Panel = Collapse.Panel
const FormItem = Form.Item
import { forceCheckNum, transformStringWithDot } from '../../utils/util'
import { selectLocale } from '../LanguageProvider/selectors'
import { postUserTopic } from './action'

export class StreamStartForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      data: [] || '',
      userDefinedTopics: [],
      unValidate: false
    }
  }

  componentWillReceiveProps (props) {
    let dataFinal = []
    let userDefinedTopics = []
    let unValidate = props.unValidate
    if (!props.data) {
      dataFinal = 'There is no topics now.'
    } else {
      dataFinal = props.data.slice()
      userDefinedTopics = props.userDefinedTopics.slice()
    }
    this.setState({ data: dataFinal, userDefinedTopics, unValidate })
  }

  onApplyOffset = (i, index, offset, type) => (e) => {
    this.props.form.setFieldsValue({
      [`${i.name.replace(/\./g, '-')}_${index}_${type}`]: offset
    })
  }

  onApplyAll = (i, type, topicsType) => (e) => {
    let arr = []
    switch (type) {
      case 'consumer':
        arr = i.consumedLatestOffset.split(',')
        break
      case 'kafka':
        arr = i.kafkaLatestOffset.split(',')
        break
      case 'kafkaEar':
        arr = i.kafkaEarliestOffset.split(',')
    }
    for (let item of arr) {
      const itemTemp = item.split(':')
      this.props.form.setFieldsValue({
        [`${i.name.replace(/\./g, '-')}_${itemTemp[0]}_${topicsType}`]: itemTemp[1]
      })
    }
  }
  toggleItem = (type, name) => (e) => {
    if (!type) return
    let { projectIdGeted, streamIdGeted } = this.props
    switch (type) {
      case 'add':
        this.props.form.validateFieldsAndScroll((err, values) => {
          if (!err) {
            let name = values.newTopicName
            let req = { name }
            this.props.onPostUserTopic(projectIdGeted, streamIdGeted, req, (result) => {
              let userTopicList = this.state.userDefinedTopics.slice()
              result.name = transformStringWithDot(result.name)
              for (let i = 0; i < userTopicList.length; i++) {
                if (userTopicList[i].name === result.name) {
                  message.error('topic already exist')
                  return
                }
              }
              userTopicList = userTopicList.concat(result)
              this.setState({userDefinedTopics: userTopicList}, () => {
                this.props.emitStartFormDataFromSub(this.state.userDefinedTopics)
                this.props.form.resetFields(['newTopicName'])
                message.success('success', 3)
              })
            }, (error) => {
              message.error(error, 3)
            })
          }
        })
        break
      case 'delete':
        let userTopicList = this.state.userDefinedTopics.slice()
        for (let i = 0; i < userTopicList.length; i++) {
          if (userTopicList[i].name === name) {
            userTopicList.splice(i, 1)
          }
        }
        this.setState({userDefinedTopics: userTopicList}, () => {
          this.props.emitStartFormDataFromSub(this.state.userDefinedTopics)
        })
        break
    }
  }
  render () {
    const { form, streamActionType, startUdfValsOption, renewUdfValsOption, currentUdfVal, locale } = this.props
    const { getFieldDecorator } = form
    const { data, userDefinedTopics, unValidate } = this.state

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
            let myName = i.name
            if (i.consumedLatestOffset) {
              const partitionOffsetsArr = i.consumedLatestOffset.split(',')

              parOffInput = partitionOffsetsArr.map((g, index) => {
                const gKey = g.substring(0, g.indexOf(':'))
                const conOffFinal = g.substring(g.indexOf(':') + 1)

                let kafOffFinal = ''
                if (i.kafkaLatestOffset) {
                  const kafOffArr = i.kafkaLatestOffset.split(',')
                  const kafOffFilter = kafOffArr.filter(s => s.substring(0, s.indexOf(':')) === gKey)
                  kafOffFinal = kafOffFilter[0].substring(kafOffFilter[0].indexOf(':') + 1)
                } else {
                  kafOffFinal = ''
                }

                let kafEarOffFinal = ''
                if (i.kafkaEarliestOffset) {
                  const kafEarOffArr = i.kafkaEarliestOffset.split(',')
                  const kafOffFilter = kafEarOffArr.filter(s => s.substring(0, s.indexOf(':')) === gKey)
                  kafEarOffFinal = kafOffFilter[0].substring(kafOffFilter[0].indexOf(':') + 1)
                }
                const lag = kafOffFinal - conOffFinal
                const applyFormat = <FormattedMessage {...messages.streamModalApply} />
                return (
                  <Row key={`${myName}_${index}_${type}`}>
                    <Col span={2} className="partition-content">{g.substring(0, g.indexOf(':'))}</Col>
                    <Col span={5} className="offset-content">
                      <FormItem>
                        <ol key={index}>
                          {getFieldDecorator(`${myName}_${index}_${type}`, {
                            rules: [{
                              required: true,
                              message: locale === 'en' ? 'Please fill in offset' : '请填写 Offset'
                            }, {
                              validator: (rule, value, callback) => {
                                const singledata = data.find(v => v.name === myName)
                                let earliestKafkaOffsetArr = singledata.kafkaEarliestOffset.split(',')
                                let earliestKafkaOffset = earliestKafkaOffsetArr[index].split(':')[1]
                                if (Number(value) < Number(earliestKafkaOffset)) {
                                  callback(`offset必须大于等于Earliest Kafka Offset`)
                                } else {
                                  forceCheckNum(rule, value, callback)
                                }
                              }
                            }],
                            initialValue: conOffFinal
                          })(
                            <InputNumber size="medium" className="conform-table-input" min={0} />
                          )}
                        </ol>
                      </FormItem>
                    </Col>
                    <Col span={4} className="stream-start-offset-class">
                      <FormItem>
                        <ol key={index}>
                          {getFieldDecorator(`consumedLatest_${myName}_${index}_${type}`, {})(
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
                    <Col span={2} offset={1} className="stream-start-offset-class">
                      <FormItem>
                        <ol key={index}>
                          {getFieldDecorator(`kafkaEarliest_${myName}_${index}_${type}`, {})(
                            <div className="stream-start-lastest-kafka-offset">
                              <span style={{ marginRight: '-15px' }}>{lag}</span>
                            </div>
                          )}
                        </ol>
                      </FormItem>
                    </Col>
                    <Col span={4} offset={2} className="stream-start-offset-class">
                      <FormItem>
                        <ol key={index}>
                          {getFieldDecorator(`kafkaEarliest_${myName}_${index}_${type}`, {})(
                            <div className="stream-start-lastest-kafka-offset">
                              <span style={{ marginRight: '5px' }}>{kafEarOffFinal}</span>
                              <Tooltip title={applyFormat}>
                                <Button shape="circle" type="ghost" onClick={this.onApplyOffset(i, index, kafEarOffFinal, type)}>
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
                        <ol key={index}>
                          {getFieldDecorator(`kafkaLatest_${myName}_${index}_${type}`, {})(
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
              <Row key={myName}>
                <Col span={24} style={{fontWeight: '500'}}>
                  <span className="modal-topic-name">Topic Name</span>
                  {transformStringWithDot(i.name, false)}
                </Col>
              </Row>
            )

            const applyAllText = <FormattedMessage {...messages.streamModalApplyAll} />
            const cardContent = (
              <Row key={myName} className="apply-all-btn">
                <div className="rate-topic-info-wrapper">
                  <Col span={2} className="card-content ">Partition</Col>
                  <Col span={3} offset={1} className="card-content required-offset ">Offset</Col>
                  <Col span={6} className="card-content">Latest Consumed Offset
                    <Tooltip title={applyAllText}>
                      <Button shape="circle" type="ghost" onClick={this.onApplyAll(i, 'consumer', type)}>
                        <i className="iconfont icon-apply_icon_-copy-copy"></i>
                      </Button>
                    </Tooltip>
                  </Col>
                  <Col span={2} className="card-content">Lag
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
              <Row key={myName}>
                <Card title={cardTitle} className="stream-start-form-card-style">
                  <div className="rate-topic-info-wrapper">
                    <div className="rate-class">
                      <Col span={24} className="card-content required-offset ">
                        Rate (<FormattedMessage {...messages.streamModalRate} />)
                      </Col>
                      <Col span={24}>
                        <FormItem>
                          {getFieldDecorator(`${myName}_${i.rate}_rate`, {
                            rules: [{
                              required: true,
                              message: locale === 'en' ? 'Please fill in rate' : '请填写 Rate'
                            }, {
                              validator: forceCheckNum
                            }],
                            initialValue: `${i.rate}`
                          })(
                            <InputNumber size="medium" className="rate-input" min={0} />
                          )}
                        </FormItem>
                      </Col>
                    </div>
                    <div className="topic-info-class">
                      {cardContent}
                    </div>
                    {
                      hasDel ? (
                        <Button shape="circle" type="danger" style={{position: 'absolute', top: '5px', right: '5px'}} onClick={this.toggleItem('delete', myName)}>
                          <Icon type="minus" />
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
        <div className="rate-topic-info-wrapper" style={{backgroundColor: '#ddd', padding: '10px 24px', margin: '10px 0', borderRadius: '10px'}}>
          <div>
            <Col span={24} className="card-content required-offset ">
              Topic Name
            </Col>
            <Col span={24}>
              <FormItem>
                {getFieldDecorator(`newTopicName`, {
                  rules: [{
                    validator: (rule, value, callback) => {
                      if (unValidate) {
                        callback()
                        return
                      }
                      let msg = ''
                      if (value == null || value === '') {
                        msg = locale === 'en' ? 'Please fill in topic name' : '请填写 topic name'
                        callback(msg)
                        return
                      }
                      for (let i = 0, len = userDefinedTopics.length; i < len; i++) {
                        if (userDefinedTopics[i].name === value) {
                          msg = locale === 'en' ? 'The topic name has already existed!' : 'Topic name 已存在'
                          callback(msg)
                          return
                        }
                      }
                      callback()
                    }
                  }]
                })(
                  <Input />
                )}
              </FormItem>
            </Col>
          </div>
          <div className="rate-class" style={{flexGrow: 1}}>
            <Col offset={22} className="card-content ">
              Add
            </Col>
            <Col offset={22}>
              <Button shape="circle" type="default" onClick={this.toggleItem('add')}>
                <Icon type="plus" />
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
            <Collapse defaultActiveKey={['auto', 'user']}>
              <Panel header={autoRegisteredTopicsCardTitle} key="auto">
                {itemFactory(data)}
              </Panel>
              <Panel header={userDefinedTopicsCardTitle} key="user">
                {userDefinedTopicsCardAddItem}
                {userItemFactory(userDefinedTopics)}
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
  emitStartFormDataFromSub: PropTypes.func
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

function mapDispatchToProps (dispatch) {
  return {
    onPostUserTopic: (projectId, streamId, topic, resolve, reject) => dispatch(postUserTopic(projectId, streamId, topic, resolve, reject))
  }
}
export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, mapDispatchToProps)(StreamStartForm))
