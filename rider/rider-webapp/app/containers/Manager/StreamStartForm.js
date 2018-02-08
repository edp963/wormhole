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

import { forceCheckNum } from '../../utils/util'
import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Card from 'antd/lib/card'
import Tooltip from 'antd/lib/tooltip'
import Button from 'antd/lib/button'
import Select from 'antd/lib/select'
import InputNumber from 'antd/lib/input-number'
const FormItem = Form.Item

export class StreamStartForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = { data: [] }
  }

  componentWillReceiveProps (props) {
    const dataFinal = props.data.map(s => {
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

      return Object.assign({}, conTempObject, kafTempObject)
    })

    this.setState({ data: dataFinal })
  }

  onApplyConOffset = (i, index, consumerOffsetFinal) => (e) => {
    this.props.form.setFieldsValue({
      [`${i.id}_${index}`]: consumerOffsetFinal
    })
  }

  onApplyKafkaOffset = (i, index, kafkaOffsetFinal) => (e) => {
    this.props.form.setFieldsValue({
      [`${i.id}_${index}`]: kafkaOffsetFinal
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
    const { form, streamActionType, startUdfValsOption, renewUdfValsOption, currentUdfVal } = this.props
    const { getFieldDecorator } = form
    const { data } = this.state
    const languageText = localStorage.getItem('preferredLanguage')

    const noTopicCardTitle = (<Col span={24} style={{fontWeight: '500'}}><span className="modal-topic-name">Topic Name</span></Col>)

    const cardStartItem = data.length === 0
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
                <Col span={7} className="card-content">Lastest Consumed Offset</Col>
                <Col span={7} className="card-content">Lastest Kafka Offset</Col>
              </div>
            </div>
            <h3 className="no-topic-class">There is no topics now.</h3>
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
                          message: languageText === 'en' ? 'Please fill in offset' : '请填写 Offset'
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
                            <Button shape="circle" type="ghost" onClick={this.onApplyConOffset(i, index, conOffFinal)}>
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
                            <Button shape="circle" type="ghost" onClick={this.onApplyKafkaOffset(i, index, kafOffFinal)}>
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
              <Col span={7} className="card-content">Lastest Consumed Offset
                <Tooltip title={applyAllText}>
                  <Button shape="circle" type="ghost" onClick={this.onApplyAll(i, 'consumer')}>
                    <i className="iconfont icon-apply_icon_-copy-copy"></i>
                  </Button>
                </Tooltip>
              </Col>
              <Col span={7} className="card-content">Lastest Kafka Offset
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
                          message: languageText === 'en' ? 'Please fill in rate' : '请填写 Rate'
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
        </Row>
        {cardStartItem}
      </Form>
    )
  }
}

StreamStartForm.propTypes = {
  form: React.PropTypes.any,
  streamActionType: React.PropTypes.string,
  startUdfValsOption: React.PropTypes.array,
  renewUdfValsOption: React.PropTypes.array,
  currentUdfVal: React.PropTypes.array
}

export default Form.create({wrappedComponentRef: true})(StreamStartForm)
