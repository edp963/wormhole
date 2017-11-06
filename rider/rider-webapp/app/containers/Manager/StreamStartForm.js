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

import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Card from 'antd/lib/card'
import Select from 'antd/lib/select'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
const FormItem = Form.Item

export class StreamStartForm extends React.Component {
  forceCheckTopic = (rule, value, callback) => {
    const reg = /^[0-9]*$/
    if (reg.test(value)) {
      callback()
    } else {
      callback('必须是数字')
    }
  }

  render () {
    // const data = [{
    //   id: 8,
    //   rate: 100,
    //   partitionOffsets: '0:100,1:111,2:222'
    // },{
    //   id: 24,
    //   rate: 200,
    //   partitionOffsets: '0:100'
    // }, {
    //   id: 25,
    //   rate: 300,
    //   partitionOffsets: '0:200,1:300'
    // }]

    const { form, data, udfValsOption } = this.props
    const { getFieldDecorator } = form

    const noTopicCardTitle = (<Col span={24} style={{fontWeight: '500'}}><span className="modal-topic-name">Topic Name</span></Col>)

    const cardStartItem = data.length === 0
      ? (
        <Row>
          <Card title={noTopicCardTitle} className="stream-start-form-card-style">
            <div className="rate-class">
              <Col span={24} className="card-content required-offset">Rate (条/秒)</Col>
            </div>
            <div className="topic-info-class">
              <Col span={8} className="card-content">Partition</Col>
              <Col span={8} className="card-content required-offset">Offset</Col>
              <Col span={8} className="card-content">Lastest Offset</Col>
              <h3 className="no-topic-class">There is no topics now.</h3>
            </div>
          </Card>
        </Row>
      )
      : data.map(i => {
        let parOffInput = ''

        if (i.partitionOffsets === '') {
          const partitionArr = []
          for (let m = 0; m < i.partition; m++) {
            partitionArr.push(m)
          }
          parOffInput = partitionArr.map((k, index) => (
            <Row key={`${i.id}_${index}`}>
              <Col span={12} className="partition-content">{k}</Col>
              <Col span={12} className="offset-content">
                <FormItem>
                  <ol key={k}>
                    {getFieldDecorator(`${i.id}_${index}`, {
                      rules: [{
                        required: true,
                        message: '请填写 Offset'
                      }]
                    })(
                      <Input className="conform-table-input" />
                    )}
                  </ol>
                </FormItem>
              </Col>
            </Row>
          ))
        } else {
          const partitionOffsetsArr = i.partitionOffsets.split(',')
          parOffInput = partitionOffsetsArr.map((g, index) => (
            <Row key={`${i.id}_${index}`}>
              <Col span={8} className="partition-content">{g.substring(0, g.indexOf(':'))}</Col>
              <Col span={8} className="offset-content">
                <FormItem>
                  <ol key={g}>
                    {getFieldDecorator(`${i.id}_${index}`, {
                      rules: [{
                        required: true,
                        message: '请填写 Offset'
                      }, {
                        validator: this.forceCheckTopic
                      }],
                      initialValue: g.substring(g.indexOf(':') + 1)
                    })(
                      <InputNumber size="medium" className="conform-table-input" />
                    )}
                  </ol>
                </FormItem>
              </Col>
              <Col span={8} className="stream-start-offset-class">
                <FormItem>
                  <ol key={g}>
                    {getFieldDecorator(`latest_${i.id}_${index}`, {
                      initialValue: g.substring(g.indexOf(':') + 1)
                    })(
                      <InputNumber size="medium" className="conform-table-input" disabled />
                    )}
                  </ol>
                </FormItem>
              </Col>
            </Row>
          ))
        }

        const cardTitle = (
          <Row key={i.id}>
            <Col span={24} style={{fontWeight: '500'}}><span className="modal-topic-name">Topic Name</span>{i.name}</Col>
          </Row>
        )

        const cardContent = (
          <Row key={i.id}>
            <Col span={8} className="card-content">Partition</Col>
            <Col span={8} className="card-content required-offset">Offset</Col>
            <Col span={8} className="card-content">Lastest Offset</Col>
            {parOffInput}
          </Row>
        )

        return (
          <Row key={i.id}>
            <Card title={cardTitle} className="stream-start-form-card-style">
              <div className="rate-class">
                <Col span={24} className="card-content required-offset">Rate (条/秒)</Col>
                <Col span={24}>
                  <FormItem>
                    {getFieldDecorator(`${i.rate}`, {
                      rules: [{
                        required: true,
                        message: '请填写 Rate'
                      }, {
                        validator: this.forceCheckTopic
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
            </Card>
          </Row>
        )
      })

    const itemStyleUdf = {
      // labelCol: { span:  },
      wrapperCol: { span: 24 }
    }

    const udfChildren = udfValsOption.map(i => (<Select.Option key={i.id} value={`${i.id}`}>{i.functionName}</Select.Option>))

    return (
      <Form>
        <Row>
          <Card title="UDFs：" className="stream-start-form-udf-style">
            <Col span={24} className="stream-udf">
              <FormItem label="" {...itemStyleUdf}>
                {getFieldDecorator('udfs', {})(
                  <Select
                    mode="multiple"
                    placeholder="Select UDFs"
                  >
                    {udfChildren}
                  </Select>
                )}
              </FormItem>
            </Col>
          </Card>
        </Row>
        {cardStartItem}
      </Form>
    )
  }
}

StreamStartForm.propTypes = {
  form: React.PropTypes.any,
  data: React.PropTypes.array,
  udfValsOption: React.PropTypes.array
}

export default Form.create({wrappedComponentRef: true})(StreamStartForm)
