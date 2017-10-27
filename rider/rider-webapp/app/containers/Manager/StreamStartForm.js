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
import Button from 'antd/lib/button'
import Tooltip from 'antd/lib/tooltip'
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
    const { form, data } = this.props
    const { getFieldDecorator } = form

    const itemStyle = {
      labelCol: { span: 11 },
      wrapperCol: { span: 12 }
    }

    const cardStartItem = data.map(i => {
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
            <Col span={11} className="partition-content">{g.substring(0, g.indexOf(':'))}</Col>
            <Col span={11} className="offset-content">
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
            <Col span={2}>
              <Tooltip title="查看最新 Offset">
                <Button shape="circle" type="ghost">
                  <i className="iconfont icon-topiconresourcelist"></i>
                </Button>
              </Tooltip>
            </Col>
          </Row>
        ))
      }

      const cardTitle = (
        <Row key={i.id}>
          <Col span={24}><span className="modal-topic-name">Topic Name</span>{i.name}</Col>
          <Col span={24}>
            <FormItem label="Rate" {...itemStyle}>
              {getFieldDecorator(`${i.id}`, {
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
        </Row>
      )

      const cardContent = (
        <Row key={i.id}>
          <Col span={11} className="card-content">Partition</Col>
          <Col span={11} className="card-content required-offset">Offset</Col>
          <Col span={2} className="card-content">Action</Col>
          {parOffInput}
        </Row>
      )

      return (
        <Row key={i.id}>
          <Card title={cardTitle} className="stream-start-form-card-style">
            {cardContent}
          </Card>
        </Row>
      )
    })

    const itemStyleUdf = {
      // labelCol: { span:  },
      wrapperCol: { span: 24 }
    }

    // const udfChildren = []
    // for (let i = 0; i < topicsValues.length; i++) {
    //   topicChildren.push(<Option key={topicsValues[i].id} value={`${topicsValues[i].id}`}>{topicsValues[i].name}</Option>)
    // }

    // const udfChildren = topicsValues.map(i => (<Option key={i.id} value={`${i.id}`}>{i.name}</Option>))

    return (
      <Form>
        <Row>
          <Card title="UDFS：" className="stream-start-form-udf-style">
            <Col span={24} className="stream-udf">
              <FormItem label="" {...itemStyleUdf}>
                {getFieldDecorator('udfs', {})(
                  <Select
                    mode="multiple"
                    placeholder="Select UDFS"
                    // onChange={this.handleUdfsChange}
                  >
                    {/* {udfChildren} */}
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
  data: React.PropTypes.array
}

export default Form.create({wrappedComponentRef: true})(StreamStartForm)
