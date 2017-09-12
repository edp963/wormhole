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

import FlowsTime from './FlowsTime'
import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Button from 'antd/lib/button'
import Input from 'antd/lib/input'
import Icon from 'antd/lib/icon'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
const FormItem = Form.Item

export class Source2SinkForm extends React.Component {

  constructor (props) {
    super(props)
    this.state = {
      flowId: '',
      keyColumn: '',
      connectUrl: '',
      createTsColumn: '',
      updateTsColumn: '',
      sinkTimeModalVisible: false,
      flowsTime: null
    }
  }

  componentDidMount () {
    // const { form, initialValue } = this.props
    // form.setFieldsValue(initialValue)
  }

  showSinkTimeModal = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (values.keyColumn !== '') {
          this.setState({
            sinkTimeModalVisible: true
          })
        }
      }
    })
  }

  handleSinkTimeCancel = (e) => {
    this.setState({
      sinkTimeModalVisible: false
    })
  }

  handleSinkTimeOk = () => {
    if (this.flowsTime.state.startValue === null) {
      message.warning('开始时间不能为空！')
    } else if (this.flowsTime.state.endValue === null) {
      message.warning('结束时间不能为空！')
    } else {
      const s = new Date(this.flowsTime.state.startValue._d)
      const e = new Date(this.flowsTime.state.endValue._d)

      // 时间格式转换
      // start time
      let monthStringS = ''
      if (s.getMonth() + 1 < 10) {
        monthStringS = `0${s.getMonth() + 1}`
      } else {
        monthStringS = `${s.getMonth()}`
      }

      let dateStringS = ''
      if (s.getDate() < 10) {
        dateStringS = `0${s.getDate()}`
      } else {
        dateStringS = `${s.getDate()}`
      }

      let hourStringS = ''
      if (s.getHours() < 10) {
        hourStringS = `0${s.getHours()}`
      } else {
        hourStringS = `${s.getHours()}`
      }

      let minuteStringS = ''
      if (s.getMinutes() < 10) {
        minuteStringS = `0${s.getMinutes()}`
      } else {
        minuteStringS = `${s.getMinutes()}`
      }

      // end time
      let monthStringE = ''
      if (e.getMonth() + 1 < 10) {
        monthStringE = `0${e.getMonth() + 1}`
      } else {
        monthStringE = `${e.getMonth()}`
      }

      let dateStringE = ''
      if (e.getDate() < 10) {
        dateStringE = `0${e.getDate()}`
      } else {
        dateStringE = `${e.getDate()}`
      }

      let hourStringE = ''
      if (e.getHours() < 10) {
        hourStringE = `0${e.getHours()}`
      } else {
        hourStringE = `${e.getHours()}`
      }

      let minuteStringE = ''
      if (e.getMinutes() < 10) {
        minuteStringE = `0${e.getMinutes()}`
      } else {
        minuteStringE = `${e.getMinutes()}`
      }

      // 时间格式拼接
      const startDate = `${s.getFullYear()}-${monthStringS}-${dateStringS} ${hourStringS}:${minuteStringS}`
      const endDate = `${e.getFullYear()}-${monthStringE}-${dateStringE} ${hourStringE}:${minuteStringE}`

      this.props.form.validateFieldsAndScroll((err, values) => {
        if (!err) {
          console.log(startDate, endDate)
          // this.props.onCheckOutForm(this.props.initialFlowId, 'source2sink', startDate, endDate, values, () => {
          //   message.success('自动校验成功！', 5)
          // })
        }
      })
      this.setState({
        sinkTimeModalVisible: false
      })
    }
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const commonFormItemStyle = {
      labelCol: { span: 8 },
      wrapperCol: { span: 16 }
    }

    return (
      <div className="temp-style">
        <Form>
          <Row>
            <Col span={11}>
              <FormItem label="Flow ID" {...commonFormItemStyle}>
                {getFieldDecorator('flowId', {})(
                  <Input placeholder="Flow ID" disabled className="input-only-read" />
                )}
              </FormItem>
              <FormItem className="hide">
                {getFieldDecorator('taskId', {})(
                  <Input />
                )}
              </FormItem>
            </Col>
            <Col span={11}>
              <FormItem label="Connect Url" {...commonFormItemStyle}>
                {getFieldDecorator('connectUrl', {})(
                  <Input placeholder="Connect Url" />
                )}
              </FormItem>
            </Col>
            <Col span={2}> </Col>
          </Row>
          <Row>
            <Col span={11}>
              <FormItem label="Key Column" {...commonFormItemStyle}>
                {getFieldDecorator('keyColumn', {
                  rules: [{
                    required: true,
                    message: 'Key Column不能为空'
                  }]
                })(
                  <Input placeholder="Key Column" />
                )}
              </FormItem>
            </Col>
            <Col span={11}>
              <FormItem label="Create TS Column" {...commonFormItemStyle}>
                {getFieldDecorator('createTsColumn', {})(
                  <Input placeholder="Create TS Column" />
                )}
              </FormItem>
            </Col>
            <Col span={2}> </Col>
          </Row>
          <Row>
            <Col span={11}>
              <FormItem label="Update TS Column" {...commonFormItemStyle}>
                {getFieldDecorator('updateTsColumn', {})(
                  <Input placeholder="Update TS Column" />
                )}
              </FormItem>
            </Col>
            <Col span={2}> </Col>
          </Row>
        </Form>
        <div className="temp-btn-style">
          <Button type="primary" className="temp-save-style" onClick={this.showSinkTimeModal}><Icon type="caret-right" />手动校验</Button>
          <Button type="primary" className="temp-save-style" onClick={this.props.onSaveSinkClick}>保存</Button>
        </div>

        <Modal
          title="设置时间"
          visible={this.state.sinkTimeModalVisible}
          onCancel={this.handleSinkTimeCancel}
          onOk={this.handleSinkTimeOk}
        >
          <FlowsTime
            ref={(f) => { this.flowsTime = f }}
          />
        </Modal>
      </div>
    )
  }
}

Source2SinkForm.propTypes = {
  form: React.PropTypes.any,
  // initialValue: React.PropTypes.object,
  onSaveSinkClick: React.PropTypes.func
  // initialFlowId: React.PropTypes.number,
  // onCheckOutForm: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(Source2SinkForm)
