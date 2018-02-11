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

// import { formatConcat } from '../../utils/util'

export class Source2LogForm extends React.Component {

  constructor (props) {
    super(props)
    this.state = {
      flowId: '',
      keyColumn: '',
      connectUrl: '',
      createTsColumn: '',
      updateTsColumn: '',
      logTimeModalVisible: false
    }
  }

  showLogTimeModal = () => {
    // validateFieldsAndScroll 校验并获取一组输入域的值与Error
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (values.keyColumn !== '') {
          this.setState({
            logTimeModalVisible: true
          })
        }
      }
    })
  }

  handleLogTimeCancel = (e) => this.setState({ logTimeModalVisible: false })

  handleLogTimeOk = () => {
    if (this.flowsTime.state.startValue === null) {
      message.warning('开始时间不能为空！')
    } else if (this.flowsTime.state.endValue === null) {
      message.warning('结束时间不能为空！')
    } else {
      // const sVal = new Date(this.flowsTime.state.startValue._d)
      // const eVal = new Date(this.flowsTime.state.endValue._d)
      // const temp = formatConcat(sVal, eVal)

      this.props.form.validateFieldsAndScroll((err, values) => {
        if (!err) {
          // this.props.onCheckOutForm(this.props.initialFlowId, 'source2hdfs', temp.startDate, temp.endDate, values, () => {
          //   message.success('自动校验成功！', 5)
          // })
        }
      })
      this.setState({ logTimeModalVisible: false })
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
          <Button type="primary" className="temp-save-style" onClick={this.showLogTimeModal}><Icon type="caret-right" />手动校验</Button>
          <Button type="primary" className="temp-save-style" onClick={this.props.onSaveLogClick}>保存</Button>
        </div>

        <Modal
          title="设置时间"
          visible={this.state.logTimeModalVisible}
          onCancel={this.handleLogTimeCancel}
          onOk={this.handleLogTimeOk}
        >
          <FlowsTime
            ref={(f) => { this.flowsTime = f }}
          />
        </Modal>
      </div>
    )
  }
}

Source2LogForm.propTypes = {
  form: React.PropTypes.any,
  onSaveLogClick: React.PropTypes.func
  // onCheckOutForm: React.PropTypes.func,
  // initialFlowId: React.PropTypes.number
}

export default Form.create({wrappedComponentRef: true})(Source2LogForm)
