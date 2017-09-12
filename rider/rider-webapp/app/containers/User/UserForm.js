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
import Input from 'antd/lib/input'
const FormItem = Form.Item
import Select from 'antd/lib/select'
const Option = Select.Option

export class UserForm extends React.Component {
  onEmailInputChange = (e) => {
    this.props.onInitEmailInputValue(e.target.value)
  }

  checkPasswordConfirm = (rule, value, callback) => {
    if (value && value !== this.props.form.getFieldValue('password')) {
      callback('两次输入的密码不一致')
    } else {
      callback()
    }
  }

  forceCheckConfirm = (rule, value, callback) => {
    const { form } = this.props
    if (form.getFieldValue('confirmPassword')) {
      form.validateFields(['confirmPassword'], { force: true })
    }
    callback()
  }

  handleChange = (value) => {
    // console.log('select:', value)
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { type } = this.props

    let msgModalInput = false
    let pswModalInput = false

    let msgModalInputClassName = ''
    let pswModalInputClassName = ''

    if (type === 'editMsg') {
      msgModalInput = true
      msgModalInputClassName = msgModalInput ? 'hide' : ''
    } else if (type === 'editPsw') {
      pswModalInput = true
      pswModalInputClassName = pswModalInput ? 'hide' : ''
    }

    const rolrTypeDisabledOrNot = type !== 'add'

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 16 }
    }

    return (
      <Form>
        <Row gutter={8}>
          <Col span={24}>
            <FormItem className="hide">
              {getFieldDecorator('id', {
                hidden: type === 'add'
              })(
                <Input />
              )}
            </FormItem>
            <FormItem label="Email" {...itemStyle} className={pswModalInputClassName}>
              {getFieldDecorator('email', {
                rules: [{
                  required: true,
                  message: 'Email 不能为空'
                }, {
                  type: 'email',
                  message: 'Email 格式不正确'
                }]
              })(
                <Input
                  placeholder="用于登录的 Email 地址"
                  disabled={type === 'editMsg'}
                  onChange={this.onEmailInputChange}
                />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="密 码" {...itemStyle} className={msgModalInputClassName}>
              {getFieldDecorator('password', {
                rules: [{
                  required: true,
                  message: '密码不能为空'
                }, {
                  min: 6,
                  max: 20,
                  message: '密码长度为6-20位'
                }, {
                  validator: this.forceCheckConfirm
                }],
                hidden: msgModalInput
              })(
                <Input type="password" placeholder="密码长度为 6-20 位" />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="确认密码" {...itemStyle} className={msgModalInputClassName}>
              {getFieldDecorator('confirmPassword', {
                rules: [{
                  required: true,
                  message: '请确认密码'
                }, {
                  validator: this.checkPasswordConfirm
                }],
                hidden: msgModalInput
              })(
                <Input type="password" placeholder="确认密码" />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="姓 名" {...itemStyle} className={pswModalInputClassName}>
              {getFieldDecorator('name', {
                rules: [{
                  required: true,
                  message: '请输入姓名'
                }],
                initialValue: '',
                hidden: pswModalInput
              })(
                <Input placeholder="用户姓名" />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="用户类型" {...itemStyle} className={pswModalInputClassName}>
              {getFieldDecorator('roleType', {
                rules: [{
                  required: true,
                  message: '请选择用户类型'
                }],
                hidden: pswModalInput
              })(
                <Select style={{ width: '246px' }} placeholder="选择用户类型" onChange={this.handleChange} disabled={rolrTypeDisabledOrNot}>
                  <Option value="admin">admin</Option>
                  <Option value="user">user</Option>
                  <Option value="app">app</Option>
                </Select>
              )}
            </FormItem>
          </Col>
        </Row>
      </Form>
    )
  }
}

UserForm.propTypes = {
  form: React.PropTypes.any,
  type: React.PropTypes.string,
  onInitEmailInputValue: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(UserForm)
