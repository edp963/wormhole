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
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
const FormItem = Form.Item
import Select from 'antd/lib/select'
const Option = Select.Option
import { selectLocale } from '../LanguageProvider/selectors'
import { loadEmailInputValue } from './action'

export class UserForm extends React.Component {
  checkEmail = (rule, value, callback) => {
    const { type, locale } = this.props
    if (type === 'add') {
      const emailtext = locale === 'en' ? 'This email already exists' : '该 Email 已存在'
      this.props.onLoadEmailInputValue(value, res => callback(), () => {
        callback(emailtext)
      })
    } else {
      callback()
    }
  }

  checkPasswordConfirm = (rule, value, callback) => {
    const { locale } = this.props
    if (value && value !== this.props.form.getFieldValue('password')) {
      callback(locale === 'en' ? 'The password you entered is inconsistent with the former' : '两次输入的密码不一致')
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

  render () {
    const { getFieldDecorator } = this.props.form
    const { type, locale } = this.props

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

    const itemStyle = {
      labelCol: { span: 7 },
      wrapperCol: { span: 15 }
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
                  message: locale === 'en' ? 'Email cannot be empty' : 'Email 不能为空'
                }, {
                  type: 'email',
                  message: locale === 'en' ? 'Incorrect Email format' : 'Email 格式不正确'
                }, {
                  validator: this.checkEmail
                }]
              })(
                <Input
                  placeholder={locale === 'en' ? 'Email for login' : '用于登录的 Email 地址'}
                  disabled={type === 'editMsg'}
                />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label={<FormattedMessage {...messages.userPassword} />} {...itemStyle} className={msgModalInputClassName}>
              {getFieldDecorator('password', {
                rules: [{
                  required: true,
                  message: locale === 'en' ? 'Password cannot be empty' : 'Password 不能为空'
                }, {
                  min: 6,
                  max: 20,
                  message: locale === 'en' ? 'The password length should be 6-20 characters' : '密码长度为6-20位'
                }, {
                  validator: this.forceCheckConfirm
                }],
                hidden: msgModalInput
              })(
                <Input
                  type="password"
                  placeholder={locale === 'en' ? 'The password length should be 6-20 characters' : '密码长度为6-20位'}
                />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label={<FormattedMessage {...messages.userSurePassword} />} {...itemStyle} className={msgModalInputClassName}>
              {getFieldDecorator('confirmPassword', {
                rules: [{
                  required: true,
                  message: locale === 'en' ? 'Please Re-enter password' : '请确认密码'
                }, {
                  validator: this.checkPasswordConfirm
                }],
                hidden: msgModalInput
              })(
                <Input
                  type="password"
                  placeholder={locale === 'en' ? 'enter your password again' : '确认密码'}
                />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label={<FormattedMessage {...messages.userModalName} />} {...itemStyle} className={pswModalInputClassName}>
              {getFieldDecorator('name', {
                rules: [{
                  required: true,
                  message: locale === 'en' ? 'Please fill in name' : '请输入姓名'
                }],
                initialValue: '',
                hidden: pswModalInput
              })(
                <Input
                  placeholder={locale === 'en' ? 'User Name' : '用户姓名'}
                />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label={<FormattedMessage {...messages.userModalRoleType} />} {...itemStyle} className={pswModalInputClassName}>
              {getFieldDecorator('roleType', {
                rules: [{
                  required: true,
                  message: locale === 'en' ? 'Please select user type' : '请选择用户类型'
                }],
                hidden: pswModalInput
              })(
                <Select
                  style={{ width: '305px' }}
                  placeholder={locale === 'en' ? 'Select user type' : '选择用户类型'}
                  disabled={type !== 'add'}>
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
  form: PropTypes.any,
  type: PropTypes.string,
  onLoadEmailInputValue: PropTypes.func,
  locale: PropTypes.string
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadEmailInputValue: (value, resolve, reject) => dispatch(loadEmailInputValue(value, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, mapDispatchToProps)(UserForm))
