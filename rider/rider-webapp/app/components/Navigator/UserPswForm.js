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
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
const FormItem = Form.Item

export class UserPswForm extends React.Component {
  checkPasswordConfirm = (rule, value, callback) => {
    const languageText = localStorage.getItem('preferredLanguage')
    if (value && value !== this.props.form.getFieldValue('password')) {
      callback(languageText === 'en' ? 'The password you entered is inconsistent with the former' : '两次输入的密码不一致')
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
    const languageText = localStorage.getItem('preferredLanguage')
    const pwdText = languageText === 'en' ? 'The password length should be 6-20 characters' : '密码长度为6-20位'

    const itemStyle = {
      labelCol: { span: 8 },
      wrapperCol: { span: 15 }
    }

    return (
      <Form>
        <Row gutter={8}>
          <Col span={24}>
            <FormItem label={<FormattedMessage {...messages.navOldPsw} />} {...itemStyle}>
              {getFieldDecorator('oldPassword', {
                rules: [{
                  required: true,
                  message: languageText === 'en' ? 'Password cannot be empty' : '密码不能为空'
                }, {
                  min: 6,
                  max: 20,
                  message: pwdText
                }, {
                  validator: this.forceCheckConfirm
                }]
              })(
                <Input type="password" placeholder={pwdText} />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label={<FormattedMessage {...messages.navNewPsw} />} {...itemStyle}>
              {getFieldDecorator('password', {
                rules: [{
                  required: true,
                  message: languageText === 'en' ? 'Password cannot be empty' : '密码不能为空'
                }, {
                  min: 6,
                  max: 20,
                  message: pwdText
                }, {
                  validator: this.forceCheckConfirm
                }]
              })(
                <Input type="password" placeholder={pwdText} />
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label={<FormattedMessage {...messages.navSureNewPsw} />} {...itemStyle}>
              {getFieldDecorator('confirmPassword', {
                rules: [{
                  required: true,
                  message: languageText === 'en' ? 'Please confirm new password' : '请确认新密码'
                }, {
                  validator: this.checkPasswordConfirm
                }]
              })(
                <Input
                  type="password"
                  placeholder={languageText === 'en' ? 'Confirm new password' : '确认新密码'} />
              )}
            </FormItem>
          </Col>
        </Row>
      </Form>
    )
  }
}

UserPswForm.propTypes = {
  form: PropTypes.any
}

export default Form.create({wrappedComponentRef: true})(UserPswForm)
