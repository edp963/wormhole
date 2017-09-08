/*-
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
import { connect } from 'react-redux'

import '../../../node_modules/particles.js/particles'
import Form from 'antd/lib/form'
const FormItem = Form.Item
import Input from 'antd/lib/input'
import Button from 'antd/lib/button'
import message from 'antd/lib/message'

import { login } from './action'

export class Login extends React.PureComponent {
  componentWillMount () {
    if (localStorage.getItem('token')) {
      this.props.router.push('/projects')
    }
  }

  componentDidMount () {
    window.particlesJS('loginBg', require('../../assets/json/particlesjs-config.json'))
  }

  doLogin = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const logoInfo = {
          email: values.userName,
          password: values.password
        }
        this.props.onLogin(logoInfo, () => {
          this.props.router.push('/projects')
        }, (result) => {
          if (result === 'Not found') {
            message.error('用户不存在！', 3)
          } else if (result === 'Wrong password') {
            message.error('密码错误！', 3)
          } else if (result === 'app type user has no permission to login') {
            message.error('App 类型的用户不能登录！', 3)
          }
        })
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form

    return (
      <div className="login-container">
        <div className="login-panel">
          <h2 className="login-title">Rider 登录</h2>
          <Form>
            <FormItem>
              {getFieldDecorator('userName', {
                rules: [{
                  required: true,
                  message: '邮箱不能为空'
                }]
              })(
                <Input placeholder="邮箱" />
              )}
            </FormItem>
            <FormItem>
              {getFieldDecorator('password', {
                rules: [{
                  required: true,
                  message: '密码不能为空'
                }]
              })(
                <Input type="password" placeholder="密码" />
              )}
            </FormItem>
          </Form>
          <Button size="large" onClick={this.doLogin}>登 录</Button>
        </div>
        <div id="loginBg"></div>
      </div>
    )
  }
}

Login.propTypes = {
  form: React.PropTypes.any,
  router: React.PropTypes.any,
  onLogin: React.PropTypes.func
}

export function mapDispatchToProps (dispatch) {
  return {
    onLogin: (logoInfo, resolve, reject) => dispatch(login(logoInfo, resolve, reject))
  }
}

export default connect(null, mapDispatchToProps)(Form.create()(Login))
