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

import '../../../node_modules/particles.js/particles'
import Form from 'antd/lib/form'
const FormItem = Form.Item
import Input from 'antd/lib/input'
import Button from 'antd/lib/button'
import message from 'antd/lib/message'

import { login } from '../App/actions'
import { selectLocale } from '../../containers/LanguageProvider/selectors'
import { changeLocale } from '../../containers/LanguageProvider/actions'

export class Login extends React.PureComponent {
  componentWillMount () {
    if (localStorage.getItem('token')) {
      this.props.router.push('/projects')
    }
  }

  componentDidMount () {
    window.particlesJS('loginBg', require('../../assets/json/particlesjs-config.json'))

    // 当前组件必须处于被激活选中状态键盘事件才生效
    window.addEventListener('keydown', this.handleKeyDown())
  }

  // 点击登录按钮登录
  doLogin = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const logoInfo = {
          email: values.userName,
          password: values.password
        }
        this.loginRequestApi(logoInfo)
      }
    })
  }

  // 键盘回车登录
  handleKeyDown = (e) => {
    if (e) {
      // IE、Chrome 获取敲击键盘值：e.keyCode，是否在方法中传入event事件参数无所谓
      // Firefox、Opera 获取敲击键盘值的条件：1、需要在方法中传入event事件参数，2、e.which 获得键盘索引值
      const pressKey = e.keyCode || e.which
      if (pressKey === 13) {
        e.returnValue = false
        e.cancel = true

        const userNameVal = this.props.form.getFieldValue('userName')
        const passwordVal = this.props.form.getFieldValue('password')

        if (userNameVal !== undefined && passwordVal !== undefined) {
          const logoInfo = {
            email: userNameVal,
            password: passwordVal
          }
          this.loginRequestApi(logoInfo)
        }
      }
    }
  }

  loginRequestApi (logoInfo) {
    this.props.onLogin(logoInfo, (result) => {
      this.props.router.push('/projects')

      const langText = result.preferredLanguage === 'chinese' ? 'zh' : 'en'
      this.props.onChangeLanguage(langText)
    }, (result) => {
      const userText = result.preferredLanguage === 'english' ? 'User does not exist!' : '用户不存在！'
      const pswText = result.preferredLanguage === 'english' ? 'Incorrect password!' : '密码错误！'
      const typeText = result.preferredLanguage === 'english' ? 'User of App type cannot login!' : 'App 类型的用户不能登录！'

      switch (result) {
        case 'Not found':
          message.error(userText, 3)
          break
        case 'Wrong password':
          message.error(pswText, 3)
          break
        case 'app type user has no permission to login':
          message.error(typeText, 3)
          break
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const languageText = localStorage.getItem('preferredLanguage')
    return (
      <div className="login-container">
        <div className="login-panel">
          <h2 className="login-title">Wormhole Rider</h2>
          <Form name="Form1">
            <FormItem>
              {getFieldDecorator('userName', {
                rules: [{
                  required: true,
                  message: languageText === 'en' ? 'User name cannot be empty' : '用户名不能为空'
                }]
              })(
                <Input placeholder="User Name" onKeyDown={this.handleKeyDown} />
              )}
            </FormItem>
            <FormItem>
              {getFieldDecorator('password', {
                rules: [{
                  required: true,
                  message: languageText === 'en' ? 'Password cannot be empty' : '密码不能为空'
                }]
              })(
                <Input type="password" placeholder="Password" onKeyDown={this.handleKeyDown} />
              )}
            </FormItem>
          </Form>
          <Button size="large" onClick={this.doLogin}>Sign in</Button>

        </div>
        <div id="loginBg"></div>
      </div>
    )
  }
}

Login.propTypes = {
  form: PropTypes.any,
  router: PropTypes.any,
  onLogin: PropTypes.func,
  onChangeLanguage: PropTypes.func
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export function mapDispatchToProps (dispatch) {
  return {
    onLogin: (logoInfo, resolve, reject) => dispatch(login(logoInfo, resolve, reject)),
    onChangeLanguage: (type) => dispatch(changeLocale(type))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(Form.create()(Login))
