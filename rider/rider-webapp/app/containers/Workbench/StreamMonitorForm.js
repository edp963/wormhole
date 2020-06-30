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

import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
const FormItem = Form.Item
import { selectLocale } from '../LanguageProvider/selectors'
import {Switch} from 'antd'
import Input from 'antd/lib/input'

export class StreamMonitorForm extends React.Component {
  render () {
    const { form, streamSubPanelKey } = this.props
    const { getFieldDecorator } = form

    const itemStyleOthers = {
      labelCol: { span: 12 },
      wrapperCol: { span: 12 }
    }

    return (
      <Form>
        {streamSubPanelKey === 'spark' || streamSubPanelKey === 'flink' ? (
          <Row>
            <Col span={12}>
              <FormItem label="Email To User When Stream Failed：" {...itemStyleOthers}>
                {getFieldDecorator('monitorToEmail', {
                  valuePropName: 'checked',
                  defaultValue: false
                })(
                  <Switch />
                )}
              </FormItem>
            </Col>
          </Row>
          ) : ''
        }
        {
          <Row>
            <Col span={12}>
              <FormItem label="Restart Stream When Failed：" {...itemStyleOthers}>
                {getFieldDecorator('monitorToRestart', {
                  valuePropName: 'checked',
                  defaultValue: false
                })(
                  <Switch />
                )}
              </FormItem>
            </Col>
          </Row>
        }
        {
          <Row>
            <Col span={12}>
              <FormItem label="Send Dingding When Stream Failed：" {...itemStyleOthers}>
                {getFieldDecorator('monitorToDingding', {
                  rules: [{
                    defaultValue: ''
                  }]
                })(
                  <Input placeholder="Dingding Token" />
                )}
              </FormItem>
            </Col>
          </Row>
        }
      </Form>
    )
  }
}

StreamMonitorForm.propTypes = {
  form: PropTypes.any,
  streamSubPanelKey: PropTypes.string
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, null)(StreamMonitorForm))
