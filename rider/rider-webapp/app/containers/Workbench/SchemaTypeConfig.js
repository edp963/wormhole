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
import Select from 'antd/lib/select'
const FormItem = Form.Item

export class SchemaTypeConfig extends React.Component {
  render () {
    const { form } = this.props
    const { getFieldDecorator } = form

    const itemStyle = {
      labelCol: { span: 4 },
      wrapperCol: { span: 19 }
    }

    return (
      <Form>
        <Row>
          <Col span={24}>
            <FormItem label="已有 Flow：" {...itemStyle}>
              {getFieldDecorator('exitedFlow', {
                rules: [{
                  required: true,
                  message: '不能为空'
                }]
              })(
                <Select
                  dropdownClassName="ri-workbench-select-dropdown"
                  onSelect={this.onHandleChangeFlow}
                  placeholder="Select a Source to Sink"
                >
                  {/* {sourceToSinkOptions} */}
                </Select>
              )}
            </FormItem>
          </Col>
        </Row>
      </Form>
    )
  }
}

SchemaTypeConfig.propTypes = {
  form: React.PropTypes.any
}

export default Form.create({wrappedComponentRef: true})(SchemaTypeConfig)
