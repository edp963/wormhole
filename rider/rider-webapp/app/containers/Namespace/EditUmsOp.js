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
const FormItem = Form.Item
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'

export class EditUmsOp extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }

  render () {
    const { getFieldDecorator } = this.props.form

    return (
      <Form>
        <Row gutter={1} style={{ marginBottom: '-25px', marginTop: '5px' }}>
          <Col span={8}>
            <FormItem>
              {getFieldDecorator('insert', {})(
                <Input placeholder="Insert" />
              )}
            </FormItem>
          </Col>
          <Col span={8}>
            <FormItem>
              {getFieldDecorator('update', {})(
                <Input placeholder="Update" />
              )}
            </FormItem>
          </Col>
          <Col span={8}>
            <FormItem>
              {getFieldDecorator('delete', {})(
                <Input placeholder="Delete" />
              )}
            </FormItem>
          </Col>
        </Row>
      </Form>
    )
  }
}

EditUmsOp.propTypes = {
  form: React.PropTypes.any
}

export default Form.create({wrappedComponentRef: true})(EditUmsOp)
