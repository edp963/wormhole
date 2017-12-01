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
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
import message from 'antd/lib/message'

export class EditUmsOp extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      umsOpString: ''
    }
  }

  checkUmsOp = () => {
    const umsTemp = this.props.form.getFieldsValue()
    const addTemp = umsTemp.add
    const uptateTemp = umsTemp.update
    const deleteTemp = umsTemp.delete

    let umsString = '' // 8种组合
    if (addTemp && addTemp !== '') {
      if (uptateTemp && uptateTemp !== '') {
        umsString = (deleteTemp && deleteTemp !== '')
          ? `i:${umsTemp.add},u:${umsTemp.update},d:${umsTemp.delete}`
          : `i:${umsTemp.add},u:${umsTemp.update}`
      } else {
        umsString = (deleteTemp && deleteTemp !== '')
          ? `i:${umsTemp.add},d:${umsTemp.delete}`
          : `i:${umsTemp.add}`
      }
    } else {
      if (uptateTemp && uptateTemp !== '') {
        umsString = (deleteTemp && deleteTemp !== '')
          ? `u:${umsTemp.update},d:${umsTemp.delete}`
          : `u:${umsTemp.update}`
      } else {
        umsString = (deleteTemp && deleteTemp !== '')
          ? `d:${umsTemp.delete}`
          : ''
      }
    }

    if (umsString === '') {
      message.warning('UMS_OP 值不为空！', 3)
    } else {
      this.props.initCheckUmsOp(umsString)
      this.setState({
        umsOpString: umsString
      })
    }
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { umsOpString } = this.state
    const itemStyle = {
      // labelCol: { span: 6 },
      wrapperCol: { span: 24 }
    }

    const temp = umsOpString === ''
      ? (
        <Row gutter={1}>
          <Col span={7}>
            <FormItem {...itemStyle}>
              {getFieldDecorator('add', {})(
                <Input
                  placeholder="Add"
                />
              )}
            </FormItem>
          </Col>
          <Col span={7}>
            <FormItem {...itemStyle}>
              {getFieldDecorator('update', {})(
                <Input
                  placeholder="Update"
                />
              )}
            </FormItem>
          </Col>
          <Col span={7}>
            <FormItem {...itemStyle}>
              {getFieldDecorator('delete', {})(
                <Input
                  placeholder="Delete"
                />
              )}
            </FormItem>
          </Col>
          <Col span={1}></Col>
          <Col span={2} className="umsop-check">
            <Icon
              type="check"
              onClick={this.checkUmsOp}
            />
          </Col>
        </Row>
      )
      : umsOpString

    return (
      <Form className="editable-umsop-cell-text-wrapper">
        {temp}
      </Form>
    )
  }
}

EditUmsOp.propTypes = {
  form: React.PropTypes.any,
  initCheckUmsOp: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(EditUmsOp)
