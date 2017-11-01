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
import InputNumber from 'antd/lib/input-number'
import Card from 'antd/lib/card'
const FormItem = Form.Item

export class ProjectForm extends React.Component {
  forceCheckProjectName = (rule, value, callback) => {
    const reg = /^[\w-]+$/
    if (reg.test(value)) {
      callback()
    } else {
      callback('必须是字母、数字、中划线或下划线')
    }
  }

  forceCheckRes = (rule, value, callback) => {
    const reg = /^[0-9]*$/
    if (reg.test(value)) {
      callback()
    } else {
      callback('必须是数字')
    }
  }

  // 验证project name 是否存在
  onProjectNameInputChange = (e) => {
    this.props.onInitProjectNameInputValue(e.target.value)
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { projectFormType } = this.props

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 16 }
    }

    // edit 时，不能修改部分元素
    let disabledOrNot = false
    if (projectFormType === 'add') {
      disabledOrNot = false
    } else if (projectFormType === 'edit') {
      disabledOrNot = true
    }

    return (
      <Form>
        <Row gutter={8}>
          <Col span={24}>
            <Card title="项目信息" className="project-form-card-style project-form-card-style-left">
              <FormItem className="hide">
                {getFieldDecorator('id', {
                  hidden: projectFormType === 'add'
                })(
                  <Input />
                )}
              </FormItem>
              <FormItem className="hide">
                {getFieldDecorator('pic', {
                  hidden: projectFormType === 'add'
                })(
                  <Input />
                )}
              </FormItem>
              <FormItem label="项目标识" {...itemStyle}>
                {getFieldDecorator('name', {
                  rules: [{
                    required: true,
                    message: '项目名称不能为空'
                  }, {
                    validator: this.forceCheckProjectName
                  }]
                })(
                  <Input
                    placeholder="由大小写字母、中划线、下划线、数字组成"
                    onChange={this.onProjectNameInputChange}
                    disabled={disabledOrNot}
                  />
                )}
              </FormItem>
              <FormItem label="项目描述" {...itemStyle}>
                {getFieldDecorator('desc', {})(
                  <Input placeholder="项目详情描述" />
                )}
              </FormItem>
            </Card>
          </Col>

          <Col span={24}>
            <Card title="项目资源" className="project-form-card-style project-form-card-style-right">
              <FormItem label="CPU上限" {...itemStyle}>
                {getFieldDecorator('resCores', {
                  rules: [{
                    required: true,
                    message: 'CPU上限不能为空'
                  }, {
                    validator: this.forceCheckRes
                  }]
                })(
                  <InputNumber min={1} step={1} placeholder="VCores 个数" />
                )}
              </FormItem>
              <FormItem label="内存上限" {...itemStyle}>
                {getFieldDecorator('resMemoryG', {
                  rules: [{
                    required: true,
                    message: '内存上限不能为空'
                  }, {
                    validator: this.forceCheckRes
                  }]
                })(
                  <InputNumber min={1} step={1} placeholder="GB" />
                )}
              </FormItem>
            </Card>
          </Col>
        </Row>
      </Form>
    )
  }
}

ProjectForm.propTypes = {
  form: React.PropTypes.any,
  projectFormType: React.PropTypes.string,
  onInitProjectNameInputValue: React.PropTypes.func
}

export default Form.create({wrappedComponentRef: true})(ProjectForm)
