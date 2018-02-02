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
import { FormattedMessage } from 'react-intl'
import messages from './messages'
// import PlaceholderInputIntl from '../../components/PlaceholderInputIntl'
// import PlaceholderInputNumberIntl from '../../components/PlaceholderInputNumberIntl'

import { forceCheckNum, forceCheckProjectName } from '../../utils/util'
import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
import Card from 'antd/lib/card'
const FormItem = Form.Item

export class ProjectForm extends React.Component {
  // 验证project name 是否存在
  onProjectNameInputChange = (e) => this.props.onInitProjectNameInputValue(e.target.value)

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
            <Card title={<FormattedMessage {...messages.projectInformation} />} className="project-form-card-style project-form-card-style-left">
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
              <FormItem label={<FormattedMessage {...messages.projectIdentification} />} {...itemStyle}>
                {getFieldDecorator('name', {
                  rules: [{
                    required: true,
                    message: '项目标识不能为空'
                  }, {
                    validator: forceCheckProjectName
                  }]
                })(
                  <Input
                    placeholder="由大小写字母、中划线、下划线、数字组成"
                    onChange={this.onProjectNameInputChange}
                    disabled={disabledOrNot}
                  />
                )}
              </FormItem>
              <FormItem label={<FormattedMessage {...messages.projectDescription} />} {...itemStyle}>
                {getFieldDecorator('desc', {})(
                  <Input placeholder="项目详情描述" />
                )}
              </FormItem>
            </Card>
          </Col>

          <Col span={24}>
            <Card title={<FormattedMessage {...messages.projectResource} />} className="project-form-card-style project-form-card-style-right">
              <FormItem label={<FormattedMessage {...messages.projectCpu} />} {...itemStyle}>
                {getFieldDecorator('resCores', {
                  rules: [{
                    required: true,
                    message: 'CPU上限不能为空'
                  }, {
                    validator: forceCheckNum
                  }]
                })(
                  <InputNumber min={1} step={1} placeholder="VCores 个数" />
                )}
              </FormItem>
              <FormItem label={<FormattedMessage {...messages.projectMemory} />} {...itemStyle}>
                {getFieldDecorator('resMemoryG', {
                  rules: [{
                    required: true,
                    message: '内存上限不能为空'
                  }, {
                    validator: forceCheckNum
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
  // intl: intlShape.isRequired
}

export default Form.create({wrappedComponentRef: true})(ProjectForm)
