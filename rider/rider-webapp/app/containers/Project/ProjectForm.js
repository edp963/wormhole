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
// import PlaceholderInputIntl from '../../components/PlaceholderInputIntl'
// import PlaceholderInputNumberIntl from '../../components/PlaceholderInputNumberIntl'

import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
import Card from 'antd/lib/card'
const FormItem = Form.Item
import { forceCheckNum } from '../../utils/util'
import { loadProjectNameInputValue } from './action'
import { selectLocale } from '../LanguageProvider/selectors'

export class ProjectForm extends React.Component {
  checkProjectName = (rule, value, callback) => {
    const { locale } = this.props
    const reg = /^[\w-]+$/
    if (reg.test(value)) {
      const { projectFormType, onLoadProjectNameInputValue } = this.props
      projectFormType === 'add'
        ? onLoadProjectNameInputValue(value, res => callback(), (err) => callback(err))
        : callback()
    } else {
      const textZh = '必须是字母、数字、下划线或中划线'
      const textEn = 'It should be letters, figures, underscore or hyphen'
      callback(locale === 'en' ? textEn : textZh)
    }
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { projectFormType, locale } = this.props

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 16 }
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
                    message: locale === 'en' ? 'Project name cannot be empty' : '项目标识不能为空'
                  }, {
                    validator: this.checkProjectName
                  }]
                })(
                  <Input
                    placeholder={locale === 'en' ? 'composed of capital/lowercase letters, hyphen, underscore or number' : '由大小写字母、中划线、下划线、数字组成'}
                    disabled={projectFormType === 'edit'}
                  />
                )}
              </FormItem>
              <FormItem label={<FormattedMessage {...messages.projectDescription} />} {...itemStyle}>
                {getFieldDecorator('desc', {})(
                  <Input placeholder={locale === 'en' ? 'description of project details' : '项目详情描述'} />
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
                    message: locale === 'en' ? 'Upper Limit of CPU cannot be empty' : 'CPU上限不能为空'
                  }, {
                    validator: forceCheckNum
                  }]
                })(
                  <InputNumber
                    min={1}
                    step={1}
                    placeholder={locale === 'en' ? 'Number of VCores' : 'VCores 个数'} />
                )}
              </FormItem>
              <FormItem label={<FormattedMessage {...messages.projectMemory} />} {...itemStyle}>
                {getFieldDecorator('resMemoryG', {
                  rules: [{
                    required: true,
                    message: locale === 'en' ? 'Upper Limit of Memory cannot be empty' : '内存上限不能为空'
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
  form: PropTypes.any,
  projectFormType: PropTypes.string,
  onLoadProjectNameInputValue: PropTypes.func,
  locale: PropTypes.string
}

function mapDispatchToProps (dispatch) {
  return {
    onLoadProjectNameInputValue: (value, resolve, reject) => dispatch(loadProjectNameInputValue(value, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, mapDispatchToProps)(ProjectForm))
