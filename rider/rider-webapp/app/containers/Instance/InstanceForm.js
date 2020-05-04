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
import {connect} from 'react-redux'
import { createStructuredSelector } from 'reselect'
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
const FormItem = Form.Item

import DataSystemSelector from '../../components/DataSystemSelector'
import { loadDataSystemData } from '../../components/DataSystemSelector/dataSystemFunction'
import { checkInstance } from './action'
import { selectLocale } from '../LanguageProvider/selectors'

export class InstanceForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      instanceDSValue: ''
    }
  }

  checkInstanceName = (rule, value = '', callback) => {
    const { onCheckInstance, instanceFormType } = this.props
    const { instanceDSValue } = this.state

    instanceFormType === 'add'
      ? onCheckInstance(instanceDSValue, value, res => {
        callback()
      }, err => {
        callback(err)
      })
      : callback()
  }

  onSourceDataSystemItemSelect = (e) => {
    this.setState({ instanceDSValue: e })
    this.props.onInitInstanceSourceDs(e)
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { type, instanceFormType, locale } = this.props
    const { instanceDSValue } = this.state

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 16 }
    }

    // help
    let questionDS = ''
    if (instanceDSValue === 'oracle' || instanceDSValue === 'mysql' ||
      instanceDSValue === 'postgresql' || instanceDSValue === 'vertica' ||
      instanceDSValue === 'phoenix') {
      questionDS = 'ip:port'
    } else if (instanceDSValue === 'es') {
      questionDS = 'sink: http url list, lookup: tcp url, ip:port'
    } else if (instanceDSValue === 'hbase') {
      questionDS = 'zk node list'
    } else if (instanceDSValue === 'kafka' || instanceDSValue === 'cassandra' ||
      instanceDSValue === 'redis' || instanceDSValue === 'kudu') {
      questionDS = 'ip:port list'
    } else if (instanceDSValue === 'parquet') {
      questionDS = 'hdfs root path: hdfs://ip:port/test'
    }

    const connectionURLMsg = (
      <span>
        Connection URL
        <Tooltip title={<FormattedMessage {...messages.instanceHelp} />}>
          <Popover
            placement="top"
            content={
              <div style={{ width: '260px', height: '55px' }}>
                <p>{questionDS}</p>
              </div>
            }
            title={
              <h3><FormattedMessage {...messages.instanceHelp} /></h3>
            }
            trigger="click">
            <Icon type="question-circle-o" className="question-class" />
          </Popover>
        </Tooltip>
      </span>
    )

    return (
      <Form>
        <Row gutter={8}>
          <Col span={24}>
            <FormItem className="hide">
              {getFieldDecorator('id', {
                hidden: type === 'add'
              })(
                <Input />
              )}
            </FormItem>
            <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
              {getFieldDecorator('instanceDataSystem', {
                rules: [{
                  required: true,
                  message: `${locale === 'en' ? 'Please select Data System' : '请选择 Data System'}`
                }]
              })(
                <DataSystemSelector
                  data={loadDataSystemData()}
                  onItemSelect={this.onSourceDataSystemItemSelect}
                  dataSystemDisabled={instanceFormType === 'edit'}
                />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Instance" {...itemStyle}>
              {getFieldDecorator('instance', {
                rules: [{
                  required: true,
                  message: `${locale === 'en' ? 'Please fill in instance' : '请填写 Instance'}`
                },
                {
                  validator: this.checkInstanceName
                }]
              })(
                <Input
                  placeholder="Instance"
                  disabled={instanceFormType === 'edit'}
                />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label={connectionURLMsg} {...itemStyle}>
              {getFieldDecorator('connectionUrl', {
                rules: [{
                  required: true,
                  message: `${locale === 'en' ? 'Please fill in connection url' : '请填写 Connection Url'}`
                }]
              })(
                <Input
                  placeholder="Connection URL"
                  onChange={(e) => this.props.onInitCheckUrl(e.target.value)}
                />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Description" {...itemStyle}>
              {getFieldDecorator('description', {})(
                <Input type="textarea" placeholder="Description" autosize={{ minRows: 3, maxRows: 8 }} />
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Connection Config" {...itemStyle}>
              {getFieldDecorator('connConfig', {})(
                <Input type="textarea" placeholder="Connection Config" autosize={{ minRows: 3, maxRows: 8 }} />
              )}
            </FormItem>
          </Col>

        </Row>
      </Form>
    )
  }
}

InstanceForm.propTypes = {
  form: PropTypes.any,
  type: PropTypes.string,
  instanceFormType: PropTypes.string,
  onInitInstanceSourceDs: PropTypes.func,
  onCheckInstance: PropTypes.func,
  onInitCheckUrl: PropTypes.func,
  locale: PropTypes.string
}

function mapDispatchToProps (dispatch) {
  return {
    onCheckInstance: (type, nsInstance, resolve, reject) => dispatch(checkInstance(type, nsInstance, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, mapDispatchToProps)(InstanceForm))
