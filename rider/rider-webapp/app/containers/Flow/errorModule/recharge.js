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

import Button from 'antd/lib/button'
import Radio from 'antd/lib/radio'
import Form from 'antd/lib/form'
import Popover from 'antd/lib/popover'
import Tooltip from 'antd/lib/tooltip'
import message from 'antd/lib/message'
const FormItem = Form.Item

import { selectLocale } from '../../LanguageProvider/selectors'
import { confirmReChange } from '../action'
import { selectConfirmRechargeLoading } from '../selectors'

export class FlowRecharge extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      typeOfOpTopics: 'increment'
    }
  }

  handleReChangeVal = (e) => {
    this.setState({ typeOfOpTopics: e.target.value })
  }

  handleReChangeVisible = (record) => (visible) => {
    this.setState({
      visible
    })
  }

  confirmReChange = () => {
    const { locale, projectIdGeted, record } = this.props
    const { typeOfOpTopics } = this.state
    this.props.onConfirmReChange(projectIdGeted, record.id, typeOfOpTopics, () => {
      const msg = locale === 'en' ? 'success' : '回灌成功'
      message.success(msg)
      this.closeReChange()
    })
  }

  closeReChange = () => {
    this.setState({
      visible: false
    })
  }

  render () {
    const {
      locale, title, record, confirmRechargeLoading
    } = this.props

    const rechargeOpContent = (
      <div className="recharge-op-content">
        <Form className="ri-workbench-form workbench-flow-form">
          <FormItem
            label={locale === 'en' ? 'Type operated topics' : '操作topics类型'}
            labelCol={{span: 8}}
            wrapperCol={{span: 16}}>
            <Radio.Group defaultValue="increment" onChange={this.handleReChangeVal}>
              <Radio.Button value="initial">{locale === 'en' ? 'initial' : '全量'}</Radio.Button>
              <Radio.Button value="increment">{locale === 'en' ? 'increment' : '增量'}</Radio.Button>
              <Radio.Button value="all">{locale === 'en' ? 'initial and increment' : '全量and增量'}</Radio.Button>
            </Radio.Group>
          </FormItem>
        </Form>
        <div className="recharge-btn">
          <Button style={{ marginRight: '8px' }} onClick={this.closeReChange}>取消</Button>
          <Button type="primary" onClick={this.confirmReChange} loading={confirmRechargeLoading}>确定</Button>
        </div>
      </div>
    )

    return (
      <Popover
        placement="bottomRight"
        content={rechargeOpContent}
        title={title}
        trigger="click"
        visible={this.state.visible}
        onVisibleChange={this.handleReChangeVisible(record)}>
        <Tooltip title={title}>
          <Button icon="retweet" shape="circle" type="ghost"></Button>
        </Tooltip>
      </Popover>
    )
  }
}

FlowRecharge.propTypes = {
  locale: PropTypes.string,
  title: PropTypes.object,
  record: PropTypes.any,
  projectIdGeted: PropTypes.string,
  onConfirmReChange: PropTypes.func,
  confirmRechargeLoading: PropTypes.bool
}

export function mapDispatchToProps (dispatch) {
  return {
    onConfirmReChange: (projectIdGeted, id, protocolType, resolve, reject) => dispatch(confirmReChange(projectIdGeted, id, protocolType, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale(),
  confirmRechargeLoading: selectConfirmRechargeLoading()
})

export default connect(mapStateToProps, mapDispatchToProps)(FlowRecharge)
