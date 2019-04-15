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

import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Popover from 'antd/lib/popover'
import Tooltip from 'antd/lib/tooltip'

import { selectLocale } from '../../LanguageProvider/selectors'
import recharge from './recharge'

export class FlowRecharge extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      typeOfOpTopics: 'increase',
      rechargeList: []
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
    console.log(this.state.typeOfOpTopics)
    this.closeReChange()
  }

  closeReChange = () => {
    this.setState({
      visible: false
    })
  }

  render () {
    const {
      locale, title, record
    } = this.props

    const { rechargeList } = this.state

    const columns = [
      {
        title: '回灌时间',
        key: 'createTime',
        className: 'text-align-center'
      },
      {
        title: '回灌详情',
        key: 'detail',
        className: 'text-align-center'
      }
    ]

    const rechargeOpContent = (
      <div className="recharge-history-content">
        <Table
          dataSource={rechargeList}
          columns={columns}
          className="ri-workbench-table-container"
          rowKey="id"
          bordered
        />
        <div className="recharge-btn">
          <Button type="primary" onClick={this.closeReChange}>关闭</Button>
        </div>
      </div>
    )

    return (
      <Popover
        placement="left"
        content={rechargeOpContent}
        title={title}
        trigger="click"
        visible={this.state.visible}
        onVisibleChange={this.handleReChangeVisible(record)}>
        <Tooltip title={title}>
          <Button icon="file-text" shape="circle" type="ghost" ></Button>
        </Tooltip>
      </Popover>
    )
  }
}

FlowRecharge.propTypes = {
  locale: PropTypes.string,
  title: PropTypes.object,
  record: PropTypes.any
}

export function mapDispatchToProps (dispatch) {
  return {
    // onLoadAdminAllFlows: (resolve) => dispatch(loadAdminAllFlows(resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  locale: selectLocale()
})

export default connect(mapStateToProps, mapDispatchToProps)(FlowRecharge)
