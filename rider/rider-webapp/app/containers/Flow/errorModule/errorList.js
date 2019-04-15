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
// import { FormattedMessage } from 'react-intl'
// import messages from './messages'

import Table from 'antd/lib/table'
import messages from '../messages'
import { FormattedMessage } from 'react-intl'
import { Button } from 'antd/lib/radio'
import { Tooltip } from 'antd'
// import { selectLocale } from '../LanguageProvider/selectors'
export class FlowErrorList extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      currentErrors: []
    }
  }

  componentWillMount () {

  }

  componentWillReceiveProps (props) {
  }

  componentWillUnmount () {
    // 频繁使用的组件，手动清除数据，避免出现闪现上一条数据
    // this.props.onChuckAwayFlow()
  }
  render () {
    const { roleType } = this.props
    const { currentErrors } = this.state
    const columns = [{
      title: 'batchId',
      dataIndex: 'batchId',
      key: 'batchId',
      className: 'text-align-center'
    },
    {
      title: 'Flow Name',
      dataIndex: 'flowName',
      key: 'flowName',
      className: 'text-align-center'
    },
    {
      title: '数据类型',
      dataIndex: 'dataType',
      key: 'dataType',
      className: 'text-align-center'
    }, {
      title: 'topics',
      dataIndex: 'topics',
      key: 'topics',
      className: 'text-align-center'
    }, {
      title: '错误条数',
      dataIndex: 'errorNum',
      key: 'errorNum',
      className: 'text-align-center'
    }, {
      title: 'errorMinWatermark',
      dataIndex: 'errorMinWatermark',
      key: 'errorMinWatermark',
      className: 'text-align-center'
    }, {
      title: 'errorMaxWatermark',
      dataIndex: 'errorMaxWatermark',
      key: 'errorMaxWatermark',
      className: 'text-align-center'
    }, {
      title: '错误信息',
      dataIndex: 'errorInfo',
      key: 'errorInfo',
      className: 'text-align-center'
    },
    {
      title: '错误时间',
      dataIndex: 'errorTime',
      key: 'errorTime',
      className: 'text-align-center'
    },
    {
      title: 'Action',
      key: 'action',
      className: 'text-align-center',
      render: (text, record) => {
        let FlowActionSelect = ''
        if (roleType === 'admin') {
          FlowActionSelect = ''
        } else if (roleType === 'user') {
          const backfillTxt = <FormattedMessage {...messages.errorListBackfillData} />
          const backfillHistoryTxt = <FormattedMessage {...messages.errorListBackfillData} />
          let backfillBtn = (
            <Tooltip title={backfillTxt}>
              <Button icon="bar-chart" shape="circle" type="ghost" ></Button>
            </Tooltip>
          )
          let backfillHistoryBtn = (
            <Tooltip title={backfillHistoryTxt}>
              <Button icon="bar-chart" shape="circle" type="ghost" ></Button>
            </Tooltip>
          )
          FlowActionSelect = (
            <span>
              {backfillBtn}
              {backfillHistoryBtn}
            </span>
          )
        }
        return (
          <span className="ant-table-action-column">
            {FlowActionSelect}
          </span>
        )
      }
    }]
    const pagination = {
      showSizeChanger: true,
      onChange: (current) => this.setState({ pageIndex: current })
    }
    // const flowStartForm = startModalVisible
    //   ? (
    //     <FlowStartForm
    //     />
    //   )
    //   : ''
    return (
      <div>
        <Table
          dataSource={currentErrors}
          columns={columns}
          onChange={this.handleFlowChange}
          pagination={pagination}
          className="ri-workbench-table-container"
          bordered>
        </Table>
        {/* {flowStartForm} */}
      </div>
    )
  }
}

FlowErrorList.propTypes = {
  roleType: PropTypes.string
}

export function mapDispatchToProps (dispatch) {
  return {
    // onLoadAdminAllFlows: (resolve) => dispatch(loadAdminAllFlows(resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  // flows: selectFlows(),
})

export default connect(mapStateToProps, mapDispatchToProps)(FlowErrorList)

