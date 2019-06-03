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
import messages from '../messages'

import Table from 'antd/lib/table'
import Tooltip from 'antd/lib/tooltip'
import Recharge from './recharge'
import RechargeHistory from './rechargeHistory'

import { selectRoleType } from '../../App/selectors'
import { selectLocale } from '../../LanguageProvider/selectors'

export class FlowErrorList extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      currentErrors: []
      // currentErrors: [
      //   {
      //     'batchId': '04yzmerp',
      //     'createTime': '2019-03-27 11:38:05',
      //     'dataType': 'inital',
      //     'errorCount': 21,
      //     'errorInfo': 'error',
      //     'errorMaxWatermarkTs': '2019-03-20 16:17:18',
      //     'errorMinWatermarkTs': '2019-03-20 16:17:18',
      //     'errorPattern': 'flow',
      //     'feedbackTime': '2019-03-20 16:17:18',
      //     'flowId': 35,
      //     'id': 1903,
      //     'projectId': 1,
      //     'sinkNs': 'oracle.oracle01.db.table.v2.dbpar01.tablepar03',
      //     'sinkTable': 'oracle.oracle01.db.table.v2.dbpar01.tablepar03',
      //     'sourceNs': 'oracle.oracle01.db.table.v2.dbpar01.tablepar01',
      //     'sourceTable': 'oracle.oracle01.db.table.v2.dbpar01.tablepar01',
      //     'streamId': 42,
      //     'topics': '[{"topic_name":"wormhole_feedback_new","topic_type": "increment", "partition_offset":[{"partition_num":0,"from_offset":10000,"until_offset":20000}]'
      //   }
      // ]
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

  handleFlowChange = (pagination, filters, sorter) => {}

  render () {
    const {
      roleType, projectIdGeted, data
    } = this.props
    // const { currentErrors } = this.state

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
      className: 'text-align-center',
      render: (text, record) => {
        let comp = ''
        let textLen = text.length
        if (textLen > 20) {
          comp = (
            <Tooltip title={text} overlayStyle={{wordBreak: 'break-all'}}>
              <span>{text.slice(0, 20)}...</span>
            </Tooltip>
          )
        } else {
          comp = (
            <span>{text}</span>
          )
        }
        return comp
      }
    }, {
      title: '错误条数',
      dataIndex: 'errorCount',
      key: 'errorCount',
      className: 'text-align-center'
    }, {
      title: 'errorMinWatermark',
      dataIndex: 'errorMinWaterMarkTs',
      key: 'errorMinWaterMarkTs',
      className: 'text-align-center'
    }, {
      title: 'errorMaxWatermark',
      dataIndex: 'errorMaxWaterMarkTs',
      key: 'errorMaxWaterMarkTs',
      className: 'text-align-center'
    }, {
      title: '错误信息',
      dataIndex: 'errorInfo',
      key: 'errorInfo',
      className: 'text-align-center',
      render: (text, record) => {
        let comp = ''
        let textLen = text.length
        if (textLen > 20) {
          comp = (
            <Tooltip title={text} overlayStyle={{wordBreak: 'break-all', height: '300px'}} placement="left">
              <span>{text.slice(0, 20)}...</span>
            </Tooltip>
          )
        } else {
          comp = (
            <span>{text}</span>
          )
        }
        return comp
      }
    },
    {
      title: '错误时间',
      dataIndex: 'feedbackTime',
      key: 'feedbackTime',
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
          const backfillHistoryTxt = <FormattedMessage {...messages.errorListBackfillHistory} />

          let backfillBtn = (
            <Recharge title={backfillTxt} projectIdGeted={projectIdGeted} record={record} />
          )
          let backfillHistoryBtn = (
            <RechargeHistory title={backfillHistoryTxt} projectIdGeted={projectIdGeted} record={record} />
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
      <div style={{height: '80vh'}}>
        <Table
          dataSource={data}
          columns={columns}
          onChange={this.handleFlowChange}
          pagination={pagination}
          className="ri-workbench-table-container"
          rowKey="id"
          bordered>
        </Table>
        {/* {flowStartForm} */}
      </div>
    )
  }
}

FlowErrorList.propTypes = {
  roleType: PropTypes.string,
  // locale: PropTypes.string,
  projectIdGeted: PropTypes.string,
  data: PropTypes.array
}

export function mapDispatchToProps (dispatch) {
  return {
    // onLoadErrorList: (resolve) => dispatch(getErrorList(resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  // flows: selectFlows(),
  roleType: selectRoleType(),
  locale: selectLocale()
})

export default connect(mapStateToProps, mapDispatchToProps)(FlowErrorList)

