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
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Helmet from 'react-helmet'

import FlowsDetail from './FlowsDetail'
import FlowsTime from './FlowsTime'
import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Icon from 'antd/lib/icon'
import Dropdown from 'antd/lib/dropdown'
import Menu from 'antd/lib/menu'
import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
import Tooltip from 'antd/lib/tooltip'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
import Tag from 'antd/lib/tag'
import Popconfirm from 'antd/lib/popconfirm'
import Popover from 'antd/lib/popover'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { selectFlows, selectError } from './selectors'
import { loadAdminAllFlows, loadUserAllFlows, loadAdminSingleFlow, operateUserFlow, editLogForm, saveForm, checkOutForm, loadSourceLogDetail, loadSourceSinkDetail, loadSinkWriteRrrorDetail, loadSourceInput, chuckAwayFlow, operateFlow } from './action'

export class Flow extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      originFlows: [],
      currentFlows: [],
      selectedRowKeys: [],
      modalVisible: false,
      refreshFlowLoading: false,
      refreshFlowText: 'Refresh',

      current: 'detail',
      flowDetail: null,
      flowId: 0,
      timeModalVisible: false,
      timeDetail: null,
      flowIdTemp: '',

      filteredInfo: null,
      sortedInfo: null,

      searchStartIdText: '',
      searchEndIdText: '',
      filterDropdownVisibleId: false,
      searchTextFlowProject: '',
      filterDropdownVisibleFlowProject: false,
      searchTextSourceNs: '',
      filterDropdownVisibleSourceNs: false,
      searchTextSinkNs: '',
      filterDropdownVisibleSinkNs: false,
      filterDatepickerShown: false,
      startTimeText: '',
      endTimeText: '',
      startedStartTimeText: '',
      startedEndTimeText: '',
      filterDropdownVisibleStartedTime: false,
      stoppedStartTimeText: '',
      stoppedEndTimeText: '',
      filterDropdownVisibleStoppedTime: false
    }
  }

  componentWillMount () {
    this.loadFlowData()
  }

  componentWillReceiveProps (props) {
    if (props.flows) {
      const originFlows = props.flows.map(s => {
        s.key = s.id
        s.visible = false
        return s
      })
      this.setState({
        originFlows: originFlows.slice(),
        currentFlows: originFlows.slice()
      })
    }
  }
  componentWillUnmount () {
    // 频繁使用的组件，手动清除数据，避免出现闪现上一条数据
    this.props.onChuckAwayFlow()
  }

  refreshFlow = () => {
    this.setState({
      refreshFlowLoading: true,
      refreshFlowText: 'Refreshing'
    })
    this.loadFlowData()
  }

  loadFlowData () {
    if (localStorage.getItem('loginRoleType') === 'admin') {
      this.props.flowClassHide === 'hide'
        ? this.props.onLoadAdminSingleFlow(this.props.projectIdGeted, () => { this.flowRefreshState() })
        : this.props.onLoadAdminAllFlows(() => { this.flowRefreshState() })
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      this.props.onLoadUserAllFlows(this.props.projectIdGeted, () => {
        this.flowRefreshState()
      })
    }
  }

  flowRefreshState () {
    this.setState({
      refreshFlowLoading: false,
      refreshFlowText: 'Refresh'
    })
  }

  onSelectChange = (selectedRowKeys) => this.setState({ selectedRowKeys })

  /**
   * 批量操作
   * @param selectedRowKeys
   */
  handleMenuClick = (selectedRowKeys) => (e) => {
    if (selectedRowKeys.length > 0) {
      let menuAction = ''
      let menuMsg = ''
      if (e.key === 'menuStart') {
        menuAction = 'start'
        menuMsg = '启动'
      } else if (e.key === 'menuStop') {
        menuAction = 'stop'
        menuMsg = '停止'
      } else if (e.key === 'menuDelete') {
        menuAction = 'delete'
        menuMsg = '删除'
      }

      const requestValue = {
        projectId: Number(this.props.projectIdGeted),
        flowIds: this.state.selectedRowKeys.join(','),
        action: menuAction
      }

      this.props.onOperateUserFlow(requestValue, (result) => {
        this.setState({
          selectedRowKeys: []
        })

        if (typeof (result) === 'object') {
          const resultFailed = result.filter(i => i.msg.indexOf('failed') > -1)
          if (resultFailed.length > 0) {
            const resultFailedIdArr = resultFailed.map(i => i.id)
            const resultFailedIdStr = resultFailedIdArr.join('、')
            message.error(`Flow ID ${resultFailedIdStr} ${menuMsg}失败！`, 5)
          } else {
            message.success(`${menuMsg}成功！`, 3)
          }
        } else {
          message.success(`${menuMsg}成功！`, 3)
        }
      }, (result) => {
        message.error(`操作失败：${result}`, 3)
      })
    } else {
      message.warning('请选择 Flow！', 3)
    }
  }

  showTimeModal = () => {
    this.setState({
      timeModalVisible: true,
      timeDetail: null
    })
  }

  /**
   * 单行操作
   * @param record
   */
  singleOpreateFlow (record, action) {
    const requestValue = {
      projectId: record.projectId,
      action: action,
      flowIds: `${record.id}`
    }

    let singleMsg = ''
    if (action === 'start') {
      singleMsg = '启动'
    } else if (action === 'stop') {
      singleMsg = '停止'
    } else if (action === 'delete') {
      singleMsg = '删除'
    }

    this.props.onOperateUserFlow(requestValue, (result) => {
      if (action === 'delete') {
        message.success(`${singleMsg}成功！`, 3)
      } else {
        if (result.msg.indexOf('failed') > -1) {
          message.error(`Flow ID ${result.id} ${singleMsg}失败！`, 3)
        } else {
          if (action === 'renew') {
            message.success('生效！', 3)
          } else {
            message.success(`${singleMsg}成功！`, 3)
          }
        }
      }
    }, (result) => {
      message.error(`操作失败：${result}`, 3)
    })
  }

  // start
  onShowFlowStart = (record, action) => (e) => this.singleOpreateFlow(record, action)

  // stop
  stopFlowBtn = (record, action) => (e) => this.singleOpreateFlow(record, action)

  // renew
  updateFlow = (record, action) => (e) => this.singleOpreateFlow(record, action)

  // delete
  onSingleDeleteFlow = (record, action) => (e) => this.singleOpreateFlow(record, action)

  // backfill
  onShowBackfill =(record) => (e) => {
    this.showTimeModal()
    this.setState({ flowIdTemp: record.id })
  }

  // copy
  onCopyFlow = (record) => (e) => this.props.onShowCopyFlow(record)

  handleFlowChange = (pagination, filters, sorter) => {
    this.setState({
      filteredInfo: filters,
      sortedInfo: sorter
    })
  }

  onInputChange = (value) => (e) => this.setState({ [value]: e.target.value })

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      [visible]: false,
      currentFlows: this.state.originFlows.map((record) => {
        const match = String(record[columnName]).match(reg)
        if (!match) {
          return null
        }
        return {
          ...record,
          [`${columnName}Origin`]: record[columnName],
          [columnName]: (
            <span>
              {String(record[columnName]).split(reg).map((text, i) => (
                i > 0 ? [<span className="highlight">{match[0]}</span>, text] : text
              ))}
            </span>
          )
        }
      }).filter(record => !!record)
    })
  }

  showModal = (id) => () => {
    this.setState({
      modalVisible: true,
      flowDetail: null,
      flowId: id
    }, () => {
      // this.flowsDetail.onLoadData(id)
    })
  }

  handleCancel = (e) => {
    this.setState({
      modalVisible: false,
      current: 'detail'
    })
    this.flowsDetail.onCancelCleanData()
  }

  handleTimeCancel = (e) => this.setState({ timeModalVisible: false })

  handleTimeOk = () => {
    if (this.flowsTime.state.startValue === null) {
      message.warning('开始时间不能为空！')
    } else if (this.flowsTime.state.endValue === null) {
      message.warning('结束时间不能为空！')
    } else {
      const s = new Date(this.flowsTime.state.startValue._d)
      const e = new Date(this.flowsTime.state.endValue._d)

      // 时间格式转换
      // start time
      let monthStringS = ''
      if (s.getMonth() + 1 < 10) {
        monthStringS = `0${s.getMonth() + 1}`
      } else {
        monthStringS = `${s.getMonth()}`
      }

      let dateStringS = ''
      if (s.getDate() < 10) {
        dateStringS = `0${s.getDate()}`
      } else {
        dateStringS = `${s.getDate()}`
      }

      let hourStringS = ''
      if (s.getHours() < 10) {
        hourStringS = `0${s.getHours()}`
      } else {
        hourStringS = `${s.getHours()}`
      }

      let minuteStringS = ''
      if (s.getMinutes() < 10) {
        minuteStringS = `0${s.getMinutes()}`
      } else {
        minuteStringS = `${s.getMinutes()}`
      }

      // end time
      let monthStringE = ''
      if (e.getMonth() + 1 < 10) {
        monthStringE = `0${e.getMonth() + 1}`
      } else {
        monthStringE = `${e.getMonth()}`
      }

      let dateStringE = ''
      if (e.getDate() < 10) {
        dateStringE = `0${e.getDate()}`
      } else {
        dateStringE = `${e.getDate()}`
      }

      let hourStringE = ''
      if (e.getHours() < 10) {
        hourStringE = `0${e.getHours()}`
      } else {
        hourStringE = `${e.getHours()}`
      }

      let minuteStringE = ''
      if (e.getMinutes() < 10) {
        minuteStringE = `0${e.getMinutes()}`
      } else {
        minuteStringE = `${e.getMinutes()}`
      }

      // 时间格式拼接
      const startDate = `${s.getFullYear()}-${monthStringS}-${dateStringS} ${hourStringS}:${minuteStringS}`
      const endDate = `${e.getFullYear()}-${monthStringE}-${dateStringE} ${hourStringE}:${minuteStringE}`

      const flowIds = this.state.selectedRowKeys.length === 0
        ? `${this.state.flowIdTemp}`
        : this.state.selectedRowKeys.join(',')

      this.props.onOperateFlow(this.props.projectIdGeted, flowIds, 'backfill', startDate, endDate, () => {
        message.success('Backfill 成功！', 3)
        this.setState({
          selectedRowKeys: []
        })
      })
      this.setState({
        timeModalVisible: false
      })
    }
  }

  handleEndOpenChange = (status) => this.setState({ filterDatepickerShown: status })

  onRangeTimeChange = (value, dateString) => {
    this.setState({
      startTimeText: dateString[0],
      endTimeText: dateString[1]
    })
  }

  onRangeTimeSearch = (columnName, startTimeText, endTimeText, visible) => () => {
    const startSearchTime = (new Date(this.state.startTimeText)).getTime()
    const endSearchTime = (new Date(this.state.endTimeText)).getTime()

    let startOrEnd = ''
    if (columnName === 'createTime') {
      startOrEnd = startSearchTime || endSearchTime ? { createTime: [0] } : { createTime: [] }
    } else if (columnName === 'updateTime') {
      startOrEnd = startSearchTime || endSearchTime ? { updateTime: [0] } : { updateTime: [] }
    }

    this.setState({
      [visible]: false,
      currentFlows: this.state.originFlows.map((record) => {
        const match = (new Date(record[columnName])).getTime()
        if ((match < startSearchTime) || (match > endSearchTime)) {
          return null
        }
        return {
          ...record,
          [columnName]: (
            this.state.startTimeText === ''
              ? <span>{record[columnName]}</span>
              : <span className="highlight">{record[columnName]}</span>
          )
        }
      }).filter(record => !!record),
      filteredInfo: startOrEnd
    })
  }

  onRangeIdSearch = (columnName, startText, endText, visible) => () => {
    let infoFinal = ''
    if (columnName === 'id') {
      infoFinal = this.state[startText] || this.state[endText] ? { id: [0] } : { id: [] }
    } else if (columnName === 'streamId') {
      infoFinal = this.state[startText] || this.state[endText] ? { streamId: [0] } : { streamId: [] }
    }

    this.setState({
      [visible]: false,
      currentFlows: this.state.originFlows.map((record) => {
        const match = record[columnName]
        if ((match < parseInt(this.state[startText])) || (match > parseInt(this.state[endText]))) {
          return null
        }
        return record
      }).filter(record => !!record),
      filteredInfo: infoFinal
    })
  }

  render () {
    const { className, onShowAddFlow, onShowEditFlow, flowClassHide } = this.props
    const { refreshFlowText, refreshFlowLoading } = this.state

    let { sortedInfo, filteredInfo } = this.state
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const columns = [{
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      sorter: (a, b) => a.id - b.id,
      sortOrder: sortedInfo.columnKey === 'id' && sortedInfo.order,
      filteredValue: filteredInfo.id,
      filterDropdown: (
        <div className="custom-filter-dropdown custom-filter-dropdown-ps">
          <Form>
            <Row>
              <Col span={9}>
                <Input
                  ref={ele => { this.searchInput = ele }}
                  placeholder="Start ID"
                  onChange={this.onInputChange('searchStartIdText')}
                />
              </Col>
              <Col span={1}>
                <p className="ant-form-split">-</p>
              </Col>
              <Col span={9}>
                <Input
                  placeholder="End ID"
                  onChange={this.onInputChange('searchEndIdText')}
                />
              </Col>
              <Col span={5} className="text-align-center">
                <Button type="primary" onClick={this.onRangeIdSearch('id', 'searchStartIdText', 'searchEndIdText', 'filterDropdownVisibleId')}>Search</Button>
              </Col>
            </Row>
          </Form>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleId,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleId: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Project',
      dataIndex: 'projectName',
      key: 'projectName',
      className: `${flowClassHide}`,
      sorter: (a, b) => {
        if (typeof a.projectName === 'object') {
          return a.projectNameOrigin < b.projectNameOrigin ? -1 : 1
        } else {
          return a.projectName < b.projectName ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'projectName' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Project Name"
            value={this.state.searchTextFlowProject}
            onChange={this.onInputChange('searchTextFlowProject')}
            onPressEnter={this.onSearch('projectName', 'searchTextFlowProject', 'filterDropdownVisibleFlowProject')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('projectName', 'searchTextFlowProject', 'filterDropdownVisibleFlowProject')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleFlowProject,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleFlowProject: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Source Namespace',
      dataIndex: 'sourceNs',
      key: 'sourceNs',
      sorter: (a, b) => {
        if (typeof a.sourceNs === 'object') {
          return a.sourceNsOrigin < b.sourceNsOrigin ? -1 : 1
        } else {
          return a.sourceNs < b.sourceNs ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'sourceNs' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Source Namespace"
            value={this.state.searchTextSourceNs}
            onChange={this.onInputChange('searchTextSourceNs')}
            onPressEnter={this.onSearch('sourceNs', 'searchTextSourceNs', 'filterDropdownVisibleSourceNs')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('sourceNs', 'searchTextSourceNs', 'filterDropdownVisibleSourceNs')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleSourceNs,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleSourceNs: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Sink Namespace',
      dataIndex: 'sinkNs',
      key: 'sinkNs',
      sorter: (a, b) => {
        if (typeof a.sinkNs === 'object') {
          return a.sinkNsOrigin < b.sinkNsOrigin ? -1 : 1
        } else {
          return a.sinkNs < b.sinkNs ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'sinkNs' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Sink Namespace"
            value={this.state.searchTextSinkNs}
            onChange={this.onInputChange('searchTextSinkNs')}
            onPressEnter={this.onSearch('sinkNs', 'searchTextSinkNs', 'filterDropdownVisibleSinkNs')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('sinkNs', 'searchTextSinkNs', 'filterDropdownVisibleSinkNs')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleSinkNs,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleSinkNs: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Flow Status',
      dataIndex: 'status',
      key: 'status',
      className: 'text-align-center',
      sorter: (a, b) => a.status < b.status ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'status' && sortedInfo.order,
      filters: [
        {text: 'new', value: 'new'},
        {text: 'starting', value: 'starting'},
        {text: 'running', value: 'running'},
        {text: 'updating', value: 'updating'},
        {text: 'stopping', value: 'stopping'},
        {text: 'stopped', value: 'stopped'},
        {text: 'suspending', value: 'suspending'},
        {text: 'failed', value: 'failed'},
        {text: 'deleting', value: 'deleting'}
      ],
      filteredValue: filteredInfo.status,
      onFilter: (value, record) => record.status.includes(value),
      render: (text, record) => {
        let flowStatusColor = ''
        if (record.status === 'new') {
          flowStatusColor = 'orange'
        } else if (record.status === 'starting') {
          flowStatusColor = 'green'
        } else if (record.status === 'running') {
          flowStatusColor = 'green-inverse'
        } else if (record.status === 'updating') {
          flowStatusColor = 'cyan'
        } else if (record.status === 'stopping') {
          flowStatusColor = 'gray'
        } else if (record.status === 'stopped') {
          flowStatusColor = '#545252'
        } else if (record.status === 'suspending') {
          flowStatusColor = 'red'
        } else if (record.status === 'failed') {
          flowStatusColor = 'red-inverse'
        } else if (record.status === 'deleting') {
          flowStatusColor = 'purple'
        }
        return (
          <div>
            <Tag color={flowStatusColor} className="stream-style">{record.status}</Tag>
          </div>
        )
      }
    }, {
      title: 'Stream Status',
      dataIndex: 'streamStatus',
      key: 'streamStatus',
      className: 'text-align-center',
      sorter: (a, b) => a.streamStatus < b.streamStatus ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'streamStatus' && sortedInfo.order,
      filters: [
        {text: 'new', value: 'new'},
        {text: 'starting', value: 'starting'},
        {text: 'running', value: 'running'},
        {text: 'stopping', value: 'stopping'},
        {text: 'stopped', value: 'stopped'},
        {text: 'failed', value: 'failed'}
      ],
      filteredValue: filteredInfo.streamStatus,
      onFilter: (value, record) => record.streamStatus.includes(value),
      render: (text, record) => {
        let streamStatusColor = ''
        if (record.streamStatus === 'new') {
          streamStatusColor = 'orange'
        } else if (record.streamStatus === 'starting') {
          streamStatusColor = 'green'
        } else if (record.streamStatus === 'running') {
          streamStatusColor = 'green-inverse'
        } else if (record.streamStatus === 'stopping') {
          streamStatusColor = 'gray'
        } else if (record.streamStatus === 'stopped') {
          streamStatusColor = '#545252'
        } else if (record.streamStatus === 'failed') {
          streamStatusColor = 'red-inverse'
        }
        return (
          <div>
            <Tag color={streamStatusColor} className="stream-style">{record.streamStatus}</Tag>
          </div>
        )
      }
    }, {
      title: 'Stream ID',
      dataIndex: 'streamId',
      key: 'streamId',
      sorter: (a, b) => a.streamId - b.streamId,
      sortOrder: sortedInfo.columnKey === 'streamId' && sortedInfo.order,
      filteredValue: filteredInfo.streamId,
      filterDropdown: (
        <div className="custom-filter-dropdown custom-filter-dropdown-ps">
          <Form>
            <Row>
              <Col span={9}>
                <Input
                  ref={ele => { this.searchInput = ele }}
                  placeholder="Start ID"
                  onChange={this.onInputChange('searchStartStreamIdText')}
                />
              </Col>
              <Col span={1}>
                <p className="ant-form-split">-</p>
              </Col>
              <Col span={9}>
                <Input
                  placeholder="End ID"
                  onChange={this.onInputChange('searchEndStreamIdText')}
                />
              </Col>
              <Col span={5} className="text-align-center">
                <Button type="primary" onClick={this.onRangeIdSearch('streamId', 'searchStartStreamIdText', 'searchEndStreamIdText', 'filterDropdownVisibleStreamId')}>Search</Button>
              </Col>
            </Row>
          </Form>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleStreamId,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleStreamId: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Stream Type',
      dataIndex: 'streamType',
      key: 'streamType',
      // className: 'text-align-center',
      sorter: (a, b) => a.streamType < b.streamType ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'streamType' && sortedInfo.order,
      filters: [
        {text: 'default', value: 'default'},
        {text: 'hdfslog', value: 'hdfslog'},
        {text: 'routing', value: 'routing'}
      ],
      filteredValue: filteredInfo.streamType,
      onFilter: (value, record) => record.streamType.includes(value)
    }, {
      title: 'Start Time',
      dataIndex: 'startedTime',
      key: 'startedTime',
      sorter: (a, b) => {
        if (typeof a.startedTime === 'object') {
          return a.startedTimeOrigin < b.startedTimeOrigin ? -1 : 1
        } else {
          return a.startedTime < b.startedTime ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'startedTime' && sortedInfo.order,
      filteredValue: filteredInfo.startedTime,
      filterDropdown: (
        <div className="custom-filter-dropdown-style">
          <RangePicker
            showTime
            format="YYYY-MM-DD HH:mm:ss"
            placeholder={['Start', 'End']}
            onOpenChange={this.handleEndOpenChange}
            onChange={this.onRangeTimeChange}
            onPressEnter={this.onRangeTimeSearch('startedTime', 'startedStartTimeText', 'startedEndTimeText', 'filterDropdownVisibleStartedTime')}
          />
          <Button type="primary" className="rangeFilter" onClick={this.onRangeTimeSearch('startedTime', 'startedStartTimeText', 'startedEndTimeText', 'filterDropdownVisibleStartedTime')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleStartedTime,
      onFilterDropdownVisibleChange: visible => {
        if (!this.state.filterDatepickerShown) {
          this.setState({ filterDropdownVisibleStartedTime: visible })
        }
      }
    }, {
      title: 'End Time',
      dataIndex: 'stoppedTime',
      key: 'stoppedTime',
      sorter: (a, b) => {
        if (typeof a.updateTime === 'object') {
          return a.stoppedTimeOrigin < b.stoppedTimeOrigin ? -1 : 1
        } else {
          return a.stoppedTime < b.stoppedTime ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'stoppedTime' && sortedInfo.order,
      filteredValue: filteredInfo.stoppedTime,
      filterDropdown: (
        <div className="custom-filter-dropdown-style">
          <RangePicker
            showTime
            format="YYYY-MM-DD HH:mm:ss"
            placeholder={['Start', 'End']}
            onOpenChange={this.handleEndOpenChange}
            onChange={this.onRangeTimeChange}
            onPressEnter={this.onRangeTimeSearch('stoppedTime', 'stoppedStartTimeText', 'stoppedEndTimeText', 'filterDropdownVisibleStoppedTime')}
          />
          <Button type="primary" className="rangeFilter" onClick={this.onRangeTimeSearch('stoppedTime', 'stoppedStartTimeText', 'stoppedEndTimeText', 'filterDropdownVisibleStoppedTime')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleStoppedTime,
      onFilterDropdownVisibleChange: visible => {
        if (!this.state.filterDatepickerShown) {
          this.setState({ filterDropdownVisibleStoppedTime: visible })
        }
      }
    }, {
      title: 'Action',
      key: 'action',
      className: 'text-align-center',
      render: (text, record) => {
        let flowRenewDisabled = false

        let flStart = ''
        let flStartDisabled = (
          <Tooltip title="开始">
            <Button icon="caret-right" shape="circle" type="ghost" disabled></Button>
          </Tooltip>
        )
        let flStartDisabledNot = (
          <Popconfirm placement="bottom" title="确定开始吗？" okText="Yes" cancelText="No" onConfirm={this.onShowFlowStart(record, 'start')}>
            <Tooltip title="开始">
              <Button icon="caret-right" shape="circle" type="ghost"></Button>
            </Tooltip>
          </Popconfirm>
        )

        let flStop = ''
        let flStopDisabled = (
          <Tooltip title="停止">
            <Button shape="circle" type="ghost" disabled>
              <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
            </Button>
          </Tooltip>
        )
        let flStopDisabledNot = (
          <Popconfirm placement="bottom" title="确定停止吗？" okText="Yes" cancelText="No" onConfirm={this.stopFlowBtn(record, 'stop')}>
            <Tooltip title="停止">
              <Button shape="circle" type="ghost">
                <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
              </Button>
            </Tooltip>
          </Popconfirm>
        )

        if (record.disableActions.indexOf('start') < 0 && record.disableActions.indexOf('renew') < 0) {
          // disableActions === 'stop'
          flStart = flStartDisabledNot
          flStop = flStopDisabled
        } else if (record.disableActions.indexOf('start') < 0 && record.disableActions.indexOf('stop') < 0) {
          // disableActions === 'renew'
          flowRenewDisabled = true
          flStart = flStartDisabledNot
          flStop = flStopDisabledNot
        } else if (record.disableActions.indexOf('renew') < 0 && record.disableActions.indexOf('stop') < 0) {
          // disableActions === 'start'
          flStart = flStartDisabled
          flStop = flStopDisabledNot
        } else if (record.disableActions.indexOf('start') < 0) {
          // disableActions === stop, renew
          flowRenewDisabled = true
          flStart = flStartDisabledNot
          flStop = flStopDisabled
        } else if (record.disableActions.indexOf('stop') < 0) {
          // disableActions === start, renew
          flowRenewDisabled = true
          flStart = flStartDisabled
          flStop = flStopDisabledNot
        } else if (record.disableActions.indexOf('renew') < 0) {
          // disableActions === start, stop
          flStart = flStartDisabled
          flStop = flStopDisabled
        } else if (record.disableActions.indexOf('start') < 0 && record.disableActions.indexOf('stop') < 0 && record.disableActions.indexOf('renew') < 0) {
          // disableActions === ''
          flStart = flStartDisabledNot
          flStop = flStopDisabledNot
        } else {
          // disableActions === start, stop, renew
          flowRenewDisabled = true
          flStart = flStartDisabled
          flStop = flStopDisabled
        }

        let FlowActionSelect = ''
        if (localStorage.getItem('loginRoleType') === 'admin') {
          FlowActionSelect = ''
        } else if (localStorage.getItem('loginRoleType') === 'user') {
          FlowActionSelect = (
            <span>
              {/* <Tooltip title="数据质量">
                <Button icon="file-excel" shape="circle" type="ghost" onClick={this.showModal(record.id)}></Button>
              </Tooltip> */}

              <Tooltip title="修改">
                <Button icon="edit" shape="circle" type="ghost" onClick={onShowEditFlow(record)}></Button>
              </Tooltip>

              <Tooltip title="复制">
                <Button icon="copy" shape="circle" type="ghost" onClick={this.onCopyFlow(record)}></Button>
              </Tooltip>

              {flStart}
              {flStop}

              <Tooltip title="生效">
                <Button icon="check" shape="circle" type="ghost" onClick={this.updateFlow(record, 'renew')} disabled={flowRenewDisabled}></Button>
              </Tooltip>

              {/* <Tooltip title="backfill" onClick={this.onShowBackfill(record)}>
               <Button icon="rollback" shape="circle" type="ghost" ></Button>
               </Tooltip> */}

              <Popconfirm placement="bottom" title="确定删除吗？" okText="Yes" cancelText="No" onConfirm={this.onSingleDeleteFlow(record, 'delete')}>
                <Tooltip title="删除">
                  <Button icon="delete" shape="circle" type="ghost"></Button>
                </Tooltip>
              </Popconfirm>
            </span>
          )
        }

        return (
          <span className="ant-table-action-column">
            <Tooltip title="查看详情">
              <Popover
                placement="left"
                content={<div style={{ width: '600px', overflowY: 'auto', height: '260px', overflowX: 'auto' }}>
                  <p><strong>   Project Id：</strong>{record.projectId}</p>
                  <p><strong>   Protocol：</strong>{record.consumedProtocol}</p>
                  <p><strong>   Stream Name：</strong>{record.streamName}</p>
                  <p><strong>   Create Time：</strong>{record.createTime}</p>
                  <p><strong>   Update Time：</strong>{record.updateTime}</p>
                  <p><strong>   Create By：</strong>{record.createBy}</p>
                  <p><strong>   Update By：</strong>{record.updateBy}</p>
                  <p><strong>   Disable Actions：</strong>{record.disableActions}</p>
                  <p><strong>   Sink Config：</strong>{record.sinkConfig}</p>
                  <p><strong>   Transformation Config：</strong>{record.tranConfig}</p>
                  <p><strong>   Message：</strong>{record.msg}</p>
                </div>}
                title={<h3>详情</h3>}
                trigger="click">
                <Button icon="file-text" shape="circle" type="ghost"></Button>
              </Popover>
            </Tooltip>
            {FlowActionSelect}
          </span>
        )
      }
    }]

    const pagination = {
      defaultPageSize: this.state.pageSize,
      showSizeChanger: true,
      onShowSizeChange: (current, pageSize) => {
        this.setState({
          pageIndex: current,
          pageSize: pageSize
        })
      },
      onChange: (current) => {
        this.setState({
          pageIndex: current
        })
      }
    }

    const { selectedRowKeys } = this.state

    let rowSelection = null
    if (localStorage.getItem('loginRoleType') === 'admin') {
      rowSelection = null
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      rowSelection = {
        selectedRowKeys,
        onChange: this.onSelectChange,
        onShowSizeChange: this.onShowSizeChange
      }
    }

    const menuItems = (
      <Menu onClick={this.handleMenuClick(selectedRowKeys)} className="ri-workbench-select-dropdown">
        <Menu.Item key="menuStart"><Icon type="caret-right" />  开始</Menu.Item>
        <Menu.Item key="menuStop">
          <i className="iconfont icon-8080pxtubiaokuozhan100" style={{ fontSize: '12px' }}></i>  停止</Menu.Item>
        <Menu.Item key="menuDelete"><Icon type="delete" />  删除</Menu.Item>
      </Menu>
      )

    let FlowAddOrNot = ''
    if (localStorage.getItem('loginRoleType') === 'admin') {
      FlowAddOrNot = ''
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      FlowAddOrNot = (
        <span>
          <Button icon="plus" type="primary" onClick={onShowAddFlow}>新建</Button>
          <Dropdown trigger={['click']} overlay={menuItems}>
            <Button type="ghost" className="flow-action-btn">
              批量操作 <Icon type="down" />
            </Button>
          </Dropdown>
        </span>
      )
    }

    const helmetHide = this.props.flowClassHide !== 'hide'
      ? (<Helmet title="Flow" />)
      : (<Helmet title="Workbench" />)

    return (
      <div className={`ri-workbench-table ri-common-block ${className}`}>
        {helmetHide}
        <h3 className="ri-common-block-title">
          <Icon type="bars" /> Flow 列表
        </h3>
        <div className="ri-common-block-tools">
          <Button icon="poweroff" type="ghost" className="refresh-button-style" loading={refreshFlowLoading} onClick={this.refreshFlow}>{refreshFlowText}</Button>
          {FlowAddOrNot}
        </div>
        <Table
          dataSource={this.state.currentFlows}
          columns={columns}
          onChange={this.handleFlowChange}
          pagination={pagination}
          rowSelection={rowSelection}
          className="ri-workbench-table-container"
          bordered>
        </Table>
        <Modal
          visible={this.state.modalVisible}
          onCancel={this.handleCancel}
          wrapClassName="ant-modal-xlarge ant-modal-no-footer"
          footer={<span></span>}
        >
          <FlowsDetail
            flowIdGeted={this.state.flowId}
            ref={(f) => { this.flowsDetail = f }}

            onEditLogForm={this.props.onEditLogForm}
            onSaveForm={this.props.onSaveForm}
            onCheckOutForm={this.props.onCheckOutForm}
            onLoadSourceLogDetail={this.props.onLoadSourceLogDetail}
            onLoadSourceSinkDetail={this.props.onLoadSourceSinkDetail}
            onLoadSinkWriteRrrorDetail={this.props.onLoadSinkWriteRrrorDetail}
            onLoadSourceInput={this.props.onLoadSourceInput}
          />
        </Modal>

        <Modal
          title="设置时间"
          visible={this.state.timeModalVisible}
          onCancel={this.handleTimeCancel}
          onOk={this.handleTimeOk}
        >
          <FlowsTime
            ref={(f) => { this.flowsTime = f }} />
        </Modal>
      </div>
    )
  }
}

Flow.propTypes = {
  // flows: React.PropTypes.oneOfType([
  //   React.PropTypes.array,
  //   React.PropTypes.bool
  // ]),
  className: React.PropTypes.string,
  onShowAddFlow: React.PropTypes.func,
  onShowEditFlow: React.PropTypes.func,
  onShowCopyFlow: React.PropTypes.func,

  onEditLogForm: React.PropTypes.func,
  onSaveForm: React.PropTypes.func,
  onCheckOutForm: React.PropTypes.func,
  onLoadSourceLogDetail: React.PropTypes.func,
  onLoadSourceSinkDetail: React.PropTypes.func,
  onLoadSinkWriteRrrorDetail: React.PropTypes.func,
  onLoadSourceInput: React.PropTypes.func,
  projectIdGeted: React.PropTypes.string,
  flowClassHide: React.PropTypes.string,

  onOperateFlow: React.PropTypes.func,
  onLoadAdminAllFlows: React.PropTypes.func,
  onLoadUserAllFlows: React.PropTypes.func,
  onLoadAdminSingleFlow: React.PropTypes.func,
  onOperateUserFlow: React.PropTypes.func,
  onChuckAwayFlow: React.PropTypes.func
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadAdminAllFlows: (resolve) => dispatch(loadAdminAllFlows(resolve)),
    onLoadUserAllFlows: (projectId, resolve) => dispatch(loadUserAllFlows(projectId, resolve)),
    onLoadAdminSingleFlow: (projectId, resolve) => dispatch(loadAdminSingleFlow(projectId, resolve)),
    onOperateUserFlow: (values, resolve, reject) => dispatch(operateUserFlow(values, resolve, reject)),

    onEditLogForm: (flow, resolve) => dispatch(editLogForm(flow, resolve)),
    onSaveForm: (flowId, taskType, value, resolve) => dispatch(saveForm(flowId, taskType, value, resolve)),
    onCheckOutForm: (flowId, taskType, startDate, endDate, value, resolve) => dispatch(checkOutForm(flowId, taskType, startDate, endDate, value, resolve)),
    onLoadSourceLogDetail: (id, pageIndex, pageSize, resolve) => dispatch(loadSourceLogDetail(id, pageIndex, pageSize, resolve)),
    onLoadSourceSinkDetail: (id, pageIndex, pageSize, resolve) => dispatch(loadSourceSinkDetail(id, pageIndex, pageSize, resolve)),
    onLoadSinkWriteRrrorDetail: (id, pageIndex, pageSize, resolve) => dispatch(loadSinkWriteRrrorDetail(id, pageIndex, pageSize, resolve)),
    onLoadSourceInput: (flowId, taskType, resolve) => dispatch(loadSourceInput(flowId, taskType, resolve)),
    onChuckAwayFlow: () => dispatch(chuckAwayFlow()),

    onOperateFlow: (projectId, flowIds, operate, startDate, endDate, resolve, reject) => dispatch(operateFlow(projectId, flowIds, operate, startDate, endDate, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  flows: selectFlows(),
  error: selectError()
})

export default connect(mapStateToProps, mapDispatchToProps)(Flow)

