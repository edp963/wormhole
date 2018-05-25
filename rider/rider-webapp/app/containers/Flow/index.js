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
import Helmet from 'react-helmet'
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import FlowsDetail from './FlowsDetail'
import FlowsTime from './FlowsTime'
import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Icon from 'antd/lib/icon'
import Dropdown from 'antd/lib/dropdown'
import Menu from 'antd/lib/menu'
import Input from 'antd/lib/input'
import Tooltip from 'antd/lib/tooltip'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
import Tag from 'antd/lib/tag'
import Popconfirm from 'antd/lib/popconfirm'
import Popover from 'antd/lib/popover'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { changeLocale } from '../../containers/LanguageProvider/actions'
import { selectFlows, selectError } from './selectors'
import {
  loadAdminAllFlows, loadUserAllFlows, loadAdminSingleFlow, operateUserFlow, editLogForm,
  saveForm, checkOutForm, loadSourceLogDetail, loadSourceSinkDetail, loadSinkWriteRrrorDetail,
  loadSourceInput, loadFlowDetail, chuckAwayFlow
} from './action'

// import { formatConcat } from '../../utils/util'

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
      showFlowDetails: {},

      filteredInfo: null,
      sortedInfo: null,

      searchTextFlowProject: '',
      filterDropdownVisibleFlowProject: false,
      searchTextSourceNs: '',
      filterDropdownVisibleSourceNs: false,
      searchTextSinkNs: '',
      filterDropdownVisibleSinkNs: false,
      filterDatepickerShown: false,
      searchTextStreamId: '',
      filterDropdownVisibleStreamId: false,
      searchTextStreamType: '',
      filterDropdownVisibleStreamType: false,
      startTimeText: '',
      endTimeText: '',
      startedStartTimeText: '',
      startedEndTimeText: '',
      filterDropdownVisibleStartedTime: false,
      stoppedStartTimeText: '',
      stoppedEndTimeText: '',
      filterDropdownVisibleStoppedTime: false,

      columnNameText: '',
      valueText: '',
      visibleBool: false,
      startTimeTextState: '',
      endTimeTextState: '',
      paginationInfo: null,
      startTextState: '',
      endTextState: ''
    }
  }

  componentWillMount () {
    this.refreshFlow()
    this.props.onChangeLanguage(localStorage.getItem('preferredLanguage'))
  }

  componentWillReceiveProps (props) {
    if (props.flows) {
      const originFlows = props.flows.map(s => {
        s.key = s.id
        // s.visible = false
        return s
      })
      this.setState({ originFlows: originFlows.slice() })
      this.state.columnNameText === ''
        ? this.setState({ currentFlows: originFlows.slice() })
        : this.searchOperater()
    }
  }
  componentWillUnmount () {
    // 频繁使用的组件，手动清除数据，避免出现闪现上一条数据
    this.props.onChuckAwayFlow()
  }

  searchOperater () {
    const { columnNameText, valueText, visibleBool, startTimeTextState, endTimeTextState } = this.state

    if (columnNameText !== '') {
      this.onSearch(columnNameText, valueText, visibleBool)()

      if (columnNameText === 'startedTime' || columnNameText === 'stoppedTime') {
        this.onRangeTimeSearch(columnNameText, startTimeTextState, endTimeTextState, visibleBool)()
      }
    }
  }

  refreshFlow = () => {
    this.setState({
      refreshFlowLoading: true,
      refreshFlowText: 'Refreshing'
    })
    this.loadFlowData()
  }

  loadFlowData () {
    const {
      projectIdGeted, flowClassHide, onLoadAdminSingleFlow, onLoadAdminAllFlows, onLoadUserAllFlows
    } = this.props

    if (localStorage.getItem('loginRoleType') === 'admin') {
      flowClassHide === 'hide'
        ? onLoadAdminSingleFlow(projectIdGeted, () => { this.flowRefreshState() })
        : onLoadAdminAllFlows(() => { this.flowRefreshState() })
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      onLoadUserAllFlows(projectIdGeted, () => { this.flowRefreshState() })
    }
  }

  flowRefreshState () {
    this.setState({
      refreshFlowLoading: false,
      refreshFlowText: 'Refresh'
    })

    const {
      columnNameText, valueText, visibleBool, paginationInfo, filteredInfo, sortedInfo, startTimeTextState, endTimeTextState
    } = this.state

    if (columnNameText !== '') {
      if (columnNameText === 'startedTime' || columnNameText === 'stoppedTime') {
        this.onRangeTimeSearch(columnNameText, startTimeTextState, endTimeTextState, visibleBool)()
      } else {
        this.handleFlowChange(paginationInfo, filteredInfo, sortedInfo)
        this.onSearch(columnNameText, valueText, visibleBool)()
      }
    }
  }

  onSelectChange = (selectedRowKeys) => this.setState({ selectedRowKeys })

  // 批量操作
  handleMenuClick = (selectedRowKeys) => (e) => {
    const languagetext = localStorage.getItem('preferredLanguage')
    if (selectedRowKeys.length > 0) {
      let menuAction = ''
      let menuMsg = ''
      switch (e.key) {
        case 'menuStart':
          menuAction = 'start'
          menuMsg = languagetext === 'en' ? 'Start' : '启动'
          break
        case 'menuStop':
          menuAction = 'stop'
          menuMsg = languagetext === 'en' ? 'Stop' : '停止'
          break
        case 'menuDelete':
          menuAction = 'delete'
          menuMsg = languagetext === 'en' ? 'Delete' : '删除'
          break
        case 'menuRenew':
          menuAction = 'renew'
          menuMsg = languagetext === 'en' ? 'Renew' : '生效'
          break
      }

      const requestValue = {
        projectId: Number(this.props.projectIdGeted),
        flowIds: this.state.selectedRowKeys.join(','),
        action: menuAction
      }

      this.props.onOperateUserFlow(requestValue, (result) => {
        this.setState({ selectedRowKeys: [] })
        const languagetextSuccess = languagetext === 'en' ? 'successfully!' : '成功！'

        if (typeof (result) === 'object') {
          const resultFailed = result.filter(i => i.msg.includes('failed'))
          if (resultFailed.length > 0) {
            const resultFailedIdArr = resultFailed.map(i => i.id)
            const resultFailedIdStr = resultFailedIdArr.join('、')

            const languagetextFailFlowId = languagetext === 'en'
              ? `It fails to ${menuMsg} Flow ID ${resultFailedIdStr}!`
              : `Flow ID ${resultFailedIdStr} ${menuMsg}失败！`
            message.error(languagetextFailFlowId, 5)
          } else {
            message.success(`${menuMsg}${languagetextSuccess}`, 3)
          }
        } else {
          message.success(`${menuMsg}${languagetextSuccess}`, 3)
        }
      }, (result) => {
        message.error(`${languagetext === 'en' ? 'Operation failed:' : '操作失败：'}${result}`, 3)
      })
    } else {
      message.warning(`${languagetext === 'en' ? 'Please select Flow!' : '请选择 Flow！'}`, 3)
    }
  }

  showTimeModal = () => {
    this.setState({
      timeModalVisible: true,
      timeDetail: null
    })
  }

  // 单行操作
  singleOpreateFlow = (record, action) => (e) => {
    const languagetext = localStorage.getItem('preferredLanguage')
    const requestValue = {
      projectId: record.projectId,
      action: action,
      flowIds: `${record.id}`
    }

    let singleMsg = ''
    switch (action) {
      case 'start':
        singleMsg = languagetext === 'en' ? 'Start' : '启动'
        break
      case 'stop':
        singleMsg = languagetext === 'en' ? 'Stop' : '停止'
        break
      case 'delete':
        singleMsg = languagetext === 'en' ? 'Delete' : '删除'
        break
    }

    this.props.onOperateUserFlow(requestValue, (result) => {
      const languagetextSuccess = languagetext === 'en' ? 'successfully!' : '成功！'
      if (action === 'delete') {
        message.success(`${singleMsg}${languagetextSuccess}`, 3)
      } else {
        if (result.msg.includes('failed')) {
          const languagetextFail = languagetext === 'en'
            ? `It fails to ${singleMsg} Flow ID ${result.id}!`
            : `Flow ID ${result.id} ${singleMsg}失败！`

          message.error(languagetextFail, 3)
        } else {
          action === 'renew'
            ? message.success(languagetext === 'en' ? 'Renew successfully！' : '生效！', 3)
            : message.success(`${singleMsg}${languagetextSuccess}`, 3)
        }
      }
    }, (result) => {
      message.error(`${languagetext === 'en' ? 'Operation failed:' : '操作失败：'}${result}`, 3)
    })
  }

  onCopyFlow = (record) => (e) => this.props.onShowCopyFlow(record)

  handleFlowChange = (pagination, filters, sorter) => {
    const { filteredInfo } = this.state

    let filterValue = {}
    if (filteredInfo !== null) {
      if (filteredInfo) {
        if (filters.status && filters.streamStatus) {
          if (filters.status.length === 0 && filters.streamStatus.length === 0) {
            return
          } else {
            this.onSearch('', '', false)()
            if (filteredInfo.status && filteredInfo.streamStatus) {
              if (filteredInfo.status.length !== 0 && filters.streamStatus.length !== 0) {
                filterValue = {status: [], streamStatus: filters.streamStatus}
              } else if (filteredInfo.streamStatus.length !== 0 && filters.status.length !== 0) {
                filterValue = {status: filters.status, streamStatus: []}
              } else {
                filterValue = filters
              }
            } else {
              filterValue = filters
            }
          }
        } else {
          filterValue = filters
        }
      } else {
        filterValue = filters
      }
    } else {
      filterValue = filters
    }

    this.setState({
      filteredInfo: filterValue,
      sortedInfo: sorter,
      paginationInfo: pagination
    })
  }

  onInputChange = (value) => (e) => this.setState({ [value]: e.target.value })

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      filteredInfo: { status: [], streamStatus: [] }
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        valueText: value,
        visibleBool: visible,
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
    if (!this.flowsTime.state.startValue) {
      message.warning('开始时间不能为空！')
    } else if (!this.flowsTime.state.endValue) {
      message.warning('结束时间不能为空！')
    } else {
      // const sVal = new Date(this.flowsTime.state.startValue._d)
      // const eVal = new Date(this.flowsTime.state.endValue._d)
      // formatConcat(sVal, eVal)
      //
      // const flowIds = this.state.selectedRowKeys.length === 0
      //   ? `${this.state.flowIdTemp}`
      //   : this.state.selectedRowKeys.join(',')
      //
      // this.setState({ timeModalVisible: false })
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
    if (columnName === 'startedTime') {
      startOrEnd = startSearchTime || endSearchTime ? { startedTime: [0] } : { startedTime: [] }
    } else if (columnName === 'stoppedTime') {
      startOrEnd = startSearchTime || endSearchTime ? { stoppedTime: [0] } : { stoppedTime: [] }
    }

    this.setState({
      filteredInfo: { status: [], streamStatus: [] }
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        startTimeTextState: startTimeText,
        endTimeTextState: endTimeText,
        visibleBool: visible,
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
    })
  }

  handleVisibleChangeFlow = (record) => (visible) => {
    if (visible) {
      this.setState({
        visible
      }, () => {
        const requestValue = {
          projectId: record.projectId,
          streamId: typeof (record.streamId) === 'object' ? record.streamIdOrigin : record.streamId,
          flowId: record.id,
          roleType: localStorage.getItem('loginRoleType')
        }

        this.props.onLoadFlowDetail(requestValue, (result) => this.setState({ showFlowDetails: result }))
      })
    }
  }

  render () {
    const { className, onShowAddFlow, onShowEditFlow, flowClassHide } = this.props
    const { flowId, refreshFlowText, refreshFlowLoading, currentFlows, modalVisible, timeModalVisible, showFlowDetails } = this.state

    let { sortedInfo, filteredInfo } = this.state
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const columns = [{
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      sorter: (a, b) => a.id - b.id,
      sortOrder: sortedInfo.columnKey === 'id' && sortedInfo.order
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
            onClick={this.onSearch('sourceNs', 'searchTextSourceNs', 'filterDropdownVisibleSourceNs')}
          >Search
          </Button>
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
            onClick={this.onSearch('sinkNs', 'searchTextSinkNs', 'filterDropdownVisibleSinkNs')}
          >Search
          </Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleSinkNs,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleSinkNs: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Flow State',
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
        switch (record.status) {
          case 'new':
            flowStatusColor = 'orange'
            break
          case 'starting':
            flowStatusColor = 'green'
            break
          case 'running':
            flowStatusColor = 'green-inverse'
            break
          case 'updating':
            flowStatusColor = 'cyan'
            break
          case 'stopping':
            flowStatusColor = 'gray'
            break
          case 'stopped':
            flowStatusColor = '#545252'
            break
          case 'suspending':
            flowStatusColor = 'red'
            break
          case 'failed':
            flowStatusColor = 'red-inverse'
            break
          case 'deleting':
            flowStatusColor = 'purple'
            break
        }
        return (
          <div>
            <Tag color={flowStatusColor} className="stream-style">{record.status}</Tag>
          </div>
        )
      }
    }, {
      title: 'Stream State',
      dataIndex: 'streamStatus',
      key: 'streamStatus',
      className: 'text-align-center',
      sorter: (a, b) => a.streamStatus < b.streamStatus ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'streamStatus' && sortedInfo.order,
      filters: [
        {text: 'new', value: 'new'},
        {text: 'starting', value: 'starting'},
        {text: 'waiting', value: 'waiting'},
        {text: 'running', value: 'running'},
        {text: 'stopping', value: 'stopping'},
        {text: 'stopped', value: 'stopped'},
        {text: 'failed', value: 'failed'}
      ],
      filteredValue: filteredInfo.streamStatus,
      onFilter: (value, record) => record.streamStatus.includes(value),
      render: (text, record) => {
        let streamStatusColor = ''
        switch (record.streamStatus) {
          case 'new':
            streamStatusColor = 'orange'
            break
          case 'starting':
            streamStatusColor = 'green'
            break
          case 'running':
            streamStatusColor = 'green-inverse'
            break
          case 'stopping':
            streamStatusColor = 'gray'
            break
          case 'stopped':
            streamStatusColor = '#545252'
            break
          case 'failed':
            streamStatusColor = 'red-inverse'
            break
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
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Stream Id"
            value={this.state.searchTextStreamId}
            onChange={this.onInputChange('searchTextStreamId')}
            onPressEnter={this.onSearch('streamId', 'searchTextStreamId', 'filterDropdownVisibleStreamId')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('streamId', 'searchTextStreamId', 'filterDropdownVisibleStreamId')}
          >Search
          </Button>
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
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Stream Type"
            value={this.state.searchTextStreamType}
            onChange={this.onInputChange('searchTextStreamType')}
            onPressEnter={this.onSearch('streamType', 'searchTextStreamType', 'filterDropdownVisibleStreamType')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('streamType', 'searchTextStreamType', 'filterDropdownVisibleStreamType')}
          >Search
          </Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleStreamType,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleStreamType: visible
      }, () => this.searchInput.focus())
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
          <Button
            type="primary"
            className="rangeFilter"
            onClick={this.onRangeTimeSearch('startedTime', 'startedStartTimeText', 'startedEndTimeText', 'filterDropdownVisibleStartedTime')}
          >Search
          </Button>
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
        if (typeof a.stoppedTime === 'object') {
          return a.stoppedTimeOrigin < b.stoppedTimeOrigin ? -1 : 1
        } else {
          return a.stoppedTime < b.stoppedTime ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'stoppedTime' && sortedInfo.order,
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
        let FlowActionSelect = ''
        if (localStorage.getItem('loginRoleType') === 'admin') {
          FlowActionSelect = ''
        } else if (localStorage.getItem('loginRoleType') === 'user') {
          const modifyFormat = <FormattedMessage {...messages.flowTableModify} />
          const startFormat = <FormattedMessage {...messages.flowTableStart} />
          const sureStartFormat = <FormattedMessage {...messages.flowSureStart} />
          const stopFormat = <FormattedMessage {...messages.flowTableStop} />
          const sureStopFormat = <FormattedMessage {...messages.flowSureStop} />
          const renewFormat = <FormattedMessage {...messages.flowTableRenew} />
          const copyFormat = <FormattedMessage {...messages.flowTableCopy} />
          const deleteFormat = <FormattedMessage {...messages.flowTableDelete} />
          const sureDeleteFormat = <FormattedMessage {...messages.flowSureDelete} />
          const sureRenewFormat = <FormattedMessage {...messages.flowSureRenew} />

          const strEdit = record.disableActions.includes('modify')
            ? <Button icon="edit" shape="circle" type="ghost" disabled></Button>
            : <Button icon="edit" shape="circle" type="ghost" onClick={onShowEditFlow(record)}></Button>

          const strStart = record.disableActions.includes('start')
            ? (
              <Tooltip title={startFormat}>
                <Button icon="caret-right" shape="circle" type="ghost" disabled></Button>
              </Tooltip>
            )
            : (
              <Popconfirm placement="bottom" title={sureStartFormat} okText="Yes" cancelText="No" onConfirm={this.singleOpreateFlow(record, 'start')}>
                <Tooltip title={startFormat}>
                  <Button icon="caret-right" shape="circle" type="ghost"></Button>
                </Tooltip>
              </Popconfirm>
            )

          const strStop = record.disableActions.includes('stop')
            ? (
              <Tooltip title={stopFormat}>
                <Button shape="circle" type="ghost" disabled>
                  <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
                </Button>
              </Tooltip>
            )
            : (
              <Popconfirm placement="bottom" title={sureStopFormat} okText="Yes" cancelText="No" onConfirm={this.singleOpreateFlow(record, 'stop')}>
                <Tooltip title={stopFormat}>
                  <Button shape="circle" type="ghost">
                    <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
                  </Button>
                </Tooltip>
              </Popconfirm>
            )

          const strRenew = record.disableActions.includes('renew')
            ? (
              <Tooltip title={renewFormat}>
                <Button icon="check" shape="circle" type="ghost" disabled></Button>
              </Tooltip>
            )
            : (
              <Popconfirm placement="bottom" title={sureRenewFormat} okText="Yes" cancelText="No" onConfirm={this.singleOpreateFlow(record, 'renew')}>
                <Tooltip title={renewFormat}>
                  <Button icon="check" shape="circle" type="ghost"></Button>
                </Tooltip>
              </Popconfirm>
            )

          FlowActionSelect = (
            <span>
              <Tooltip title={modifyFormat}>
                {strEdit}
              </Tooltip>
              <Tooltip title={copyFormat}>
                <Button icon="copy" shape="circle" type="ghost" onClick={this.onCopyFlow(record)}></Button>
              </Tooltip>
              {strStart}
              {strStop}
              {strRenew}
              <Popconfirm placement="bottom" title={sureDeleteFormat} okText="Yes" cancelText="No" onConfirm={this.singleOpreateFlow(record, 'delete')}>
                <Tooltip title={deleteFormat}>
                  <Button icon="delete" shape="circle" type="ghost"></Button>
                </Tooltip>
              </Popconfirm>
            </span>
          )
        }

        let sinkConfigFinal = ''
        if (!showFlowDetails.sinkConfig) {
          sinkConfigFinal = ''
        } else {
          const sinkJson = JSON.parse(showFlowDetails.sinkConfig)
          sinkConfigFinal = JSON.stringify(sinkJson['sink_specific_config'])
        }

        return (
          <span className="ant-table-action-column">
            <Tooltip title={<FormattedMessage {...messages.flowViewDetails} />}>
              <Popover
                placement="left"
                content={
                  <div className="flow-table-detail">
                    <p className={flowClassHide}><strong>   Project Id：</strong>{showFlowDetails.projectId}</p>
                    <p><strong>   Protocol：</strong>{showFlowDetails.consumedProtocol}</p>
                    <p><strong>   Stream Name：</strong>{showFlowDetails.streamName}</p>
                    <p><strong>   Sink Config：</strong>{sinkConfigFinal}</p>
                    <p><strong>   Transformation Config：</strong>{showFlowDetails.tranConfig}</p>
                    <p><strong>   Create Time：</strong>{showFlowDetails.createTime}</p>
                    <p><strong>   Update Time：</strong>{showFlowDetails.updateTime}</p>
                    <p><strong>   Create By：</strong>{showFlowDetails.createBy}</p>
                    <p><strong>   Update By：</strong>{showFlowDetails.updateBy}</p>
                    <p><strong>   Disable Actions：</strong>{showFlowDetails.disableActions}</p>
                    <p><strong>   Message：</strong>{showFlowDetails.msg}</p>
                  </div>}
                title={<h3><FormattedMessage {...messages.flowDetails} /></h3>}
                trigger="click"
                onVisibleChange={this.handleVisibleChangeFlow(record)}>
                <Button icon="file-text" shape="circle" type="ghost"></Button>
              </Popover>
            </Tooltip>
            {FlowActionSelect}
          </span>
        )
      }
    }]

    const pagination = {
      showSizeChanger: true,
      onChange: (current) => this.setState({ pageIndex: current })
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
        <Menu.Item key="menuStart"><Icon type="caret-right" /> <FormattedMessage {...messages.flowTableStart} />
        </Menu.Item>
        <Menu.Item key="menuStop">
          <i className="iconfont icon-8080pxtubiaokuozhan100" style={{ fontSize: '12px' }}></i> <FormattedMessage {...messages.flowTableStop} />
        </Menu.Item>
        <Menu.Item key="menuRenew"><Icon type="check" /> <FormattedMessage {...messages.flowTableRenew} />
        </Menu.Item>
        <Menu.Item key="menuDelete"><Icon type="delete" /> <FormattedMessage {...messages.flowTableDelete} />
        </Menu.Item>
      </Menu>
      )

    let FlowAddOrNot = ''
    if (localStorage.getItem('loginRoleType') === 'admin') {
      FlowAddOrNot = ''
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      FlowAddOrNot = (
        <span>
          <Button icon="plus" type="primary" onClick={onShowAddFlow}>
            <FormattedMessage {...messages.flowTableCreate} />
          </Button>
          <Dropdown trigger={['click']} overlay={menuItems}>
            <Button type="ghost" className="flow-action-btn">
              <FormattedMessage {...messages.flowBatchAction} /> <Icon type="down" />
            </Button>
          </Dropdown>
        </span>
      )
    }

    const helmetHide = flowClassHide !== 'hide'
      ? (<Helmet title="Flow" />)
      : (<Helmet title="Workbench" />)

    return (
      <div className={`ri-workbench-table ri-common-block ${className}`}>
        {helmetHide}
        <h3 className="ri-common-block-title">
          <Icon type="bars" /> Flow <FormattedMessage {...messages.flowTableList} />
        </h3>
        <div className="ri-common-block-tools">
          {FlowAddOrNot}
          <Button icon="reload" type="ghost" className="refresh-button-style" loading={refreshFlowLoading} onClick={this.refreshFlow}>{refreshFlowText}</Button>
        </div>
        <Table
          dataSource={currentFlows}
          columns={columns}
          onChange={this.handleFlowChange}
          pagination={pagination}
          rowSelection={rowSelection}
          className="ri-workbench-table-container"
          bordered>
        </Table>
        <Modal
          visible={modalVisible}
          onCancel={this.handleCancel}
          wrapClassName="ant-modal-xlarge ant-modal-no-footer"
          footer={<span></span>}
        >
          <FlowsDetail
            flowIdGeted={flowId}
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
          visible={timeModalVisible}
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
  className: PropTypes.string,
  onShowAddFlow: PropTypes.func,
  onShowEditFlow: PropTypes.func,
  onShowCopyFlow: PropTypes.func,

  onEditLogForm: PropTypes.func,
  onSaveForm: PropTypes.func,
  onCheckOutForm: PropTypes.func,
  onLoadSourceLogDetail: PropTypes.func,
  onLoadSourceSinkDetail: PropTypes.func,
  onLoadSinkWriteRrrorDetail: PropTypes.func,
  onLoadSourceInput: PropTypes.func,
  projectIdGeted: PropTypes.string,
  flowClassHide: PropTypes.string,
  onLoadFlowDetail: PropTypes.func,

  onLoadAdminAllFlows: PropTypes.func,
  onLoadUserAllFlows: PropTypes.func,
  onLoadAdminSingleFlow: PropTypes.func,
  onOperateUserFlow: PropTypes.func,
  onChuckAwayFlow: PropTypes.func,
  onChangeLanguage: PropTypes.func
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
    onLoadFlowDetail: (requestValue, resolve) => dispatch(loadFlowDetail(requestValue, resolve)),
    onChuckAwayFlow: () => dispatch(chuckAwayFlow()),
    onChangeLanguage: (type) => dispatch(changeLocale(type))
  }
}

const mapStateToProps = createStructuredSelector({
  flows: selectFlows(),
  error: selectError()
})

export default connect(mapStateToProps, mapDispatchToProps)(Flow)

