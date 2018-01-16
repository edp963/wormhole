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

import JobLogs from './JobLogs'
import Table from 'antd/lib/table'
import Modal from 'antd/lib/modal'
import Button from 'antd/lib/button'
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
import Tooltip from 'antd/lib/tooltip'
import message from 'antd/lib/message'
import Tag from 'antd/lib/tag'
import Popconfirm from 'antd/lib/popconfirm'
import Popover from 'antd/lib/popover'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { selectJobs, selectError } from './selectors'
import {
  loadAdminAllJobs, loadUserAllJobs, loadAdminSingleJob,
  loadAdminJobLogs, loadUserJobLogs, operateJob, loadJobDetail
} from './action'

export class Job extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      originJobs: [],
      currentJobs: [],
      selectedRowKeys: [],
      modalVisible: false,
      refreshJobLoading: false,
      refreshJobText: 'Refresh',
      refreshJobLogLoading: false,
      refreshJobLogText: 'Refresh',
      showJobDetail: {},

      logsJobModalVisible: false,
      logLogsContent: '',
      logsJobId: 0,

      filteredInfo: null,
      sortedInfo: null,

      searchTextName: '',
      filterDropdownVisibleName: false,
      searchTextSourceNs: '',
      filterDropdownVisibleSourceNs: false,
      searchTextSinkNs: '',
      filterDropdownVisibleSinkNs: false,
      searchTextAppId: '',
      filterDropdownVisibleAppId: false,
      filterDatepickerShown: false,
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
      paginationInfo: null
    }
  }

  componentWillMount () {
    this.refreshJob()
  }

  componentWillReceiveProps (props) {
    if (props.jobs) {
      const originJobs = props.jobs.map(s => {
        const responseOriginJob = Object.assign({}, s.job, {
          disableActions: s.disableActions,
          projectName: s.projectName
        })
        responseOriginJob.key = responseOriginJob.id
        return responseOriginJob
      })
      this.setState({ originJobs: originJobs.slice() })

      this.state.columnNameText === ''
        ? this.setState({ currentJobs: originJobs.slice() })
        : this.searchOperater()
    }
  }

  searchOperater () {
    const { columnNameText, valueText, visibleBool } = this.state
    const { startTimeTextState, endTimeTextState } = this.state

    if (columnNameText !== '') {
      this.onSearch(columnNameText, valueText, visibleBool)()

      if (columnNameText === 'startedTime' || columnNameText === 'stoppedTime') {
        this.onRangeTimeSearch(columnNameText, startTimeTextState, endTimeTextState, visibleBool)()
      }
    }
  }

  refreshJob = () => {
    this.setState({
      refreshJobLoading: true,
      refreshJobText: 'Refreshing'
    })
    this.loadJobData()
  }

  loadJobData () {
    if (localStorage.getItem('loginRoleType') === 'admin') {
      this.props.jobClassHide === 'hide'
        ? this.props.onLoadAdminSingleJob(this.props.projectIdGeted, () => { this.jobRefreshState() })
        : this.props.onLoadAdminAllJobs(() => { this.jobRefreshState() })
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      this.props.onLoadUserAllJobs(this.props.projectIdGeted, () => { this.jobRefreshState() })
    }
  }

  jobRefreshState () {
    this.setState({
      refreshJobLoading: false,
      refreshJobText: 'Refresh'
    })
    const { columnNameText, valueText, visibleBool } = this.state
    const { paginationInfo, filteredInfo, sortedInfo } = this.state
    const { startTimeTextState, endTimeTextState } = this.state

    if (columnNameText !== '') {
      if (columnNameText === 'startedTime' || columnNameText === 'stoppedTime') {
        this.onRangeTimeSearch(columnNameText, startTimeTextState, endTimeTextState, visibleBool)()
      } else {
        this.handleJobChange(paginationInfo, filteredInfo, sortedInfo)
        this.onSearch(columnNameText, valueText, visibleBool)()
      }
    }
  }

  onSelectChange = (selectedRowKeys) => this.setState({ selectedRowKeys })

  opreateJobFunc (record, action) {
    const requestValue = {
      projectId: record.projectId,
      action: action,
      jobId: `${record.id}`
    }

    this.props.onOperateJob(requestValue, (result) => {
      let singleMsg = ''
      if (action === 'start') {
        singleMsg = '启动'
      } else if (action === 'stop') {
        singleMsg = '停止'
      } else if (action === 'delete') {
        singleMsg = '删除'
      }
      message.success(`${singleMsg} 成功！`, 3)
    }, (result) => {
      message.error(`操作失败：${result}`, 5)
    })
  }

  startJobBtn = (record, action) => (e) => this.opreateJobFunc(record, action)

  stopJobBtn = (record, action) => (e) => this.opreateJobFunc(record, action)

  deleteJobBtn = (record, action) => (e) => this.opreateJobFunc(record, action)

  onShowJobLogs = (record) => (e) => {
    this.setState({
      logsJobModalVisible: true,
      logsProjectId: record.projectId,
      logsJobId: record.id
    })
    this.loadLogsData(record.projectId, record.id)
  }

  onInitRefreshLogs = (projectId, jobId) => {
    this.setState({
      refreshJobLogLoading: true,
      refreshJobLogText: 'Refreshing'
    })
    this.loadLogsData(projectId, jobId)
  }

  loadLogsData = (projectId, jobId) => {
    if (localStorage.getItem('loginRoleType') === 'admin') {
      this.props.onLoadAdminJobLogs(projectId, jobId, (result) => {
        this.setState({ jobLogsContent: result })
        this.jobLogRefreshState()
      })
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      this.props.onLoadUserJobLogs(projectId, jobId, (result) => {
        this.setState({ jobLogsContent: result })
        this.jobLogRefreshState()
      })
    }
  }

  jobLogRefreshState () {
    this.setState({
      refreshJobLogLoading: false,
      refreshJobLogText: 'Refresh'
    })
  }

  handleLogsCancel = (e) => this.setState({ logsJobModalVisible: false })

  handleJobChange = (pagination, filters, sorter) => {
    const { filteredInfo } = this.state

    let filterValue = {}
    if (filteredInfo !== null) {
      if (filteredInfo) {
        if (filters.status && filters.sourceType) {
          if (filters.status.length === 0 && filters.sourceType.length === 0) {
            return
          } else {
            this.onSearch('', '', false)()
            if (filteredInfo.status && filteredInfo.sourceType) {
              if (filteredInfo.status.length !== 0 && filters.sourceType.length !== 0) {
                filterValue = {status: [], sourceType: filters.sourceType}
              } else if (filteredInfo.sourceType.length !== 0 && filters.status.length !== 0) {
                filterValue = {status: filters.status, sourceType: []}
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
      filteredInfo: {status: [], sourceType: []}
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        valueText: value,
        visibleBool: visible,
        currentJobs: this.state.originJobs.map((record) => {
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
      filteredInfo: {status: [], sourceType: []}
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        startTimeTextState: startTimeText,
        endTimeTextState: endTimeText,
        visibleBool: visible,
        currentJobs: this.state.originJobs.map((record) => {
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

  handleVisibleChangeJob = (record) => (visible) => {
    if (visible) {
      this.setState({
        visible
      }, () => {
        let roleType = ''
        if (localStorage.getItem('loginRoleType') === 'admin') {
          roleType = 'admin'
        } else if (localStorage.getItem('loginRoleType') === 'user') {
          roleType = 'user'
        }

        const requestValue = {
          projectId: record.projectId,
          streamId: record.streamId,
          jobId: record.id,
          roleType: roleType
        }

        this.props.onLoadJobDetail(requestValue, (result) => {
          this.setState({
            showJobDetail: result
          })
        })
      })
    }
  }

  render () {
    const { className, onShowAddJob, onShowEditJob } = this.props
    const { refreshJobText, refreshJobLoading, showJobDetail } = this.state

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
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      // className: `${jobClassHide}`,
      sorter: (a, b) => {
        if (typeof a.name === 'object') {
          return a.nameOrigin < b.nameOrigin ? -1 : 1
        } else {
          return a.name < b.name ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'name' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Name"
            value={this.state.searchTextName}
            onChange={this.onInputChange('searchTextName')}
            onPressEnter={this.onSearch('name', 'searchTextName', 'filterDropdownVisibleName')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('name', 'searchTextName', 'filterDropdownVisibleName')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleName,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleName: visible
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
      title: 'AppId',
      dataIndex: 'sparkAppid',
      key: 'sparkAppid',
      sorter: (a, b) => {
        if (typeof a.sparkAppid === 'object') {
          return a.sparkAppidOrigin < b.sparkAppidOrigin ? -1 : 1
        } else {
          return a.sparkAppid < b.sparkAppid ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'sparkAppid' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="App Id"
            value={this.state.searchTextAppId}
            onChange={this.onInputChange('searchTextAppId')}
            onPressEnter={this.onSearch('sparkAppid', 'searchTextAppId', 'filterDropdownVisibleAppId')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('sparkAppid', 'searchTextAppId', 'filterDropdownVisibleAppId')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleAppId,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleAppId: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      className: 'text-align-center',
      sorter: (a, b) => a.status < b.status ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'status' && sortedInfo.order,
      filters: [
        {text: 'new', value: 'new'},
        {text: 'starting', value: 'starting'},
        {text: 'waiting,', value: 'waiting,'},
        {text: 'running', value: 'running'},
        {text: 'stopping', value: 'stopping'},
        {text: 'stopped', value: 'stopped'},
        {text: 'failed', value: 'failed'},
        {text: 'done', value: 'done'}
      ],
      filteredValue: filteredInfo.status,
      onFilter: (value, record) => record.status.includes(value),
      render: (text, record) => {
        let jobStatusColor = ''
        if (record.status === 'new') {
          jobStatusColor = 'orange'
        } else if (record.status === 'starting') {
          jobStatusColor = 'green'
        } else if (record.status === 'waiting') {
          jobStatusColor = '#22D67C'
        } else if (record.status === 'running') {
          jobStatusColor = 'green-inverse'
        } else if (record.status === 'stopping') {
          jobStatusColor = 'gray'
        } else if (record.status === 'stopped') {
          jobStatusColor = '#545252'
        } else if (record.status === 'failed') {
          jobStatusColor = 'red-inverse'
        } else if (record.status === 'done') {
          jobStatusColor = '#87d068'
        }
        return (
          <div>
            <Tag color={jobStatusColor} className="stream-style">{record.status}</Tag>
          </div>
        )
      }
    }, {
      title: 'Source Type',
      dataIndex: 'sourceType',
      key: 'sourceType',
      // className: 'text-align-center',
      sorter: (a, b) => a.sourceType < b.sourceType ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'sourceType' && sortedInfo.order,
      filters: [
        {text: 'hdfs_txt', value: 'hdfs_txt'}
      ],
      filteredValue: filteredInfo.sourceType,
      onFilter: (value, record) => record.sourceType.includes(value)
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
        // console.log('re', record.disableActions.indexOf('stop'))
        const strStart = record.disableActions.indexOf('start') > -1
          ? (
            <Tooltip title="开始">
              <Button icon="caret-right" shape="circle" type="ghost" disabled></Button>
            </Tooltip>
          )
          : (
            <Popconfirm placement="bottom" title="确定开始吗？" okText="Yes" cancelText="No" onConfirm={this.startJobBtn(record, 'start')}>
              <Tooltip title="开始">
                <Button icon="caret-right" shape="circle" type="ghost"></Button>
              </Tooltip>
            </Popconfirm>
          )

        const strStop = record.disableActions.indexOf('stop') > -1
          ? (
            <Tooltip title="停止">
              <Button shape="circle" type="ghost" disabled>
                <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
              </Button>
            </Tooltip>
          )
          : (
            <Popconfirm placement="bottom" title="确定停止吗？" okText="Yes" cancelText="No" onConfirm={this.stopJobBtn(record, 'stop')}>
              <Tooltip title="停止">
                <Button shape="circle" type="ghost">
                  <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
                </Button>
              </Tooltip>
            </Popconfirm>
          )

        const strDelete = record.disableActions.indexOf('delete') > -1
          ? (
            <Tooltip title="删除">
              <Button icon="delete" shape="circle" type="ghost" disabled></Button>
            </Tooltip>
          )
          : (
            <Popconfirm placement="bottom" title="确定删除吗？" okText="Yes" cancelText="No" onConfirm={this.deleteJobBtn(record, 'delete')}>
              <Tooltip title="删除">
                <Button icon="delete" shape="circle" type="ghost"></Button>
              </Tooltip>
            </Popconfirm>
          )

        let jobActionSelect = ''
        if (localStorage.getItem('loginRoleType') === 'admin') {
          jobActionSelect = ''
        } else if (localStorage.getItem('loginRoleType') === 'user') {
          jobActionSelect = (
            <span>
              <Tooltip title="修改">
                <Button icon="edit" shape="circle" type="ghost" onClick={onShowEditJob(record)}></Button>
              </Tooltip>
              {strStart}
              {strStop}
              {strDelete}
            </span>
          )
        }

        let jobDetailContent = ''
        if (showJobDetail !== {}) {
          const showJob = showJobDetail.job
          if (showJob) {
            jobDetailContent = (
              <div style={{ width: '600px', overflowY: 'auto', height: '260px', overflowX: 'auto' }}>
                <p className={this.props.jobClassHide}><strong>   Project Name：</strong>{showJobDetail.projectName}</p>
                <p><strong>   Event Ts Start：</strong>{showJob.eventTsStart}</p>
                <p><strong>   Event Ts End：</strong>{showJob.eventTsEnd}</p>
                <p><strong>   Log Path：</strong>{showJob.logPath}</p>
                <p><strong>   Source Config：</strong>{showJob.sourceConfig}</p>
                <p><strong>   Sink Config：</strong>{showJob.sinkConfig}</p>
                <p><strong>   Transformation Config：</strong>{showJob.tranConfig}</p>
                <p><strong>   Create Time：</strong>{showJob.createTime}</p>
                <p><strong>   Update Time：</strong>{showJob.updateTime}</p>
                <p><strong>   Create By：</strong>{showJob.createBy}</p>
                <p><strong>   Update By：</strong>{showJob.updateBy}</p>
              </div>
            )
          }
        }

        return (
          <span className="ant-table-action-column">
            <Tooltip title="查看详情">
              <Popover
                placement="left"
                content={jobDetailContent}
                title={<h3>详情</h3>}
                trigger="click"
                onVisibleChange={this.handleVisibleChangeJob(record)}>
                <Button icon="file-text" shape="circle" type="ghost"></Button>
              </Popover>
            </Tooltip>
            {jobActionSelect}
          </span>
        )
      }
    }, {
      title: 'Logs',
      key: 'logs',
      className: 'text-align-center',
      render: (text, record) => (
        <Tooltip title="logs">
          <Button shape="circle" type="ghost" onClick={this.onShowJobLogs(record)}>
            <i className="iconfont icon-log"></i>
          </Button>
        </Tooltip>
      )
    }]

    const pagination = {
      defaultPageSize: 10,
      showSizeChanger: true,
      onChange: (current) => {
        this.setState({
          pageIndex: current
        })
      }
    }

    let jobAddOrNot = ''
    if (localStorage.getItem('loginRoleType') === 'admin') {
      jobAddOrNot = ''
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      jobAddOrNot = (
        <Button icon="plus" type="primary" onClick={onShowAddJob}>新建</Button>
      )
    }

    const helmetHide = this.props.jobClassHide !== 'hide'
      ? (<Helmet title="Job" />)
      : (<Helmet title="Workbench" />)

    return (
      <div className={`ri-workbench-table ri-common-block ${className}`}>
        {helmetHide}
        <h3 className="ri-common-block-title">
          <Icon type="bars" /> Job 列表
        </h3>
        <div className="ri-common-block-tools">
          {jobAddOrNot}
          <Button icon="poweroff" type="ghost" className="refresh-button-style" loading={refreshJobLoading} onClick={this.refreshJob}>{refreshJobText}</Button>
        </div>
        <Table
          dataSource={this.state.currentJobs}
          columns={columns}
          onChange={this.handleJobChange}
          pagination={pagination}
          className="ri-workbench-table-container"
          bordered>
        </Table>
        <Modal
          title="Logs"
          visible={this.state.logsJobModalVisible}
          onCancel={this.handleLogsCancel}
          wrapClassName="ant-modal-xlarge ant-modal-no-footer"
          footer={<span></span>}
        >
          <JobLogs
            jobLogsContent={this.state.jobLogsContent}
            refreshJobLogLoading={this.state.refreshJobLogLoading}
            refreshJobLogText={this.state.refreshJobLogText}
            onInitRefreshLogs={this.onInitRefreshLogs}
            logsProjectId={this.state.logsProjectId}
            logsJobId={this.state.logsJobId}
            ref={(f) => { this.streamLogs = f }}
          />
        </Modal>
      </div>
    )
  }
}

Job.propTypes = {
  projectIdGeted: React.PropTypes.string,
  jobClassHide: React.PropTypes.string,
  className: React.PropTypes.string,
  onShowAddJob: React.PropTypes.func,

  onLoadAdminAllJobs: React.PropTypes.func,
  onLoadUserAllJobs: React.PropTypes.func,
  onLoadAdminSingleJob: React.PropTypes.func,
  onLoadAdminJobLogs: React.PropTypes.func,
  onLoadUserJobLogs: React.PropTypes.func,
  onOperateJob: React.PropTypes.func,
  onShowEditJob: React.PropTypes.func,
  onLoadJobDetail: React.PropTypes.func
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadAdminAllJobs: (resolve) => dispatch(loadAdminAllJobs(resolve)),
    onLoadUserAllJobs: (projectId, resolve) => dispatch(loadUserAllJobs(projectId, resolve)),
    onLoadAdminSingleJob: (projectId, resolve) => dispatch(loadAdminSingleJob(projectId, resolve)),
    onLoadAdminJobLogs: (projectId, jobId, resolve) => dispatch(loadAdminJobLogs(projectId, jobId, resolve)),
    onLoadUserJobLogs: (projectId, jobId, resolve) => dispatch(loadUserJobLogs(projectId, jobId, resolve)),
    onOperateJob: (values, resolve, reject) => dispatch(operateJob(values, resolve, reject)),
    onLoadJobDetail: (value, resolve) => dispatch(loadJobDetail(value, resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  jobs: selectJobs(),
  error: selectError()
})

export default connect(mapStateToProps, mapDispatchToProps)(Job)

