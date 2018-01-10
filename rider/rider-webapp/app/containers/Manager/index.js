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
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'

import StreamLogs from './StreamLogs'
import StreamStartForm from './StreamStartForm'
import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Icon from 'antd/lib/icon'
import Popover from 'antd/lib/popover'
import Input from 'antd/lib/input'
import Popconfirm from 'antd/lib/popconfirm'
import Tooltip from 'antd/lib/tooltip'
import Tag from 'antd/lib/tag'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker
import { uuid, isEquivalent } from '../../utils/util'

import {loadUserStreams, loadAdminSingleStream, loadAdminAllStreams, operateStream, startOrRenewStream,
  deleteStream, loadStreamDetail, loadLogsInfo, loadAdminLogsInfo, loadLastestOffset} from './action'
import {loadSingleUdf} from '../Udf/action'
import {selectStreams} from './selectors'

export class Manager extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      modalLoading: false,
      refreshStreamLoading: false,
      refreshStreamText: 'Refresh',
      refreshLogLoading: false,
      refreshLogText: 'Refresh',

      originStreams: [],
      currentStreams: [],
      selectedRowKeys: [],

      editModalVisible: false,
      logsModalVisible: false,
      startModalVisible: false,
      showStreamdetails: null,
      streamStartFormData: [],
      topicInfoModal: '',
      streamIdGeted: 0,
      actionType: '',

      filteredInfo: null,
      sortedInfo: null,

      searchTextStreamProject: '',
      filterDropdownVisibleStreamProject: false,
      searchTextName: '',
      filterDropdownVisibleName: false,
      searchTextSparkAppid: '',
      filterDropdownVisibleSparkAppid: false,
      searchTextInstance: '',
      filterDropdownVisibleInstance: false,
      filterDatepickerShown: false,
      startTimeText: '',
      endTimeText: '',
      startStartTimeText: '',
      startEndTimeText: '',
      filterDropdownVisibleStartTime: false,
      endStartTimeText: '',
      endEndTimeText: '',
      filterDropdownVisibleEndTime: false,

      columnNameText: '',
      valueText: '',
      visibleBool: false,
      startTimeTextState: '',
      endTimeTextState: '',
      paginationInfo: null,

      logsContent: '',
      logsProjectId: 0,
      logsStreamId: 0,

      startUdfVals: [],
      renewUdfVals: [],
      currentUdfVal: [],

      consumedOffsetValue: [],
      kafkaOffsetValue: []
    }
  }

  componentWillMount () {
    this.refreshStream()
  }

  componentWillReceiveProps (props) {
    if (props.streams) {
      const originStreams = props.streams.map(s => {
        const responseOriginStream = Object.assign({}, s.stream, {
          disableActions: s.disableActions,
          topicInfo: s.topicInfo,
          instance: s.kafkaInfo.instance,
          connUrl: s.kafkaInfo.connUrl,
          projectName: s.projectName,
          currentUdf: s.currentUdf,
          usingUdf: s.usingUdf
        })
        responseOriginStream.key = responseOriginStream.id
        responseOriginStream.visible = false
        return responseOriginStream
      })

      this.setState({
        originStreams: originStreams.slice(),
        currentStreams: originStreams.slice()
      })
    }
  }

  refreshStream = () => {
    this.setState({
      refreshStreamLoading: true,
      refreshStreamText: 'Refreshing'
    })
    this.loadStreamData()
  }

  loadStreamData () {
    if (localStorage.getItem('loginRoleType') === 'admin') {
      this.props.streamClassHide === 'hide'
        ? this.props.onLoadAdminSingleStream(this.props.projectIdGeted, () => { this.refreshStreamState() })
        : this.props.onLoadAdminAllStreams(() => { this.refreshStreamState() })
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      this.props.onLoadUserStreams(this.props.projectIdGeted, () => { this.refreshStreamState() })
    }
  }

  refreshStreamState () {
    this.setState({
      refreshStreamLoading: false,
      refreshStreamText: 'Refresh'
    })
    const { columnNameText, valueText, visibleBool } = this.state
    const { paginationInfo, filteredInfo, sortedInfo } = this.state
    const { startTimeTextState, endTimeTextState } = this.state

    if (columnNameText !== '') {
      if (columnNameText === 'startedTime' || columnNameText === 'stoppedTime') {
        this.onRangeTimeSearch(columnNameText, startTimeTextState, endTimeTextState, visibleBool)()
      } else {
        this.handleStreamChange(paginationInfo, filteredInfo, sortedInfo)
        this.onSearch(columnNameText, valueText, visibleBool)()
      }
    }
  }

  handleVisibleChange = (stream) => (visible) => {
    // visible=true时，调接口，获取最新详情
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

        this.props.onLoadStreamDetail(stream.projectId, stream.id, roleType, (result) => {
          this.setState({
            showStreamdetails: result
          })
        })
      })
    }
  }

  updateStream = (record) => (e) => {
    const { projectIdGeted } = this.props

    this.setState({
      actionType: 'renew',
      startModalVisible: true,
      streamIdGeted: record.id
    })

    new Promise((resolve) => {
      this.props.onLoadStreamDetail(projectIdGeted, record.id, 'user', (result) => {
        resolve(result)

        this.setState({
          topicInfoModal: result.topicInfo.length === 0 ? 'hide' : '',
          streamStartFormData: result.topicInfo,
          currentUdfVal: result.currentUdf
        })
      })
    })
      .then((result) => {
        // 与 user UDF table 相同的接口获得全部的 UDF
        this.props.onLoadSingleUdf(projectIdGeted, 'user', (resultUdf) => {
          // 下拉框除去回显的 UDFs，id options
          const resultCurrentUdf = result.currentUdf
          const resultUdfIdArr = resultUdf.map(i => i.id)
          const currentUdfIdArr = resultCurrentUdf.map(i => i.id)

          let renewUdfValIds = []
          let tmp = resultUdfIdArr.concat(currentUdfIdArr)
          let o = {}
          for (let i = 0; i < tmp.length; i++) (tmp[i] in o) ? o[tmp[i]] ++ : o[tmp[i]] = 1
          for (let x in o) if (o[x] === 1) renewUdfValIds.push(x)

          // id:value 键值对
          let arrTemp = []
          for (let m = 0; m < resultUdf.length; m++) {
            const objTemp = {
              [resultUdf[m].id]: resultUdf[m]
            }
            arrTemp.push(objTemp)
          }

          // 根据 id 找到对应的项，形成数组
          let renewUdfValFinal = []
          for (let n = 0; n < arrTemp.length; n++) {
            for (let k = 0; k < renewUdfValIds.length; k++) {
              const tt = renewUdfValIds[k]
              if (arrTemp[n][tt] !== undefined) {
                renewUdfValFinal.push(arrTemp[n][tt])
              }
            }
          }

          this.setState({
            renewUdfVals: renewUdfValFinal
          })
        })

        // 显示 lastest offset
        this.props.onLoadLastestOffset(projectIdGeted, record.id, (result) => {
          this.setState({
            consumedOffsetValue: result.consumedLatestOffset,
            kafkaOffsetValue: result.kafkaLatestOffset

          })
        })
      })
  }

  /**
   * start操作  获取最新数据，并回显
   * @param record
   */
  onShowEditStart = (record) => (e) => {
    const { projectIdGeted } = this.props

    this.setState({
      actionType: 'start',
      startModalVisible: true,
      streamIdGeted: record.id
    })

    // 单条查询接口获得回显的topic Info，回显选中的UDFs
    this.props.onLoadStreamDetail(projectIdGeted, record.id, 'user', (result) => {
      this.setState({
        topicInfoModal: result.topicInfo.length === 0 ? 'hide' : '',
        streamStartFormData: result.topicInfo
      })

      // 回显选中的 topic，必须有 id
      const currentUdfTemp = result.currentUdf
      let topicsSelectValue = []
      for (let i = 0; i < currentUdfTemp.length; i++) {
        topicsSelectValue.push(`${currentUdfTemp[i].id}`)
      }

      this.streamStartForm.setFieldsValue({
        udfs: topicsSelectValue
      })
    })

    // 与user UDF table相同的接口获得全部的UDFs
    this.props.onLoadSingleUdf(projectIdGeted, 'user', (result) => {
      const allOptionVal = {
        createBy: 1,
        createTime: '',
        desc: '',
        fullClassName: '',
        functionName: '全选',
        id: -1,
        jarName: '',
        pubic: false,
        updateBy: 1,
        updateTime: ''
      }
      result.unshift(allOptionVal)
      this.setState({
        startUdfVals: result
      })
    })

    // 显示 lastest offset
    this.props.onLoadLastestOffset(projectIdGeted, record.id, (result) => {
      this.setState({
        consumedOffsetValue: result.consumedLatestOffset,
        kafkaOffsetValue: result.kafkaLatestOffset
      })
    })
  }

  queryLastestoffset = (e) => {
    const { projectIdGeted } = this.props
    const { streamIdGeted } = this.state

    this.loadLastestOffsetFunc(projectIdGeted, streamIdGeted)
  }

  // Load Lastest Offset
  loadLastestOffsetFunc (projectId, streamId) {
    this.props.onLoadLastestOffset(projectId, streamId, (result) => {
      this.setState({
        consumedOffsetValue: result.consumedLatestOffset,
        kafkaOffsetValue: result.kafkaLatestOffset
      })
      // for (let k = 0; k < result.length; k++) {
      //   const partitionAndOffset = result[k].partitionOffsets.split(',')
      //   for (let j = 0; j < partitionAndOffset.length; j++) {
      //     this.streamStartForm.setFieldsValue({
      //       [`latest_${result[k].id}_${j}`]: partitionAndOffset[j].substring(partitionAndOffset[j].indexOf(':') + 1)
      //     })
      //   }
      // }
    })
  }

  onChangeEditSelect = () => {
    const { streamStartFormData } = this.state

    for (let k = 0; k < streamStartFormData.length; k++) {
      const partitionAndOffset = streamStartFormData[k].partitionOffsets.split(',')

      for (let j = 0; j < partitionAndOffset.length; j++) {
        this.streamStartForm.setFieldsValue({
          [`${streamStartFormData[k].id}_${j}`]: partitionAndOffset[j].substring(partitionAndOffset[j].indexOf(':') + 1),
          [`${streamStartFormData[k].rate}`]: streamStartFormData[k].rate
        })
      }
    }
  }

  /**
   *  start/renew ok
   * @param e
   */
  handleEditStartOk = (e) => {
    const { actionType, streamIdGeted, streamStartFormData, startUdfVals } = this.state
    const { projectIdGeted } = this.props

    this.streamStartForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        let requestVal = {}
        if (actionType === 'start') {
          if (streamStartFormData.length === 0) {
            if (values.udfs === undefined || values.udfs.length === 0) {
              requestVal = {}
            } else {
              if (values.udfs.find(i => i === '-1')) {
                // 全选
                const startUdfValsOrigin = startUdfVals.filter(k => k.id !== -1)
                requestVal = {
                  udfInfo: startUdfValsOrigin.map(p => p.id)
                }
              } else {
                requestVal = {
                  udfInfo: values.udfs.map(q => Number(q))
                }
              }
            }
          } else {
            const mergedData = streamStartFormData.map((i) => {
              const parOffTemp = i.partitionOffsets
              const partitionTemp = parOffTemp.split(',')

              const offsetArr = []
              for (let r = 0; r < partitionTemp.length; r++) {
                const offsetArrTemp = values[`${i.id}_${r}`]
                if (offsetArrTemp === '') {
                  message.warning('Offset 不能为空！', 3)
                } else {
                  offsetArr.push(`${r}:${offsetArrTemp}`)
                }
              }
              const offsetVal = offsetArr.join(',')

              const robj = {
                id: i.id,
                partitionOffsets: offsetVal,
                rate: Number(values[`${i.id}_${i.rate}`])
              }
              return robj
            })

            if (values.udfs === undefined || values.udfs.length === 0) {
              requestVal = {
                topicInfo: mergedData
              }
            } else {
              if (values.udfs.find(i => i === '-1')) {
                // 全选
                const startUdfValsOrigin = startUdfVals.filter(k => k.id !== -1)
                requestVal = {
                  udfInfo: startUdfValsOrigin.map(p => p.id),
                  topicInfo: mergedData
                }
              } else {
                requestVal = {
                  udfInfo: values.udfs.map(q => Number(q)),
                  topicInfo: mergedData
                }
              }
            }
          }
        } else if (actionType === 'renew') {
          if (streamStartFormData.length === 0) {
            if (values.udfs === undefined || values.udfs.length === 0) {
              requestVal = {}
            } else {
              requestVal = {
                udfInfo: values.udfs.map(q => Number(q))
              }
            }
          } else {
            const mergedData = streamStartFormData.map((i) => {
              const parOffTemp = i.partitionOffsets
              const partitionTemp = parOffTemp.split(',')

              const offsetArr = []
              for (let r = 0; r < partitionTemp.length; r++) {
                const offsetArrTemp = values[`${i.id}_${r}`]
                if (offsetArrTemp === '') {
                  message.warning('Offset 不能为空！', 3)
                } else {
                  offsetArr.push(`${r}:${offsetArrTemp}`)
                }
              }
              const offsetVal = offsetArr.join(',')

              const robj = {
                id: i.id,
                name: i.name,
                partitionOffsets: offsetVal,
                rate: Number(values[`${i.id}_${i.rate}`])
              }
              return robj
            })

            // 接口参数：改动的topicInfo
            let topicInfoTemp = []
            for (let f = 0; f < streamStartFormData.length; f++) {
              for (let g = 0; g < mergedData.length; g++) {
                if (streamStartFormData[f].id === mergedData[g].id) {
                  if (!isEquivalent(streamStartFormData[f], mergedData[g])) {
                    topicInfoTemp.push({
                      id: mergedData[g].id,
                      partitionOffsets: mergedData[g].partitionOffsets,
                      rate: mergedData[g].rate
                    })
                  }
                }
              }
            }

            if (topicInfoTemp.length === 0) {
              requestVal = (values.udfs === undefined || values.udfs.length === 0)
                ? {}
                : requestVal = { udfInfo: values.udfs.map(q => Number(q)) }
            } else {
              requestVal = (values.udfs === undefined || values.udfs.length === 0)
                ? { topicInfo: topicInfoTemp }
                : {
                  udfInfo: values.udfs.map(q => Number(q)),
                  topicInfo: topicInfoTemp
                }
            }
          }
        }

        let actionTypeRequest = ''
        let actionTypeMsg = ''
        if (actionType === 'start') {
          actionTypeRequest = 'start'
          actionTypeMsg = '启动成功！'
        } else if (actionType === 'renew') {
          actionTypeRequest = 'renew'
          actionTypeMsg = '生效！'
        }

        this.props.onStartOrRenewStream(projectIdGeted, streamIdGeted, requestVal, actionTypeRequest, () => {
          this.setState({
            startModalVisible: false,
            streamStartFormData: [],
            modalLoading: false
          })

          message.success(actionTypeMsg, 3)
        }, (result) => {
          message.error(`操作失败：${result}`, 3)
          this.setState({
            modalLoading: false
          })
        })
      }
    })
  }

  handleEditStartCancel = (e) => {
    this.setState({
      startModalVisible: false,
      streamStartFormData: []
    })

    this.streamStartForm.resetFields()
  }

  stopStreamBtn = (record, action) => (e) => {
    this.props.onOperateStream(this.props.projectIdGeted, record.id, 'stop', () => {
      message.success('停止成功！', 3)
    }, (result) => {
      message.error(`操作失败：${result}`, 3)
    })
  }

  deleteStreambtn = (record, action) => (e) => {
    this.props.onDeleteStream(this.props.projectIdGeted, record.id, 'delete', () => {
      message.success('删除成功！', 3)
    }, (result) => {
      message.error(`操作失败：${result}`, 3)
    })
  }

  onShowLogs = (record) => (e) => {
    this.setState({
      logsModalVisible: true,
      logsProjectId: record.projectId,
      logsStreamId: record.id
    })
    this.loadLogsData(record.projectId, record.id)
  }

  onInitRefreshLogs = (projectId, streamId) => {
    this.setState({
      refreshLogLoading: true,
      refreshLogText: 'Refreshing'
    })
    this.loadLogsData(projectId, streamId)
  }

  loadLogsData = (projectId, streamId) => {
    if (localStorage.getItem('loginRoleType') === 'admin') {
      this.props.onLoadAdminLogsInfo(projectId, streamId, (result) => {
        this.setState({
          logsContent: result
        })
        this.streamLogRefreshState()
      })
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      this.props.onLoadLogsInfo(projectId, streamId, (result) => {
        this.setState({
          logsContent: result
        })
        this.streamLogRefreshState()
      })
    }
  }

  streamLogRefreshState () {
    this.setState({
      refreshLogLoading: false,
      refreshLogText: 'Refresh'
    })
  }

  handleLogsCancel = (e) => {
    this.setState({
      logsModalVisible: false
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
      filteredInfo: {status: [], streamType: []}
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        startTimeTextState: startTimeText,
        endTimeTextState: endTimeText,
        visibleBool: visible,
        currentStreams: this.state.originStreams.map((record) => {
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

  handleEndOpenChange = (status) => this.setState({ filterDatepickerShown: status })

  onRangeTimeChange = (value, dateString) => {
    this.setState({
      startTimeText: dateString[0],
      endTimeText: dateString[1]
    })
  }

  handleStreamChange = (pagination, filters, sorter) => {
    const { filteredInfo } = this.state

    let filterValue = {}
    if (filteredInfo !== null) {
      if (filteredInfo) {
        if (filters.status && filters.streamType) {
          if (filters.status.length === 0 && filters.streamType.length === 0) {
            return
          } else {
            this.onSearch('', '', false)()
            if (filteredInfo.status && filteredInfo.streamType) {
              if (filteredInfo.status.length !== 0 && filters.streamType.length !== 0) {
                filterValue = {status: [], streamType: filters.streamType}
              } else if (filteredInfo.streamType.length !== 0 && filters.status.length !== 0) {
                filterValue = {status: filters.status, streamType: []}
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

  onInputChange = (value) => (e) => this.setState({[value]: e.target.value})

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      filteredInfo: {status: [], streamType: []}
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        valueText: value,
        visibleBool: visible,
        currentStreams: this.state.originStreams.map((record) => {
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

  render () {
    const { refreshStreamLoading, refreshStreamText, showStreamdetails } = this.state
    const { className, onShowAddStream, onShowEditStream, streamClassHide } = this.props

    let {
      modalLoading,
      sortedInfo,
      filteredInfo,
      startModalVisible
    } = this.state
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
      className: `${streamClassHide}`,
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
            value={this.state.searchTextStreamProject}
            onChange={this.onInputChange('searchTextStreamProject')}
            onPressEnter={this.onSearch('projectName', 'searchTextStreamProject', 'filterDropdownVisibleStreamProject')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('projectName', 'searchTextStreamProject', 'filterDropdownVisibleStreamProject')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleStreamProject,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleStreamProject: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
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
      title: 'App Id',
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
            value={this.state.searchTextSparkAppid}
            onChange={this.onInputChange('searchTextSparkAppid')}
            onPressEnter={this.onSearch('sparkAppid', 'searchTextSparkAppid', 'filterDropdownVisibleSparkAppid')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('sparkAppid', 'searchTextSparkAppid', 'filterDropdownVisibleSparkAppid')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleSparkAppid,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleSparkAppid: visible
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
        {text: 'waiting', value: 'waiting'},
        {text: 'running', value: 'running'},
        {text: 'stopping', value: 'stopping'},
        {text: 'stopped', value: 'stopped'},
        {text: 'failed', value: 'failed'}
      ],
      filteredValue: filteredInfo.status,
      onFilter: (value, record) => record.status.includes(value),
      render: (text, record) => {
        let streamStatusColor = ''
        if (record.status === 'new') {
          streamStatusColor = 'orange'
        } else if (record.status === 'starting') {
          streamStatusColor = 'green'
        } else if (record.status === 'waiting') {
          streamStatusColor = '#22D67C'
        } else if (record.status === 'running') {
          streamStatusColor = 'green-inverse'
        } else if (record.status === 'stopping') {
          streamStatusColor = 'gray'
        } else if (record.status === 'stopped') {
          streamStatusColor = '#545252'
        } else if (record.status === 'failed') {
          streamStatusColor = 'red-inverse'
        }

        return (
          <div>
            <Tag color={streamStatusColor} className="stream-style">{record.status}</Tag>
          </div>
        )
      }
    }, {
      title: 'Type',
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
      title: 'Kafka',
      dataIndex: 'instance',
      key: 'instance',
      sorter: (a, b) => {
        if (typeof a.instance === 'object') {
          return a.instanceOrigin < b.instanceOrigin ? -1 : 1
        } else {
          return a.instance < b.instance ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'instance' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Kafka"
            value={this.state.searchTextInstance}
            onChange={this.onInputChange('searchTextInstance')}
            onPressEnter={this.onSearch('instance', 'searchTextInstance', 'filterDropdownVisibleInstance')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('instance', 'searchTextInstance', 'filterDropdownVisibleInstance')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleInstance,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleInstance: visible
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
      // filteredValue: filteredInfo.startedTime,
      filterDropdown: (
        <div className="custom-filter-dropdown-style">
          <RangePicker
            showTime
            format="YYYY-MM-DD HH:mm:ss"
            placeholder={['Start', 'End']}
            onOpenChange={this.handleEndOpenChange}
            onChange={this.onRangeTimeChange}
            onPressEnter={this.onRangeTimeSearch('startedTime', 'startStartTimeText', 'startEndTimeText', 'filterDropdownVisibleStartTime')}
          />
          <Button type="primary" className="rangeFilter" onClick={this.onRangeTimeSearch('startedTime', 'startStartTimeText', 'startEndTimeText', 'filterDropdownVisibleStartTime')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleStartTime,
      onFilterDropdownVisibleChange: visible => {
        if (!this.state.filterDatepickerShown) {
          this.setState({ filterDropdownVisibleStartTime: visible })
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
      // filteredValue: filteredInfo.stoppedTime,
      filterDropdown: (
        <div className="custom-filter-dropdown-style">
          <RangePicker
            showTime
            format="YYYY-MM-DD HH:mm:ss"
            placeholder={['Start', 'End']}
            onOpenChange={this.handleEndOpenChange}
            onChange={this.onRangeTimeChange}
            onPressEnter={this.onRangeTimeSearch('stoppedTime', 'endStartTimeText', 'endEndTimeText', 'filterDropdownVisibleEndTime')}
          />
          <Button type="primary" className="rangeFilter" onClick={this.onRangeTimeSearch('stoppedTime', 'endStartTimeText', 'endEndTimeText', 'filterDropdownVisibleEndTime')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleEndTime,
      onFilterDropdownVisibleChange: visible => {
        if (!this.state.filterDatepickerShown) {
          this.setState({ filterDropdownVisibleEndTime: visible })
        }
      }
    }, {
      title: 'Action',
      key: 'action',
      className: 'text-align-center',
      render: (text, record) => {
        const stream = this.state.currentStreams.find(s => s.id === record.id)
        let streamActionSelect = ''
        if (localStorage.getItem('loginRoleType') === 'admin') {
          streamActionSelect = ''
        } else if (localStorage.getItem('loginRoleType') === 'user') {
          const strDelete = record.disableActions.indexOf('delete') > -1
            ? (
              <Tooltip title="删除">
                <Button icon="delete" shape="circle" type="ghost" disabled></Button>
              </Tooltip>
            )
            : (
              <Popconfirm placement="bottom" title="确定删除吗？" okText="Yes" cancelText="No" onConfirm={this.deleteStreambtn(record, 'delete')}>
                <Tooltip title="删除">
                  <Button icon="delete" shape="circle" type="ghost"></Button>
                </Tooltip>
              </Popconfirm>
            )

          let streamStartDisabled = false
          let streamRenewDisabled = false
          let strStop = ''
          let strStopDisabled = (
            <Tooltip title="停止">
              <Button shape="circle" type="ghost" disabled>
                <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
              </Button>
            </Tooltip>
          )
          let strStopDisabledNot = (
            <Popconfirm placement="bottom" title="确定停止吗？" okText="Yes" cancelText="No" onConfirm={this.stopStreamBtn(record, 'stop')}>
              <Tooltip title="停止">
                <Button shape="circle" type="ghost">
                  <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
                </Button>
              </Tooltip>
            </Popconfirm>
          )

          if (record.disableActions.indexOf('start') < 0 && record.disableActions.indexOf('renew') < 0) {
            // disableActions === 'stop'
            strStop = strStopDisabled
          } else if (record.disableActions.indexOf('start') < 0 && record.disableActions.indexOf('stop') < 0) {
            // disableActions === 'renew'
            streamRenewDisabled = true
            strStop = strStopDisabledNot
          } else if (record.disableActions.indexOf('renew') < 0 && record.disableActions.indexOf('stop') < 0) {
            // disableActions === 'start'
            streamStartDisabled = true
            strStop = strStopDisabledNot
          } else if (record.disableActions.indexOf('start') < 0) {
            // disableActions === stop, renew
            streamRenewDisabled = true
            strStop = strStopDisabled
          } else if (record.disableActions.indexOf('stop') < 0) {
            // disableActions === start, renew
            streamStartDisabled = true
            streamRenewDisabled = true
            strStop = strStopDisabledNot
          } else if (record.disableActions.indexOf('renew') < 0) {
            // disableActions === start, stop
            streamStartDisabled = true
            strStop = strStopDisabled
          } else if (record.disableActions.indexOf('start') < 0 && record.disableActions.indexOf('stop') < 0 && record.disableActions.indexOf('renew') < 0) {
            // disableActions === ''
            strStop = strStopDisabledNot
          } else {
            // disableActions === start, stop, renew
            streamStartDisabled = true
            streamRenewDisabled = true
            strStop = strStopDisabled
          }

          streamActionSelect = (
            <span>
              <Tooltip title="修改">
                <Button icon="edit" shape="circle" type="ghost" onClick={onShowEditStream(record)}></Button>
              </Tooltip>

              <Tooltip title="开始">
                <Button icon="caret-right" shape="circle" type="ghost" onClick={this.onShowEditStart(record, 'start')} disabled={streamStartDisabled}></Button>
              </Tooltip>

              {strStop}

              <Tooltip title="生效">
                <Button icon="check" shape="circle" type="ghost" onClick={this.updateStream(record, 'renew')} disabled={streamRenewDisabled}></Button>
              </Tooltip>

              {strDelete}
            </span>
          )
        }

        let streamDetailContent = ''
        if (showStreamdetails) {
          const detailTemp = showStreamdetails.stream

          const topicTemp = showStreamdetails.topicInfo
          const topicFinal = topicTemp.map(s => (
            <li key={s.id}>
              <strong>Topic Name：</strong>{s.name}
              <strong>；Partition Offsets：</strong>{s.partitionOffsets}
              <strong>；Rate：</strong>{s.rate}
            </li>
          ))

          const currentudfTemp = showStreamdetails.currentUdf
          const currentUdfFinal = currentudfTemp.length !== 0
            ? currentudfTemp.map(s => (
              <li key={s.id}>
                <strong>Function Name：</strong>{s.functionName}
                <strong>；Full Class Name：</strong>{s.fullClassName}
                <strong>；Jar Name：</strong>{s.jarName}
              </li>
              ))
            : null

          const usingUdfTemp = showStreamdetails.usingUdf
          const usingUdfTempFinal = usingUdfTemp.length !== 0
            ? usingUdfTemp.map(s => (
              <li key={uuid()}>
                <strong>Function Name：</strong>{s.functionName}
                <strong>；Full Class Name：</strong>strong>{s.fullClassName}
                <strong>；Jar Name：</strong>{s.jarName}
              </li>
            ))
            : null

          streamDetailContent = (
            <div className="stream-detail">
              <p className={this.props.streamClassHide}><strong>   Project Id：</strong>{detailTemp.projectId}</p>
              <p><strong>   Topic Info：</strong>{topicFinal}</p>
              <p><strong>   Current Udf：</strong>{currentUdfFinal}</p>
              <p><strong>   Using Udf：</strong>{usingUdfTempFinal}</p>
              <p><strong>   Description：</strong>{detailTemp.desc}</p>

              <p><strong>   Launch Config：</strong>{detailTemp.launchConfig}</p>
              <p><strong>   spark Config：</strong>{detailTemp.sparkConfig}</p>
              <p><strong>   start Config：</strong>{detailTemp.startConfig}</p>

              <p><strong>   Create Time：</strong>{detailTemp.createTime}</p>
              <p><strong>   Update Time：</strong>{detailTemp.updateTime}</p>
              <p><strong>   Create By：</strong>{detailTemp.createBy}</p>
              <p><strong>   Update By：</strong>{detailTemp.updateBy}</p>
              <p><strong>   Disable Actions：</strong>{showStreamdetails.disableActions}</p>
            </div>
          )
        }

        return (
          <span className="ant-table-action-column">
            <Tooltip title="查看详情">
              <Popover
                placement="left"
                content={streamDetailContent}
                title={<h3>详情</h3>}
                trigger="click"
                onVisibleChange={this.handleVisibleChange(stream)}>
                <Button icon="file-text" shape="circle" type="ghost"></Button>
              </Popover>
            </Tooltip>
            {streamActionSelect}
          </span>
        )
      }
    }, {
      title: 'Logs',
      key: 'logs',
      className: 'text-align-center',
      render: (text, record) => (
        <Tooltip title="logs">
          <Button shape="circle" type="ghost" onClick={this.onShowLogs(record)}>
            <i className="iconfont icon-log"></i>
          </Button>
        </Tooltip>
      )
    }]

    const pagination = {
      defaultPageSize: 10,
      showSizeChanger: true,
      onChange: (current) => {
        console.log('Current: ', current)
      }
    }

    const streamStartForm = startModalVisible
      ? (
        <StreamStartForm
          data={this.state.streamStartFormData}
          consumedOffsetValue={this.state.consumedOffsetValue}
          kafkaOffsetValue={this.state.kafkaOffsetValue}
          streamActionType={this.state.actionType}
          startUdfValsOption={this.state.startUdfVals}
          renewUdfValsOption={this.state.renewUdfVals}
          currentUdfVal={this.state.currentUdfVal}
          ref={(f) => { this.streamStartForm = f }}
        />
      )
      : ''

    let StreamAddOrNot = ''
    if (localStorage.getItem('loginRoleType') === 'admin') {
      StreamAddOrNot = ''
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      StreamAddOrNot = (
        <Button icon="plus" type="primary" onClick={onShowAddStream}>新建</Button>
      )
    }

    const helmetHide = this.props.streamClassHide !== 'hide'
      ? (<Helmet title="Stream" />)
      : (<Helmet title="Workbench" />)

    const { topicInfoModal } = this.state
    const editBtn = topicInfoModal === '' ? '' : 'hide'

    return (
      <div className={`ri-workbench-table ri-common-block ${className}`}>
        {helmetHide}
        <h3 className="ri-common-block-title">
          <Icon type="bars" /> Stream 列表
        </h3>
        <div className="ri-common-block-tools">
          {StreamAddOrNot}
          <Button icon="poweroff" type="ghost" className="refresh-button-style" loading={refreshStreamLoading} onClick={this.refreshStream}>{refreshStreamText}</Button>
        </div>
        <Table
          dataSource={this.state.currentStreams}
          columns={columns}
          onChange={this.handleStreamChange}
          pagination={pagination}
          className="ri-workbench-table-container"
          bordered>
        </Table>

        <Modal
          title={`确定${this.state.actionType === 'start' ? '开始' : '生效'}吗？`}
          visible={startModalVisible}
          wrapClassName="ant-modal-large stream-start-renew-modal"
          onCancel={this.handleEditStartCancel}
          footer={[
            <Button
              className={`query-offset-btn ${this.state.topicInfoModal}`}
              key="query"
              size="large"
              onClick={this.queryLastestoffset}
            >
              查看 Lastest Offset
            </Button>,
            <Button
              className={`edit-topic-btn ${editBtn}`}
              type="default"
              onClick={this.onChangeEditSelect}
              key="renewEdit"
              size="large">
              还原
            </Button>,
            <Button
              key="cancel"
              size="large"
              onClick={this.handleEditStartCancel}
            >
              取 消
            </Button>,
            <Button
              key="submit"
              size="large"
              type="primary"
              loading={modalLoading}
              onClick={this.handleEditStartOk}
            >
              开 始
            </Button>
          ]}
        >
          {streamStartForm}
        </Modal>

        <Modal
          title="Logs"
          visible={this.state.logsModalVisible}
          onCancel={this.handleLogsCancel}
          wrapClassName="ant-modal-xlarge ant-modal-no-footer"
          footer={<span></span>}
        >
          <StreamLogs
            logsContent={this.state.logsContent}
            refreshLogLoading={this.state.refreshLogLoading}
            refreshLogText={this.state.refreshLogText}
            onInitRefreshLogs={this.onInitRefreshLogs}
            logsProjectId={this.state.logsProjectId}
            logsStreamId={this.state.logsStreamId}
            ref={(f) => { this.streamLogs = f }}
          />
        </Modal>

      </div>
    )
  }
}

Manager.propTypes = {
  className: React.PropTypes.string,
  projectIdGeted: React.PropTypes.string,
  streamClassHide: React.PropTypes.string,
  onLoadUserStreams: React.PropTypes.func,
  onLoadAdminSingleStream: React.PropTypes.func,
  onLoadAdminAllStreams: React.PropTypes.func,
  onShowAddStream: React.PropTypes.func,
  onOperateStream: React.PropTypes.func,
  onDeleteStream: React.PropTypes.func,
  onStartOrRenewStream: React.PropTypes.func,
  onLoadStreamDetail: React.PropTypes.func,
  onLoadLogsInfo: React.PropTypes.func,
  onLoadAdminLogsInfo: React.PropTypes.func,
  onShowEditStream: React.PropTypes.func,
  onLoadSingleUdf: React.PropTypes.func,
  onLoadLastestOffset: React.PropTypes.func
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadUserStreams: (projectId, resolve) => dispatch(loadUserStreams(projectId, resolve)),
    onLoadAdminAllStreams: (resolve) => dispatch(loadAdminAllStreams(resolve)),
    onLoadAdminSingleStream: (projectId, resolve) => dispatch(loadAdminSingleStream(projectId, resolve)),
    onOperateStream: (projectId, id, action, resolve, reject) => dispatch(operateStream(projectId, id, action, resolve, reject)),
    onDeleteStream: (projectId, id, action, resolve, reject) => dispatch(deleteStream(projectId, id, action, resolve, reject)),
    onStartOrRenewStream: (projectId, id, topicResult, action, resolve, reject) => dispatch(startOrRenewStream(projectId, id, topicResult, action, resolve, reject)),
    onLoadStreamDetail: (projectId, streamId, roleType, resolve) => dispatch(loadStreamDetail(projectId, streamId, roleType, resolve)),
    onLoadLogsInfo: (projectId, streamId, resolve) => dispatch(loadLogsInfo(projectId, streamId, resolve)),
    onLoadAdminLogsInfo: (projectId, streamId, resolve) => dispatch(loadAdminLogsInfo(projectId, streamId, resolve)),
    onLoadSingleUdf: (projectId, roleType, resolve) => dispatch(loadSingleUdf(projectId, roleType, resolve)),
    onLoadLastestOffset: (projectId, streamId, resolve) => dispatch(loadLastestOffset(projectId, streamId, resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  streams: selectStreams()
})

export default connect(mapStateToProps, mapDispatchToProps)(Manager)

