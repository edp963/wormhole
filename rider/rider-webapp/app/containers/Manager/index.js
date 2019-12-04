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
import {SortableContainer, SortableElement} from 'react-sortable-hoc'
import arrayMove from 'array-move'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
import { FormattedMessage } from 'react-intl'
import messages from './messages'

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
import Spin from 'antd/lib/spin'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import {
  loadUserStreams, loadFlowList, submitFlowListOfPriority, loadAdminSingleStream, loadAdminAllStreams, operateStream, startOrRenewStream,
  deleteStream, loadStreamDetail, loadLogsInfo, loadAdminLogsInfo, loadLastestOffset, loadUdfs, jumpStreamToFlowFilter,
  loadYarnUi
} from './action'
import { changeTabs } from '../Workbench/action'
import { loadSingleUdf } from '../Udf/action'
import { selectStreams, selectFlows, selectFlowsLoading, selectStreamStartModalLoading, selectFlowsPriorityConfirmLoading } from './selectors'
import { selectLocale } from '../LanguageProvider/selectors'
import { selectRoleType } from '../App/selectors'

import { operateLanguageText, transformStringWithDot } from '../../utils/util'

export class Manager extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      refreshStreamLoading: false,
      refreshStreamText: 'Refresh',
      refreshLogLoading: false,
      refreshLogText: 'Refresh',

      originStreams: [],
      currentStreams: [],
      selectedRowKeys: [],

      currentFlows: [],
      priorityModalVisible: false,

      editModalVisible: false,
      logsModalVisible: false,
      startModalVisible: false,
      showStreamdetails: null,
      streamStartFormData: [],
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

      // consumedOffsetValue: [],
      // kafkaOffsetValue: [],
      // kafkaEarliestOffset: [],
      autoRegisteredTopics: [],
      userDefinedTopics: [],
      tempUserTopics: [],
      unValidate: false
    }
  }

  componentWillMount () {
    const { streamClassHide, roleType } = this.props
    if (roleType === 'admin') {
      if (!streamClassHide) {
        this.props.onLoadAdminAllStreams(() => { this.refreshStreamState() })
      }
    }
  }

  componentWillReceiveProps (props) {
    if (props.streams) {
      const originStreams = props.streams.map(s => {
        const responseOriginStream = Object.assign(s.stream, {
          disableActions: s.disableActions,
          topicInfo: s.topicInfo,
          instance: s.kafkaInfo.instance,
          connUrl: s.kafkaInfo.connUrl,
          projectName: s.projectName,
          currentUdf: s.currentUdf,
          hideActions: s.hideActions
        })
        responseOriginStream.key = responseOriginStream.id
        responseOriginStream.visible = false
        return responseOriginStream
      })

      this.setState({ originStreams: originStreams.slice() })
      this.state.columnNameText === ''
        ? this.setState({ currentStreams: originStreams.slice() })
        : this.searchOperater()
    }
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

  refreshStream = () => {
    const { projectIdGeted, streamClassHide, roleType } = this.props

    this.setState({
      refreshStreamLoading: true,
      refreshStreamText: 'Refreshing'
    })
    if (roleType === 'admin') {
      streamClassHide === 'hide'
        ? this.props.onLoadAdminSingleStream(projectIdGeted, () => { this.refreshStreamState() })
        : this.props.onLoadAdminAllStreams(() => { this.refreshStreamState() })
    } else if (roleType === 'user') {
      this.props.onLoadUserStreams(projectIdGeted, () => { this.refreshStreamState() })
    }
  }

  refreshStreamState () {
    this.setState({
      refreshStreamLoading: false,
      refreshStreamText: 'Refresh'
    })
    const { columnNameText, valueText, visibleBool, paginationInfo, filteredInfo, sortedInfo, startTimeTextState, endTimeTextState } = this.state

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
        const { roleType } = this.props
        this.props.onLoadStreamDetail(stream.projectId, stream.id, roleType, (result) => {
          this.setState({ showStreamdetails: result })
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

          this.setState({ renewUdfVals: renewUdfValFinal })
        }, 'spark')

        // 显示 Latest offset
        this.props.onLoadLastestOffset(projectIdGeted, record.id, (result) => {
          let autoRegisteredTopics = result.autoRegisteredTopics.map(v => {
            v.name = transformStringWithDot(v.name)
            return v
          })
          let userDefinedTopics = result.userDefinedTopics.map(v => {
            v.name = transformStringWithDot(v.name)
            return v
          })
          this.setState({
            autoRegisteredTopics: autoRegisteredTopics,
            userDefinedTopics: userDefinedTopics,
            streamStartFormData: autoRegisteredTopics,
            tempUserTopics: userDefinedTopics.slice()
            // consumedOffsetValue: result.consumedLatestOffset,
            // kafkaOffsetValue: result.kafkaLatestOffset,
            // kafkaEarliestOffset: result.kafkaEarliestOffset
          })
        })
      })
  }

  /**
   * start操作  获取最新数据，并回显
   */
  onShowEditStart = (record) => (e) => {
    const { projectIdGeted, locale } = this.props

    this.setState({
      actionType: 'start',
      startModalVisible: true,
      streamIdGeted: record.id
    })

    // 单条查询接口获得回显的topic Info，回显选中的UDFs
    this.props.onLoadUdfs(projectIdGeted, record.id, 'user', (result) => {
      // 回显选中的 topic，必须有 id
      const currentUdfTemp = result
      let topicsSelectValue = []
      for (let i = 0; i < currentUdfTemp.length; i++) {
        topicsSelectValue.push(`${currentUdfTemp[i].id}`)
      }
      this.streamStartForm.setFieldsValue({ udfs: topicsSelectValue })
    })

    // 与user UDF table相同的接口获得全部的UDFs
    this.props.onLoadSingleUdf(projectIdGeted, 'user', (result) => {
      const allOptionVal = {
        createBy: 1,
        createTime: '',
        desc: '',
        fullClassName: '',
        functionName: locale === 'en' ? 'Select all' : '全选',
        id: -1,
        jarName: '',
        pubic: false,
        updateBy: 1,
        updateTime: ''
      }
      result.unshift(allOptionVal)
      this.setState({ startUdfVals: result })
    }, 'spark')

    // 显示 Latest offset
    this.props.onLoadLastestOffset(projectIdGeted, record.id, (result) => {
      if (result) {
        let autoRegisteredTopics = result.autoRegisteredTopics.map(v => {
          v.name = transformStringWithDot(v.name)
          return v
        })
        let userDefinedTopics = result.userDefinedTopics.map(v => {
          v.name = transformStringWithDot(v.name)
          return v
        })
        this.setState({
          autoRegisteredTopics: autoRegisteredTopics,
          userDefinedTopics: userDefinedTopics,
          streamStartFormData: autoRegisteredTopics
          // consumedOffsetValue: result.consumedLatestOffset,
          // kafkaOffsetValue: result.kafkaLatestOffset,
          // kafkaEarliestOffset: result.kafkaEarliestOffset
        })
      } else {
        this.setState({
          streamStartFormData: []
        })
      }
    })
  }

  startFlink = (id) => () => {
    const { projectIdGeted, locale } = this.props
    this.props.onStartOrRenewStream(projectIdGeted, id, null, 'start', (result) => {
      let actionTypeMsg = locale === 'en' ? 'Start Successfully!' : '启动成功！'
      message.success(actionTypeMsg, 3)
    }, (result) => {
      let failText = locale === 'en' ? 'Operation failed:' : '操作失败：'
      message.error(`${failText} ${result}`, 3)
    })
  }
  queryLastestoffset = (e) => {
    const { projectIdGeted } = this.props
    const { streamIdGeted, userDefinedTopics, autoRegisteredTopics } = this.state
    let topics = {}
    topics.userDefinedTopics = userDefinedTopics.map((v, i) => v.name)
    topics.autoRegisteredTopics = autoRegisteredTopics.map((v, i) => v.name)
    this.loadLastestOffsetFunc(projectIdGeted, streamIdGeted, 'post', topics)
  }

  // Load Latest Offset
  loadLastestOffsetFunc (projectId, streamId, type, topics) {
    this.props.onLoadLastestOffset(projectId, streamId, (result) => {
      let autoRegisteredTopics = result.autoRegisteredTopics.map(v => {
        v.name = transformStringWithDot(v.name)
        return v
      })
      let userDefinedTopics = result.userDefinedTopics.map(v => {
        v.name = transformStringWithDot(v.name)
        return v
      })
      this.setState({
        autoRegisteredTopics: autoRegisteredTopics,
        userDefinedTopics: userDefinedTopics,
        tempUserTopics: userDefinedTopics,
        streamStartFormData: autoRegisteredTopics
        // consumedOffsetValue: result.consumedLatestOffset,
        // kafkaOffsetValue: result.kafkaLatestOffset,
        // kafkaEarliestOffset: result.kafkaEarliestOffset
      })
    }, type, topics)
  }

  onChangeEditSelect = () => {
    const { streamStartFormData, userDefinedTopics } = this.state

    for (let i = 0; i < streamStartFormData.length; i++) {
      const partitionAndOffset = streamStartFormData[i].consumedLatestOffset.split(',')

      for (let j = 0; j < partitionAndOffset.length; j++) {
        this.streamStartForm.setFieldsValue({
          [`${streamStartFormData[i].name}_${j}_auto`]: partitionAndOffset[j].substring(partitionAndOffset[j].indexOf(':') + 1),
          [`${streamStartFormData[i].name}_${streamStartFormData[i].rate}_rate`]: streamStartFormData[i].rate
        })
      }
    }
    for (let i = 0; i < userDefinedTopics.length; i++) {
      const partitionAndOffset = userDefinedTopics[i].consumedLatestOffset.split(',')
      for (let j = 0; j < partitionAndOffset.length; j++) {
        this.streamStartForm.setFieldsValue({
          [`${userDefinedTopics[i].name}_${j}_user`]: partitionAndOffset[j].substring(partitionAndOffset[j].indexOf(':') + 1),
          [`${userDefinedTopics[i].name}_${userDefinedTopics[i].rate}_rate`]: userDefinedTopics[i].rate
        })
      }
    }
  }
  getYarnUi = (record) => () => {
    const { projectIdGeted } = this.props
    const streamId = record.id
    this.props.onLoadYarnUi(projectIdGeted, streamId, (linkUrl) => {
      window.open(linkUrl)
    })
  }
  /**
   *  start/renew ok
   */
  handleEditStartOk = (e) => {
    const { actionType, streamIdGeted, streamStartFormData, userDefinedTopics, startUdfVals } = this.state
    const { projectIdGeted, locale } = this.props
    const offsetText = locale === 'en' ? 'Offset cannot be empty' : 'Offset 不能为空！'
    this.setState(
      {unValidate: true},
      () => {
        this.streamStartForm.validateFieldsAndScroll((err, values) => {
          if (!err || err.newTopicName) {
            let requestVal = {}
            switch (actionType) {
              case 'start':
                if (!streamStartFormData) {
                  if (!values.udfs) {
                    requestVal = {}
                  } else {
                    if (values.udfs.find(i => i === '-1')) {
                      // 全选
                      const startUdfValsOrigin = startUdfVals.filter(k => k.id !== -1)
                      requestVal = { udfInfo: startUdfValsOrigin.map(p => p.id) }
                    } else {
                      requestVal = { udfInfo: values.udfs.map(q => Number(q)) }
                    }
                  }
                } else {
                  const mergedData = {}
                  const autoRegisteredData = this.formatTopicInfo(streamStartFormData, 'auto', values, offsetText)
                  const userDefinedData = this.formatTopicInfo(userDefinedTopics, 'user', values, offsetText)
                  mergedData.autoRegisteredTopics = autoRegisteredData || []
                  mergedData.userDefinedTopics = userDefinedData || []
                  mergedData.autoRegisteredTopics.forEach(v => {
                    v.name = transformStringWithDot(v.name, false)
                  })
                  mergedData.userDefinedTopics.forEach(v => {
                    v.name = transformStringWithDot(v.name, false)
                  })
                  if (!values.udfs) {
                    requestVal = { topicInfo: mergedData }
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
                break
              case 'renew':
                if (!streamStartFormData) {
                  requestVal = !values.udfs ? {} : { udfInfo: values.udfs.map(q => Number(q)) }
                } else {
                  const mergedData = {}
                  const autoRegisteredData = this.formatTopicInfo(streamStartFormData, 'auto', values, offsetText)
                  const userDefinedData = this.formatTopicInfo(userDefinedTopics, 'user', values, offsetText)

                  // 接口参数：改动的topicInfo
                  mergedData.autoRegisteredTopics = this.diffTopicInfo(streamStartFormData, autoRegisteredData)
                  mergedData.userDefinedTopics = this.diffTopicInfo(this.state.tempUserTopics, userDefinedData)
                  mergedData.autoRegisteredTopics.forEach(v => {
                    v.name = transformStringWithDot(v.name, false)
                  })
                  mergedData.userDefinedTopics.forEach(v => {
                    v.name = transformStringWithDot(v.name, false)
                  })
                  // mergedData.autoRegisteredTopics.length === 0 ? delete mergedData.autoRegisteredTopics : mergedData.userDefinedTopics.length === 0 ? delete mergedData.userDefinedTopics : ''
                  // if (mergedData.autoRegisteredTopics.length === 0 && mergedData.userDefinedTopics.length === 0) {
                  //   requestVal = (!values.udfs) ? {} : { udfInfo: values.udfs.map(q => Number(q)) }
                  // } else {
                  requestVal = (!values.udfs) ? { topicInfo: mergedData } : { udfInfo: values.udfs.map(q => Number(q)), topicInfo: mergedData }
                //   }
                }
                break
            }

            let actionTypeRequest = ''
            let actionTypeMsg = ''
            if (actionType === 'start') {
              actionTypeRequest = 'start'
              actionTypeMsg = locale === 'en' ? 'Start Successfully!' : '启动成功！'
            } else if (actionType === 'renew') {
              actionTypeRequest = 'renew'
              actionTypeMsg = locale === 'en' ? 'Renew Successfully!' : '生效！'
            }

            this.props.onStartOrRenewStream(projectIdGeted, streamIdGeted, requestVal, actionTypeRequest, (result) => {
              this.setState({
                startModalVisible: false,
                streamStartFormData: [],
                userDefinedData: [],
                tempUserTopics: [],
                unValidate: false
              })
              message.success(actionTypeMsg, 3)
            }, (result) => {
              const failText = locale === 'en' ? 'Operation failed:' : '操作失败：'
              message.error(`${failText} ${result}`, 3)
              this.setState({unValidate: false})
            })
          }
        })
      }
    )
  }

  formatTopicInfo (data = [], type = 'auto', values, offsetText) {
    if (data.length === 0) return
    return data.map((i) => {
      const parOffTemp = i.consumedLatestOffset
      const partitionTemp = parOffTemp.split(',')

      const offsetArr = []
      for (let r = 0; r < partitionTemp.length; r++) {
        const offsetArrTemp = values[`${i.name}_${r}_${type}`]
        offsetArrTemp === ''
          ? message.warning(offsetText, 3)
          : offsetArr.push(`${r}:${offsetArrTemp}`)
      }
      const offsetVal = offsetArr.join(',')

      const robj = {
        // id: i.id,
        name: i.name,
        partitionOffsets: offsetVal,
        rate: Number(values[`${i.name}_${i.rate}_rate`])
      }
      return robj
    })
  }

  diffTopicInfo (oldData = [], newData = []) {
    let topicInfoTemp = []
    if (oldData.length === 0 && newData.length > 0) {
      topicInfoTemp = newData.map(v => {
        let obj = {
          name: v.name,
          partitionOffsets: v.partitionOffsets,
          rate: v.rate,
          action: 1
        }
        return obj
      })
    } else if (oldData.length > 0 && newData.length === 0) {
      topicInfoTemp = []
    } else {
      for (let g = 0; g < newData.length; g++) {
        for (let f = 0; f < oldData.length; f++) {
          if (oldData[f].name === newData[g].name) {
            let obj = {
              name: newData[g].name,
              partitionOffsets: newData[g].partitionOffsets,
              rate: newData[g].rate
            }
            if (
              oldData[f].consumedLatestOffset === newData[g].partitionOffsets &&
              oldData[f].rate === newData[g].rate
            ) {
              obj.action = 0
            } else {
              obj.action = 1
            }
            topicInfoTemp.push(obj)
            break
          } else if (f === oldData.length - 1) {
            topicInfoTemp.push({
              name: newData[g].name,
              partitionOffsets: newData[g].partitionOffsets,
              rate: newData[g].rate,
              action: 1
            })
          }
        }
      }
    }
    return topicInfoTemp
  }

  handleEditStartCancel = (e) => {
    this.setState({
      startModalVisible: false
    }, () => {
      this.setState({
        streamStartFormData: []
      })
    })
    this.streamStartForm.resetFields()
  }

  stopStreamBtn = (record, action) => (e) => {
    const { locale } = this.props
    const successText = locale === 'en' ? 'Stop successfully!' : '停止成功！'
    const failText = locale === 'en' ? 'Operation failed:' : '操作失败：'
    this.props.onOperateStream(this.props.projectIdGeted, record.id, 'stop', () => {
      message.success(successText, 3)
    }, (result) => {
      message.error(`${failText} ${result}`, 3)
    })
  }

  deleteStreambtn = (record, action) => (e) => {
    this.props.onDeleteStream(this.props.projectIdGeted, record.id, 'delete', () => {
      message.success(operateLanguageText('success', 'delete'), 3)
    }, (result) => {
      message.error(`${operateLanguageText('fail', 'delete')} ${result}`, 3)
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
    const { roleType } = this.props
    if (roleType === 'admin') {
      this.props.onLoadAdminLogsInfo(projectId, streamId, (result) => {
        this.setState({ logsContent: result })
        this.streamLogRefreshState()
      })
    } else if (roleType === 'user') {
      this.props.onLoadLogsInfo(projectId, streamId, (result) => {
        this.setState({ logsContent: result })
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
    this.setState({ logsModalVisible: false })
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

  getStartFormDataFromSub = (userDefinedTopics) => {
    this.setState({ userDefinedTopics })
  }

  jumpToFlowFilter = (streamId) => () => {
    this.props.jumpStreamToFlowFilter(streamId)
    this.props.onChangeTabs('flow')
  }

  setPriority = (record, action) => (e) => {
    this.setState({
      priorityModalVisible: true,
      streamIdGeted: record.id
    })

    const projectId = record.projectId
    const streamId = record.id
    this.loadFlowData(projectId, streamId)
  }

  loadFlowData = (projectId, streamId) => {
    this.props.onLoadFlowList(projectId, streamId, () => {
      const { flows } = this.props
      this.setState({ currentFlows: flows.slice() })
    })
  }

  submitPriority = () => {
    const { locale } = this.props
    if (!this.state.currentFlows.length) {
      const msg = locale === 'en' ? 'No flow, no priority can be set' : '无flow，不能设置优先级'
      message.error(msg)
      return
    }

    const { projectIdGeted } = this.props
    const { streamIdGeted } = this.state
    const flowList = this.state.currentFlows.map((item, xi) => ({
      id: item.id,
      flowName: item.flowName,
      priorityId: xi + 1
    }))
    this.props.onSubmitFlowListOfPriority(projectIdGeted, streamIdGeted, 'setPriority', { flowPrioritySeq: flowList }, () => {
      const msg = locale === 'en' ? 'success' : '优先级设置成功'
      message.success(msg)
      this.closePriorityDialog()
    })
  }

  closePriorityDialog = () => {
    this.setState({
      priorityModalVisible: false,
      currentFlows: []
    })
  }

  onSortEnd = ({oldIndex, newIndex}) => {
    this.setState(({currentFlows}) => ({
      currentFlows: arrayMove(currentFlows, oldIndex, newIndex)
    }))
  }

  render () {
    const {
      refreshStreamLoading, refreshStreamText, showStreamdetails, logsModalVisible,
      logsContent, refreshLogLoading, refreshLogText, logsProjectId, logsStreamId,
      streamStartFormData, actionType, autoRegisteredTopics, userDefinedTopics,
      startUdfVals, renewUdfVals, currentUdfVal, currentStreams, priorityModalVisible
    } = this.state
    const { className, onShowAddStream, onShowEditStream, streamClassHide, streamStartModalLoading, flowsPriorityConfirmLoading, roleType } = this.props

    let {
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
      sortOrder: sortedInfo.columnKey === 'id' && sortedInfo.order,
      render: (text, record) => (
        <span className="hover-pointer" onClick={this.jumpToFlowFilter(text)}>{text}</span>
      )
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
      title: 'State',
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
      title: 'Stream Type',
      dataIndex: 'streamType',
      key: 'streamType',
      sorter: (a, b) => a.streamType < b.streamType ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'streamType' && sortedInfo.order,
      filters: [
        {text: 'spark', value: 'spark'},
        {text: 'flink', value: 'flink'}
      ],
      filteredValue: filteredInfo.streamType,
      onFilter: (value, record) => record.streamType.includes(value)
    }, {
      title: 'Function Type',
      dataIndex: 'functionType',
      key: 'functionType',
      sorter: (a, b) => a.functionType < b.functionType ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'functionType' && sortedInfo.order,
      filters: [
        {text: 'default', value: 'default'},
        {text: 'hdfslog', value: 'hdfslog'},
        {text: 'routing', value: 'routing'},
        {text: 'hdfscsv', value: 'hdfscsv'}
      ],
      filteredValue: filteredInfo.functionType,
      onFilter: (value, record) => record.functionType.includes(value)
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
        if (roleType === 'admin') {
          streamActionSelect = ''
        } else if (roleType === 'user') {
          const deleteFormat = <FormattedMessage {...messages.streamDelete} />
          const sureDeleteFormat = <FormattedMessage {...messages.streamSureDelete} />
          const startFormat = <FormattedMessage {...messages.streamTableStart} />
          const sureStartFormat = <FormattedMessage {...messages.streamSureStart} />
          const renewFormat = <FormattedMessage {...messages.streamTableRenew} />
          const stopFormat = <FormattedMessage {...messages.streamTableStop} />
          const sureStopFormat = <FormattedMessage {...messages.streamSureStop} />
          const modifyFormat = <FormattedMessage {...messages.streamModify} />
          const streamYarnLink = <FormattedMessage {...messages.streamYarnLink} />
          const streamSetPriority = <FormattedMessage {...messages.streamSetPriority} />

          const { disableActions, hideActions } = record

          let strSetPriority = <Tooltip title={streamSetPriority}>
            <Button icon="bars" shape="circle" type="ghost" onClick={this.setPriority(record, 'setPriority')}></Button>
          </Tooltip>

          let strDelete = disableActions.includes('delete')
            ? (
              <Tooltip title={deleteFormat}>
                <Button icon="delete" shape="circle" type="ghost" disabled></Button>
              </Tooltip>
            )
            : (
              <Popconfirm placement="bottom" title={sureDeleteFormat} okText="Yes" cancelText="No" onConfirm={this.deleteStreambtn(record, 'delete')}>
                <Tooltip title={deleteFormat}>
                  <Button icon="delete" shape="circle" type="ghost"></Button>
                </Tooltip>
              </Popconfirm>
            )

          let strStart = ''
          if (record.streamType === 'spark') {
            strStart = disableActions.includes('start')
              ? <Button icon="caret-right" shape="circle" type="ghost" disabled></Button>
              : <Button icon="caret-right" shape="circle" type="ghost" onClick={this.onShowEditStart(record, 'start')}></Button>
          } else if (record.streamType === 'flink') {
            strStart = record.disableActions.includes('start')
            ? (
              <Tooltip title={startFormat}>
                <Button icon="caret-right" shape="circle" type="ghost" disabled></Button>
              </Tooltip>
            )
            : (
              <Popconfirm placement="bottom" title={sureStartFormat} okText="Yes" cancelText="No" onConfirm={this.startFlink(record.id)}>
                <Tooltip title={startFormat}>
                  <Button icon="caret-right" shape="circle" type="ghost"></Button>
                </Tooltip>
              </Popconfirm>
            )
          }
          let strStop = disableActions.includes('stop')
            ? (
              <Tooltip title={stopFormat}>
                <Button shape="circle" type="ghost" disabled>
                  <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
                </Button>
              </Tooltip>
            )
            : (
              <Popconfirm placement="bottom" title={sureStopFormat} okText="Yes" cancelText="No" onConfirm={this.stopStreamBtn(record, 'stop')}>
                <Tooltip title={stopFormat}>
                  <Button shape="circle" type="ghost">
                    <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
                  </Button>
                </Tooltip>
              </Popconfirm>
            )

          let strRenew = disableActions.includes('renew')
            ? <Button icon="check" shape="circle" type="ghost" disabled></Button>
            : <Button icon="check" shape="circle" type="ghost" onClick={this.updateStream(record, 'renew')}></Button>
          if (hideActions) {
            if (hideActions.includes('delete')) strDelete = ''
            if (hideActions.includes('start')) strStart = ''
            if (hideActions.includes('stop')) strStop = ''
            if (hideActions.includes('renew')) strRenew = ''
          }
          streamActionSelect = (
            <span>
              <Tooltip title={modifyFormat}>
                <Button icon="edit" shape="circle" type="ghost" onClick={onShowEditStream(record)}></Button>
              </Tooltip>
              {strSetPriority}
              <Tooltip title={startFormat}>
                {strStart}
              </Tooltip>
              {strStop}
              <Tooltip title={renewFormat}>
                {strRenew}
              </Tooltip>
              {strDelete}
              <Tooltip title={streamYarnLink}>
                <Button shape="circle" type="ghost" onClick={this.getYarnUi(record)}>
                  <i className="iconfont icon-file_type_yarn"></i>
                </Button>
              </Tooltip>

            </span>
          )
        }

        let streamDetailContent = ''
        if (showStreamdetails && typeof showStreamdetails === 'object') {
          const detailTemp = showStreamdetails.stream

          const topicTemp = showStreamdetails.topicInfo && showStreamdetails.topicInfo.autoRegisteredTopics
          const topicUserTemp = showStreamdetails.topicInfo && showStreamdetails.topicInfo.userDefinedTopics
          let topicFinal = ''
          let topicUserFinal = ''
          if (topicTemp) {
            topicFinal = topicTemp.map(s => (
              <li key={s.name}>
                <strong>Topic Name：</strong>{s.name}
                <strong>；Consumed Latest Offset：</strong>{s.consumedLatestOffset}
                <strong>；Earliest Kafka Offset：</strong>{s.kafkaEarliestOffset}
                <strong>；Latest Kafka Offset：</strong>{s.kafkaLatestOffset}
                <strong>；Rate：</strong>{s.rate}
              </li>
            ))
          }
          if (topicUserTemp) {
            topicUserFinal = topicUserTemp.map(s => (
              <li key={s.name}>
                <strong>Topic Name：</strong>{s.name}
                <strong>；Consumed Latest Offset：</strong>{s.consumedLatestOffset}
                <strong>；Earliest Kafka Offset：</strong>{s.kafkaEarliestOffset}
                <strong>；Latest Kafka Offset：</strong>{s.kafkaLatestOffset}
                <strong>；Rate：</strong>{s.rate}
              </li>
            ))
          }

          const currentudfTemp = showStreamdetails.currentUdf
          const currentUdfFinal = currentudfTemp.length !== 0
            ? currentudfTemp.map(s => (
              <li key={s.name}>
                <strong>Function Name：</strong>{s.functionName}
                <strong>；Full Class Name：</strong>{s.fullClassName}
                <strong>；Jar Name：</strong>{s.jarName}
              </li>
              ))
            : null

          // const usingUdfTemp = showStreamdetails.usingUdf
          // const usingUdfTempFinal = usingUdfTemp.length !== 0
          //   ? usingUdfTemp.map(s => (
          //     <li key={uuid()}>
          //       <strong>Function Name：</strong>{s.functionName}
          //       <strong>；Full Class Name：</strong>strong>{s.fullClassName}
          //       <strong>；Jar Name：</strong>{s.jarName}
          //     </li>
          //   ))
          //   : null

          streamDetailContent = (
            <div className="stream-detail">
              <p className={streamClassHide}><strong>   Project Id：</strong>{detailTemp.projectId}</p>
              <p><strong>   Auto Registered Topics：</strong>{topicFinal}</p>
              <p><strong>   User Defined Topics：</strong>{topicUserFinal}</p>
              <p><strong>   Current Udf：</strong>{currentUdfFinal}</p>
              {/* <p><strong>   Using Udf：</strong>{usingUdfTempFinal}</p> */}
              <p><strong>   Description：</strong>{detailTemp.desc}</p>

              <p><strong>   Launch Config：</strong>{detailTemp.launchConfig}</p>
              <p><strong>   Stream Config：</strong>{detailTemp.streamConfig}</p>
              <p><strong>   Start Config：</strong>{detailTemp.startConfig}</p>

              <p><strong>   Create Time：</strong>{detailTemp.userTimeInfo && detailTemp.userTimeInfo.createTime}</p>
              <p><strong>   Update Time：</strong>{detailTemp.userTimeInfo && detailTemp.userTimeInfo.updateTime}</p>
              <p><strong>   Create By：</strong>{detailTemp.userTimeInfo && detailTemp.userTimeInfo.createBy}</p>
              <p><strong>   Update By：</strong>{detailTemp.userTimeInfo && detailTemp.userTimeInfo.updateBy}</p>
              <p><strong>   Disable Actions：</strong>{showStreamdetails.disableActions}</p>
            </div>
          )
        }

        return (
          <span className="ant-table-action-column">
            <Tooltip title={<FormattedMessage {...messages.streamViewDetailsBtn} />}>
              <Popover
                placement="left"
                content={streamDetailContent}
                title={<h3><FormattedMessage {...messages.streamDetails} /></h3>}
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
      showSizeChanger: true
      // onChange: (current) => {
      //   console.log('Current: ', current)
      // }
    }

    const streamStartForm = startModalVisible
      ? (
        <StreamStartForm
          data={streamStartFormData}
          autoRegisteredTopics={autoRegisteredTopics}
          userDefinedTopics={userDefinedTopics}
          emitStartFormDataFromSub={this.getStartFormDataFromSub}
          // consumedOffsetValue={consumedOffsetValue}
          // kafkaOffsetValue={kafkaOffsetValue}
          // kafkaEarliestOffset={kafkaEarliestOffset}
          streamActionType={actionType}
          startUdfValsOption={startUdfVals}
          renewUdfValsOption={renewUdfVals}
          currentUdfVal={currentUdfVal}
          projectIdGeted={this.props.projectIdGeted}
          streamIdGeted={this.state.streamIdGeted}
          unValidate={this.state.unValidate}
          ref={(f) => { this.streamStartForm = f }}
        />
      )
      : ''

    let StreamAddOrNot = ''
    if (roleType === 'admin') {
      StreamAddOrNot = ''
    } else if (roleType === 'user') {
      StreamAddOrNot = (
        <Button icon="plus" type="primary" onClick={onShowAddStream}>
          <FormattedMessage {...messages.streamCreate} />
        </Button>
      )
    }

    const helmetHide = streamClassHide !== 'hide'
      ? (<Helmet title="Stream" />)
      : (<Helmet title="Workbench" />)

    const modalTitle = actionType === 'start'
      ? <FormattedMessage {...messages.streamSureStart} />
      : <FormattedMessage {...messages.streamSureRenew} />

    const modalOkBtn = actionType === 'start'
      ? <FormattedMessage {...messages.streamTableStart} />
      : <FormattedMessage {...messages.streamTableRenew} />

    const priorityTitle = <FormattedMessage {...messages.streamSetPriority} />

    const SortableItem = SortableElement(({value}) => <div className="sort-item">{value.id}：{value.flowName}</div>)

    const SortableList = SortableContainer(({items}) => (
      <div className="set-priority">
        {items.map((value, index) => (
          <SortableItem key={`item-${index}`} index={index} value={value} />
        ))}
      </div>
    ))

    const priorityContent = priorityModalVisible ? (
      this.props.flowsLoading ? <div className="loading"><Spin /></div> : this.state.currentFlows.length ? <SortableList items={this.state.currentFlows} onSortEnd={this.onSortEnd} /> : (<p className="text-center">暂无</p>)
    ) : ''

    return (
      <div className={`ri-workbench-table ri-common-block ${className}`}>
        {helmetHide}
        <h3 className="ri-common-block-title">
          <Icon type="bars" /> Stream <FormattedMessage {...messages.streamTableList} />
        </h3>
        <div className="ri-common-block-tools">
          {StreamAddOrNot}
          <Button icon="poweroff" type="ghost" className="refresh-button-style" loading={refreshStreamLoading} onClick={this.refreshStream}>{refreshStreamText}</Button>
        </div>
        <Table
          dataSource={currentStreams}
          columns={columns}
          onChange={this.handleStreamChange}
          pagination={pagination}
          className="ri-workbench-table-container"
          bordered>
        </Table>

        <Modal
          title={modalTitle}
          visible={startModalVisible}
          wrapClassName="ant-modal-large stream-start-renew-modal"
          onCancel={this.handleEditStartCancel}
          footer={[
            <Button
              className={`query-offset-btn`}
              key="query"
              size="large"
              onClick={this.queryLastestoffset}
            >
              <FormattedMessage {...messages.streamModalView} /> Latest Offset
            </Button>,
            <Button
              className={`edit-topic-btn`}
              type="default"
              onClick={this.onChangeEditSelect}
              key="renewEdit"
              size="large">
              <FormattedMessage {...messages.streamModalReset} />
            </Button>,
            <Button
              key="cancel"
              size="large"
              onClick={this.handleEditStartCancel}
            >
              <FormattedMessage {...messages.streamModalCancel} />
            </Button>,
            <Button
              key="submit"
              size="large"
              type="primary"
              loading={streamStartModalLoading}
              onClick={this.handleEditStartOk}
            >
              {modalOkBtn}
            </Button>
          ]}
        >
          {streamStartForm}
        </Modal>

        <Modal
          title={priorityTitle}
          visible={priorityModalVisible}
          wrapClassName="ant-modal-large"
          onCancel={this.closePriorityDialog}
          onOk={this.submitPriority}
          footer={[
            <Button key="cancel" size="large" onClick={this.closePriorityDialog}>取消</Button>,
            <Button key="submit" type="primary" size="large" loading={flowsPriorityConfirmLoading} onClick={this.submitPriority}>
              保存
            </Button>
          ]}
        >
          {priorityContent}
        </Modal>

        <Modal
          title="Logs"
          visible={logsModalVisible}
          onCancel={this.handleLogsCancel}
          wrapClassName="ant-modal-xlarge ant-modal-no-footer"
          footer={<span></span>}
        >
          <StreamLogs
            logsContent={logsContent}
            refreshLogLoading={refreshLogLoading}
            refreshLogText={refreshLogText}
            onInitRefreshLogs={this.onInitRefreshLogs}
            logsProjectId={logsProjectId}
            logsStreamId={logsStreamId}
            ref={(f) => { this.streamLogs = f }}
          />
        </Modal>

      </div>
    )
  }
}

Manager.propTypes = {
  className: PropTypes.string,
  projectIdGeted: PropTypes.string,
  streamClassHide: PropTypes.string,
  onLoadUserStreams: PropTypes.func,
  onLoadFlowList: PropTypes.func,
  onSubmitFlowListOfPriority: PropTypes.func,
  onLoadAdminSingleStream: PropTypes.func,
  onLoadAdminAllStreams: PropTypes.func,
  onShowAddStream: PropTypes.func,
  onOperateStream: PropTypes.func,
  onDeleteStream: PropTypes.func,
  onStartOrRenewStream: PropTypes.func,
  onLoadStreamDetail: PropTypes.func,
  onLoadLogsInfo: PropTypes.func,
  onLoadAdminLogsInfo: PropTypes.func,
  onShowEditStream: PropTypes.func,
  onLoadSingleUdf: PropTypes.func,
  onLoadLastestOffset: PropTypes.func,
  streamStartModalLoading: PropTypes.bool,
  roleType: PropTypes.string,
  locale: PropTypes.string,
  onLoadUdfs: PropTypes.func,
  jumpStreamToFlowFilter: PropTypes.func,
  onChangeTabs: PropTypes.func,
  onLoadYarnUi: PropTypes.func,
  flows: PropTypes.array,
  flowsLoading: PropTypes.bool,
  flowsPriorityConfirmLoading: PropTypes.bool
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadUserStreams: (projectId, resolve) => dispatch(loadUserStreams(projectId, resolve)),
    onLoadFlowList: (projectId, streamId, resolve) => dispatch(loadFlowList(projectId, streamId, resolve)),
    onSubmitFlowListOfPriority: (projectId, streamId, action, flows, resolve, reject) => dispatch(submitFlowListOfPriority(projectId, streamId, action, flows, resolve, reject)),
    onLoadAdminAllStreams: (resolve) => dispatch(loadAdminAllStreams(resolve)),
    onLoadAdminSingleStream: (projectId, resolve) => dispatch(loadAdminSingleStream(projectId, resolve)),
    onOperateStream: (projectId, id, action, resolve, reject) => dispatch(operateStream(projectId, id, action, resolve, reject)),
    onDeleteStream: (projectId, id, action, resolve, reject) => dispatch(deleteStream(projectId, id, action, resolve, reject)),
    onStartOrRenewStream: (projectId, id, topicResult, action, resolve, reject) => dispatch(startOrRenewStream(projectId, id, topicResult, action, resolve, reject)),
    onLoadStreamDetail: (projectId, streamId, roleType, resolve) => dispatch(loadStreamDetail(projectId, streamId, roleType, resolve)),
    onLoadLogsInfo: (projectId, streamId, resolve) => dispatch(loadLogsInfo(projectId, streamId, resolve)),
    onLoadAdminLogsInfo: (projectId, streamId, resolve) => dispatch(loadAdminLogsInfo(projectId, streamId, resolve)),
    onLoadSingleUdf: (projectId, roleType, resolve, type) => dispatch(loadSingleUdf(projectId, roleType, resolve, type)),
    onLoadLastestOffset: (projectId, streamId, resolve, type, topics) => dispatch(loadLastestOffset(projectId, streamId, resolve, type, topics)),
    onLoadUdfs: (projectId, streamId, roleType, resolve) => dispatch(loadUdfs(projectId, streamId, roleType, resolve)),
    jumpStreamToFlowFilter: (streamFilterId) => dispatch(jumpStreamToFlowFilter(streamFilterId)),
    onChangeTabs: (key) => dispatch(changeTabs(key)),
    onLoadYarnUi: (projectId, streamId, resolve) => dispatch(loadYarnUi(projectId, streamId, resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  streams: selectStreams(),
  flows: selectFlows(),
  flowsLoading: selectFlowsLoading(),
  flowsPriorityConfirmLoading: selectFlowsPriorityConfirmLoading(),
  streamStartModalLoading: selectStreamStartModalLoading(),
  roleType: selectRoleType(),
  locale: selectLocale()
})

export default connect(mapStateToProps, mapDispatchToProps)(Manager)
