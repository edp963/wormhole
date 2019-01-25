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
import Line from '../../components/Chart/line'
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
import Select from 'antd/lib/select'
import Form from 'antd/lib/form'
const FormItem = Form.Item
const Option = Select.Option
const { RangePicker } = DatePicker

import { selectFlows, selectError, selectFlowStartModalLoading, selectStreamFilterId } from './selectors'
import { selectRoleType } from '../App/selectors'
import { selectLocale } from '../LanguageProvider/selectors'
import {
  loadAdminAllFlows, loadUserAllFlows, loadAdminSingleFlow, operateUserFlow, editLogForm,
  saveForm, checkOutForm, loadSourceLogDetail, loadSourceSinkDetail, loadSinkWriteRrrorDetail,
  loadSourceInput, loadFlowDetail, chuckAwayFlow, loadLastestOffset, loadUdfs, startFlinkFlow, stopFlinkFlow, loadAdminLogsInfo, loadLogsInfo,
  loadDriftList, postDriftList, verifyDrift, postFlowPerformance
} from './action'
import { jumpStreamToFlowFilter } from '../Manager/action'
import { loadSingleUdf } from '../Udf/action'
import FlowStartForm from './FlowStartForm'
import FlowLogs from './FlowLogs'
import { transformStringWithDot } from '../../utils/util'
const performanceRanges = [
  {
    label: 'Last 5 minutes',
    value: 300000
  },
  {
    label: 'Last 15 minutes',
    value: 900000
  },
  {
    label: 'Last 30 minutes',
    value: 1800000
  },
  {
    label: 'Last 1 hour',
    value: 3600000
  },
  {
    label: 'Last 3 hours',
    value: 10800000
  },
  {
    label: 'Last 6 hours',
    value: 21600000
  },
  {
    label: 'Last 12 hours',
    value: 43200000
  },
  {
    label: 'Last 24 hours',
    value: 86400000
  },
  {
    label: 'Last 2 days',
    value: 172800000
  },
  {
    label: 'Last 7 days',
    value: 604800000
  }
]

const performanceRefreshTime = [
  {
    label: 'off',
    value: 'off'
  },
  {
    label: '5s',
    value: 5000
  },
  {
    label: '10s',
    value: 10000
  },
  {
    label: '30s',
    value: 30000
  },
  {
    label: '1m',
    value: 60000
  },
  {
    label: '5m',
    value: 300000
  },
  {
    label: '15m',
    value: 900000
  },
  {
    label: '30m',
    value: 1800000
  },
  {
    label: '1h',
    value: 3600000
  },
  {
    label: '2h',
    value: 7200000
  },
  {
    label: '1d',
    value: 86400000
  }
]
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
      searchTextFunctionType: '',
      filterDropdownVisibleFunctionType: false,
      startTimeText: '',
      endTimeText: '',
      startedStartTimeText: '',
      startedEndTimeText: '',
      filterDropdownVisibleStartedTime: false,
      stoppedStartTimeText: '',
      stoppedEndTimeText: '',
      filterDropdownVisibleStoppedTime: false,
      searchTextFlowName: '',
      filterDropdownVisibleFlowName: false,

      columnNameText: '',
      valueText: '',
      visibleBool: false,
      startTimeTextState: '',
      endTimeTextState: '',
      paginationInfo: null,
      startTextState: '',
      endTextState: '',
      startModalVisible: false,
      flowStartFormData: [],
      autoRegisteredTopics: [],
      userDefinedTopics: [],
      tempUserTopics: [],
      actionType: '',
      startUdfVals: [],
      renewUdfVals: [],
      currentUdfVal: [],
      logsModalVisible: false,
      logsProjectId: 0,
      logsFlowId: 0,
      refreshLogLoading: false,
      refreshLogText: 'Refresh',
      logsContent: '',
      driftModalVisible: false,
      driftList: [],
      driftChosenStreamId: '',
      driftDialogConfirmLoading: false,
      driftVerifyTxt: '',
      driftVerifyStatus: '',
      driftSubmitStatusObj: {},
      performanceModalVisible: false,
      performanceMenuChosen: performanceRanges[3],
      performanceMenuRefreshChosen: performanceRefreshTime[1],
      chosenFlowId: null,
      throughputChartOpt: {},
      recordsChartOpt: {},
      latencyChartOpt: {},
      refreshTimer: null,
      performanceModelTitle: 'performance'
    }
  }

  componentWillMount () {
    this.refreshFlow()
  }

  componentWillReceiveProps (props) {
    if (props.streamFilterId) {
      this.filterStreamId(props.streamFilterId)()
    }
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
      setTimeout(() => {
        this.props.jumpStreamToFlowFilter('')
      }, 20)
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
      projectIdGeted, flowClassHide, onLoadAdminSingleFlow, onLoadAdminAllFlows, onLoadUserAllFlows, roleType
    } = this.props

    if (roleType === 'admin') {
      flowClassHide === 'hide'
        ? onLoadAdminSingleFlow(projectIdGeted, () => { this.flowRefreshState() })
        : onLoadAdminAllFlows(() => { this.flowRefreshState() })
    } else if (roleType === 'user') {
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
    const { locale } = this.props
    if (selectedRowKeys.length > 0) {
      let menuAction = ''
      let menuMsg = ''
      switch (e.key) {
        case 'menuStart':
          menuAction = 'start'
          menuMsg = locale === 'en' ? 'Start' : '启动'
          break
        case 'menuStop':
          menuAction = 'stop'
          menuMsg = locale === 'en' ? 'Stop' : '停止'
          break
        case 'menuDelete':
          menuAction = 'delete'
          menuMsg = locale === 'en' ? 'Delete' : '删除'
          break
        case 'menuRenew':
          menuAction = 'renew'
          menuMsg = locale === 'en' ? 'Renew' : '生效'
          break
      }

      const requestValue = {
        projectId: Number(this.props.projectIdGeted),
        flowIds: this.state.selectedRowKeys.join(','),
        action: menuAction
      }

      this.props.onOperateUserFlow(requestValue, (result) => {
        this.setState({ selectedRowKeys: [] })
        const languagetextSuccess = locale === 'en' ? 'successfully!' : '成功！'

        if (typeof (result) === 'object') {
          const resultFailed = result.filter(i => i.msg.includes('failed'))
          if (resultFailed.length > 0) {
            const resultFailedIdArr = resultFailed.map(i => i.id)
            const resultFailedIdStr = resultFailedIdArr.join('、')

            const languagetextFailFlowId = locale === 'en'
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
        message.error(`${locale === 'en' ? 'Operation failed:' : '操作失败：'}${result}`, 3)
      })
    } else {
      message.warning(`${locale === 'en' ? 'Please select Flow!' : '请选择 Flow！'}`, 3)
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
    const { locale } = this.props
    const requestValue = {
      projectId: record.projectId,
      action: action,
      flowIds: `${record.id}`
    }

    let singleMsg = ''
    switch (action) {
      case 'start':
        singleMsg = locale === 'en' ? 'Start' : '启动'
        break
      case 'stop':
        singleMsg = locale === 'en' ? 'Stop' : '停止'
        break
      case 'delete':
        singleMsg = locale === 'en' ? 'Delete' : '删除'
        break
    }

    this.props.onOperateUserFlow(requestValue, (result) => {
      const languagetextSuccess = locale === 'en' ? 'successfully!' : '成功！'
      if (action === 'delete') {
        message.success(`${singleMsg}${languagetextSuccess}`, 3)
      } else {
        if (result.msg.includes('failed')) {
          const languagetextFail = locale === 'en'
            ? `It fails to ${singleMsg} Flow ID ${result.id}!`
            : `Flow ID ${result.id} ${singleMsg}失败！`

          message.error(languagetextFail, 3)
        } else {
          action === 'renew'
            ? message.success(locale === 'en' ? 'Renew successfully！' : '生效！', 3)
            : message.success(`${singleMsg}${languagetextSuccess}`, 3)
        }
      }
    }, (result) => {
      message.error(`${locale === 'en' ? 'Operation failed:' : '操作失败：'}${result}`, 3)
    })
  }

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
      // this.flowStartForm.setFieldsValue({ udfs: topicsSelectValue })
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
    }, 'flink')

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
          flowStartFormData: autoRegisteredTopics
          // consumedOffsetValue: result.consumedLatestOffset,
          // kafkaOffsetValue: result.kafkaLatestOffset,
          // kafkaEarliestOffset: result.kafkaEarliestOffset
        })
      } else {
        this.setState({
          flowStartFormData: []
        })
      }
    })
  }
  throughputResolve = (flowData) => {
    let series = []
    let seriesData = []
    let xAxisData = []
    let zoomDisabled = true
    if (flowData.throughPutMetrics.length > 0) {
      zoomDisabled = false
      seriesData = flowData.throughPutMetrics.map(y => y.ops)
      xAxisData = flowData.throughPutMetrics.map(v => v.umsTs)
    }
    series.push({
      name: 'throughPutMetrics',
      type: 'line',
      areaStyle: {
        opacity: 0.1
      },
      showSymbol: false,
      data: seriesData
    })

    const obj = {
      grid: {
        top: '15%',
        left: '5%',
        right: '1%'
      },
      title: {
        text: 'throughput',
        left: '1%',
        top: 0
      },
      tooltip: {
        trigger: 'axis',
        // alwaysShowContent: true,
        // hideDelay: 9999,
        axisPointer: {
          type: 'cross',
          label: {
            backgroundColor: '#6a7985'
          }
        }
      },
      color: ['#fbc02d', '#c23531', '#2f4554', '#61a0a8', '#d48265', '#91c7ae', '#749f83', '#ca8622', '#bda29a', '#6e7074', '#546570', '#c4ccd3'],
      dataZoom: [{
        type: 'inside',
        start: 0,
        disabled: zoomDisabled
      }, {
        type: 'slider',
        show: !zoomDisabled
      }],
      xAxis: [
        {
          type: 'category',
          boundaryGap: false,
          data: xAxisData
        }
      ],
      yAxis: [
        {
          type: 'value',
          axisLabel: {
            formatter: '{value} ops'
          }
        }
      ],
      series
    }
    return obj
  }
  recordsResolve = (flowData) => {
    let series = []
    let seriesData = []
    let xAxisData = []
    let zoomDisabled = true
    if (flowData.rddCountMetrics.length > 0) {
      zoomDisabled = false
      seriesData = flowData.rddCountMetrics.map(y => y.count)
      xAxisData = flowData.rddCountMetrics.map(v => v.umsTs)
    }
    series.push({
      name: 'rddCountMetrics',
      type: 'line',
      areaStyle: {
        opacity: 0.1
      },
      showSymbol: false,
      data: seriesData
    })

    const obj = {
      grid: {
        top: '15%',
        left: '5%',
        right: '1%'
      },
      title: {
        text: 'records',
        left: '1%',
        top: 0
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          label: {
            backgroundColor: '#6a7985'
          }
        }
      },
      color: ['#fbc02d', '#c23531', '#2f4554', '#61a0a8', '#d48265', '#91c7ae', '#749f83', '#ca8622', '#bda29a', '#6e7074', '#546570', '#c4ccd3'],
      dataZoom: [{
        type: 'inside',
        start: 0,
        disabled: zoomDisabled
      }, {
        type: 'slider',
        show: !zoomDisabled
      }],
      xAxis: [
        {
          type: 'category',
          boundaryGap: false,
          data: xAxisData
        }
      ],
      yAxis: [
        {
          type: 'value'
        }
      ],
      series
    }
    return obj
  }
  latencyResolve = (flowData, quota) => {
    let series = []
    let xAxisData = []
    let zoomDisabled = true
    let arr = []
    quota.forEach(v => {
      flowData[v].forEach(p => {
        arr.push(Math.abs(p.time))
      })
    })
    let max = Math.max(...arr)
    let unitFlag = max > 3600 ? 'h' : max > 60 ? 'm' : 's'
    quota.forEach(v => {
      let obj = {
        name: v,
        type: 'line',
        areaStyle: {
          opacity: 0.1
        },
        showSymbol: false,
        data: flowData[v].map(p => p.time)
      }
      series.push(obj)
      xAxisData = flowData[quota[0]].map(v => v.umsTs)
      zoomDisabled = false
    })
    const obj = {
      grid: {
        top: '15%',
        left: '5%',
        right: '1%'
      },
      title: {
        text: 'latency',
        left: '1%',
        top: 0
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          label: {
            backgroundColor: '#6a7985'
          }
        }
      },
      color: ['#fbc02d', '#c23531', '#2f4554', '#61a0a8', '#d48265', '#91c7ae', '#749f83', '#ca8622', '#bda29a', '#6e7074', '#546570', '#c4ccd3'],
      legend: {
        data: quota,
        top: '5%'
      },
      dataZoom: [{
        type: 'inside',
        start: 0,
        disabled: zoomDisabled
      }, {
        type: 'slider',
        show: !zoomDisabled
      }],
      xAxis: [
        {
          type: 'category',
          boundaryGap: false,
          data: xAxisData
        }
      ],
      yAxis: [
        {
          type: 'value',
          axisLabel: {
            formatter: val => {
              let value
              if (unitFlag === 'h') {
                value = (val / 3600).toFixed(2)
              } else if (unitFlag === 'm') {
                value = (val / 60).toFixed(2)
              } else if (unitFlag === 's') {
                value = val
              }
              return `${Number(value)} ${unitFlag}`
            }
          }
        }
      ],
      series
    }
    return obj
  }
  performanceResolve = (projectIdGeted, flowId, startTime, endTime) => {
    const { onSearchFlowPerformance } = this.props
    onSearchFlowPerformance(projectIdGeted, flowId, startTime, endTime, (data) => {
      let flowData = data.flowMetrics[0]
      let keyStr = flowData.cols
      let keys = keyStr && keyStr.split(',')
      let quota = []
      keys.forEach(v => {
        if (v.includes('@')) {
          quota = v.split('@')
        }
      })
      const throughputChartOpt = this.throughputResolve(flowData)
      const recordsChartOpt = this.recordsResolve(flowData)
      const latencyChartOpt = this.latencyResolve(flowData, quota)
      this.setState({throughputChartOpt, recordsChartOpt, latencyChartOpt, performanceModelTitle: flowData.flowName || 'performance'})
    })
  }
  onShowPerformance = (record) => (e) => {
    const { projectIdGeted } = this.props
    const flowId = record.id
    const now = new Date().getTime()
    const endTime = now
    const startTime = now - this.state.performanceMenuChosen.value
    this.performanceResolve(projectIdGeted, flowId, startTime, endTime)
    this.setState({
      performanceModalVisible: true,
      chosenFlowId: flowId
    }, () => {
      this.refreshPerformance()
    })
  }
  closePerformanceDialog = () => {
    if (this.state.refreshTimer) {
      clearInterval(this.state.refreshTimer)
      this.setState({refreshTimer: null})
    }
    this.setState({
      performanceModalVisible: false,
      chosenFlowId: null,
      performanceModelTitle: 'performance',
      performanceMenuChosen: performanceRanges[3]
    })
  }
  choosePerformanceRange = ({item, key}) => {
    const { projectIdGeted } = this.props
    const { chosenFlowId } = this.state
    let performanceMenuChosen = {
      label: item.props.children,
      value: key
    }
    this.setState({
      performanceMenuChosen
    }, () => {
      const now = new Date().getTime()
      const endTime = now
      const startTime = now - this.state.performanceMenuChosen.value
      this.performanceResolve(projectIdGeted, chosenFlowId, startTime, endTime)
      this.refreshPerformance()
    })
  }
  refreshPerformance = (performanceMenuRefreshChosen = this.state.performanceMenuRefreshChosen) => {
    const { projectIdGeted } = this.props
    const { chosenFlowId } = this.state
    const now = new Date().getTime()
    const endTime = now
    const startTime = now - this.state.performanceMenuChosen.value
    if (performanceMenuRefreshChosen.value === 'off') {
      if (this.state.refreshTimer) {
        clearInterval(this.state.refreshTimer)
        this.setState({refreshTimer: null})
      }
    } else {
      if (this.state.refreshTimer) {
        clearInterval(this.state.refreshTimer)
      }
      let timer = setInterval(() => {
        this.performanceResolve(projectIdGeted, chosenFlowId, startTime, endTime)
      }, performanceMenuRefreshChosen.value)
      this.setState({refreshTimer: timer})
    }
  }
  choosePerformanceRefreshTime = ({item, key}) => {
    let performanceMenuRefreshChosen = {
      label: item.props.children,
      value: key
    }
    this.setState({
      performanceMenuRefreshChosen
    }, () => {
      this.refreshPerformance(performanceMenuRefreshChosen)
    })
  }
  onShowDrift = (record) => (e) => {
    const { projectIdGeted } = this.props
    const flowId = record.id
    this.setState({
      driftModalVisible: true
    })
    this.props.onLoadDriftList(projectIdGeted, flowId, (payload) => {
      if (Array.isArray(payload)) {
        this.setState({
          driftList: payload,
          streamIdGeted: flowId
        })
      } else if (typeof payload === 'string') {
        message.warn(payload)
      }
    })
  }
  closeDriftDialog = (cb) => {
    this.setState({
      driftModalVisible: false,
      driftList: [],
      driftChosenStreamId: '',
      driftDialogConfirmLoading: false,
      driftVerifyTxt: '',
      driftVerifyStatus: ''
    }, () => {
      if (cb) cb()
    })
  }
  submitDrift = () => {
    const { projectIdGeted, locale } = this.props
    const { streamIdGeted: flowId, driftChosenStreamId: id } = this.state
    if (Object.is(id, '')) {
      message.warn(`${locale === 'en' ? 'Please select others:' : '请选择其他'}`, 3)
      return
    }
    this.setState({driftDialogConfirmLoading: true})
    this.props.onSubmitDrift(projectIdGeted, flowId, id, (payload) => {
      if (typeof payload === 'string') {
        this.closeDriftDialog(() => {
          this.setState({driftSubmitStatusObj: {
            status: 'error',
            content: payload
          }})
        })
      } else {
        this.closeDriftDialog(() => {
          this.setState({driftSubmitStatusObj: {
            status: 'success',
            content: payload.msg
          }})
        })
      }
      this.setState({driftDialogConfirmLoading: false})
    })
  }
  onChangeDriftSel = (valName) => {
    const { projectIdGeted } = this.props
    const flowId = this.state.streamIdGeted
    const selName = this.state.driftList.find(s => s.name === valName)
    const { id } = selName
    this.props.onVerifyDrift(projectIdGeted, flowId, id, (result) => {
      if (result.header && result.header.code === 200) {
        this.setState({driftChosenStreamId: id, driftVerifyTxt: result.payload, driftVerifyStatus: 'success'})
      } else {
        this.setState({driftVerifyTxt: result.payload, driftVerifyStatus: 'error'})
      }
    })
  }
  driftDialogClosed = () => {
    const statusObj = this.state.driftSubmitStatusObj
    if (statusObj.status === 'success') {
      Modal.success({
        title: 'Drift success',
        content: statusObj.content
      })
    } else if (statusObj.status === 'error') {
      Modal.error({
        title: 'Drift error',
        content: statusObj.content
      })
    }
    this.setState({driftSubmitStatusObj: {}})
  }
  stopFlinkFlowBtn = (record) => () => {
    const { locale } = this.props
    const successText = locale === 'en' ? 'Stop successfully!' : '停止成功！'
    const failText = locale === 'en' ? 'Operation failed:' : '操作失败：'
    this.props.onStopFlinkFlow(this.props.projectIdGeted, record.id, () => {
      message.success(successText, 3)
    }, (result) => {
      message.error(`${failText} ${result}`, 3)
    })
  }

  handleEditStartOk = (e) => {
    const { streamIdGeted, flowStartFormData, userDefinedTopics, startUdfVals } = this.state
    const { projectIdGeted, locale } = this.props
    const offsetText = locale === 'en' ? 'Offset cannot be empty' : 'Offset 不能为空！'
    this.setState(
      {unValidate: true},
      () => {
        this.flowStartForm.validateFieldsAndScroll((err, values) => {
          if (!err || err.newTopicName) {
            let requestVal = {}

            if (!flowStartFormData) {
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
              const autoRegisteredData = this.formatTopicInfo(flowStartFormData, 'auto', values, offsetText)
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

            let actionTypeRequest = ''
            let actionTypeMsg = ''
            actionTypeRequest = 'start'
            actionTypeMsg = locale === 'en' ? 'Start Successfully!' : '启动成功！'

            this.props.onStartFlinkFlow(projectIdGeted, streamIdGeted, requestVal, actionTypeRequest, (result) => {
              this.setState({
                startModalVisible: false,
                flowStartFormData: [],
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
        flowStartFormData: []
      })
    })
    this.flowStartForm.resetFields()
  }

  queryLastestoffset = (e) => {
    const { projectIdGeted } = this.props
    const { streamIdGeted, userDefinedTopics, autoRegisteredTopics } = this.state
    userDefinedTopics
    let topics = {}
    topics.userDefinedTopics = userDefinedTopics.map((v, i) => v.name)
    topics.autoRegisteredTopics = autoRegisteredTopics.map((v, i) => v.name)
    this.loadLastestOffsetFunc(projectIdGeted, streamIdGeted, 'post', topics)
  }
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
        tempUserTopics: userDefinedTopics
        // consumedOffsetValue: result.consumedLatestOffset,
        // kafkaOffsetValue: result.kafkaLatestOffset,
        // kafkaEarliestOffset: result.kafkaEarliestOffset
      })
    }, type, topics)
  }
  onChangeEditSelect = () => {
    const { flowStartFormData, userDefinedTopics } = this.state

    for (let i = 0; i < flowStartFormData.length; i++) {
      const partitionAndOffset = flowStartFormData[i].consumedLatestOffset.split(',')

      for (let j = 0; j < partitionAndOffset.length; j++) {
        this.flowStartForm.setFieldsValue({
          [`${flowStartFormData[i].name}_${j}_auto`]: partitionAndOffset[j].substring(partitionAndOffset[j].indexOf(':') + 1),
          [`${flowStartFormData[i].name}_${flowStartFormData[i].rate}_rate`]: flowStartFormData[i].rate
        })
      }
    }
    for (let i = 0; i < userDefinedTopics.length; i++) {
      const partitionAndOffset = userDefinedTopics[i].consumedLatestOffset.split(',')
      for (let j = 0; j < partitionAndOffset.length; j++) {
        this.flowStartForm.setFieldsValue({
          [`${userDefinedTopics[i].name}_${j}_user`]: partitionAndOffset[j].substring(partitionAndOffset[j].indexOf(':') + 1),
          [`${userDefinedTopics[i].name}_${userDefinedTopics[i].rate}_rate`]: userDefinedTopics[i].rate
        })
      }
    }
  }
  getStartFormDataFromSub = (userDefinedTopics) => {
    this.setState({ userDefinedTopics })
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
          roleType: this.props.roleType
        }

        this.props.onLoadFlowDetail(requestValue, (result) => this.setState({ showFlowDetails: result }))
      })
    }
  }
  onShowLogs = (record) => (e) => {
    this.setState({
      logsModalVisible: true,
      logsProjectId: record.projectId,
      logsFlowId: record.id
    })
    this.loadLogsData(record.projectId, record.id)
  }
  onInitRefreshLogs = (projectId, flowId) => {
    this.setState({
      refreshLogLoading: true,
      refreshLogText: 'Refreshing'
    })
    this.loadLogsData(projectId, flowId)
  }
  loadLogsData = (projectId, flowId) => {
    const { roleType } = this.props
    if (roleType === 'admin') {
      this.props.onLoadAdminLogsInfo(projectId, flowId, (result) => {
        this.setState({ logsContent: result })
        this.flowLogRefreshState()
      })
    } else if (roleType === 'user') {
      this.props.onLoadLogsInfo(projectId, flowId, (result) => {
        this.setState({ logsContent: result })
        this.flowLogRefreshState()
      })
    }
  }
  flowLogRefreshState () {
    this.setState({
      refreshLogLoading: false,
      refreshLogText: 'Refresh'
    })
  }

  handleLogsCancel = (e) => {
    this.setState({ logsModalVisible: false })
  }
  filterStreamId = (streamId) => () => {
    const { searchTextStreamId } = this.state
    let value = searchTextStreamId === '' ? streamId : searchTextStreamId
    this.setState({searchTextStreamId: value}, () => {
      this.onSearch('streamId', 'searchTextStreamId', 'filterDropdownVisibleStreamId')()
    })
  }
  render () {
    const { className, onShowAddFlow, onShowEditFlow, flowClassHide, roleType, flowStartModalLoading } = this.props
    const { flowId, refreshFlowText, refreshFlowLoading, currentFlows, modalVisible, timeModalVisible, showFlowDetails, logsModalVisible,
      logsProjectId, logsFlowId, refreshLogLoading, refreshLogText, logsContent, selectedRowKeys,
      driftModalVisible, driftList, driftDialogConfirmLoading, driftVerifyTxt, driftVerifyStatus,
      performanceModalVisible } = this.state
    let { sortedInfo, filteredInfo, startModalVisible, flowStartFormData, autoRegisteredTopics, userDefinedTopics, startUdfVals, renewUdfVals, currentUdfVal, actionType } = this.state
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const columns = [{
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      sorter: (a, b) => a.id - b.id,
      sortOrder: sortedInfo.columnKey === 'id' && sortedInfo.order
    }, {
      title: 'Flow Name',
      dataIndex: 'flowName',
      key: 'flowName',
      // className: 'text-align-center',
      sorter: (a, b) => a.flowName < b.flowName ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'flowName' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Flow Name"
            value={this.state.searchTextFlowName}
            onChange={this.onInputChange('searchTextFlowName')}
            onPressEnter={this.onSearch('flowName', 'searchTextFlowName', 'filterDropdownVisibleFlowName')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('flowName', 'searchTextFlowName', 'filterDropdownVisibleFlowName')}
          >Search
          </Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleFlowName,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleFlowName: visible
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
      }, () => this.searchInput.focus()),
      render: (text, record) => {
        const streamId = record.streamIdOrigin || record.streamId
        return (
          <span className="hover-pointer" onClick={this.filterStreamId(streamId)}>{record.streamId}</span>
        )
      }
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
    },
    {
      title: 'Function Type',
      dataIndex: 'functionType',
      key: 'functionType',
      // className: 'text-align-center',
      sorter: (a, b) => a.streamType < b.streamType ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'functionType' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Function Type"
            value={this.state.searchTextFunctionType}
            onChange={this.onInputChange('searchTextFunctionType')}
            onPressEnter={this.onSearch('functionType', 'searchTextFunctionType', 'filterDropdownVisibleFunctionType')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('functionType', 'searchTextFunctionType', 'filterDropdownVisibleFunctionType')}
          >Search
          </Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleFunctionType,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleFunctionType: visible
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
        if (roleType === 'admin') {
          FlowActionSelect = ''
        } else if (roleType === 'user') {
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
          const driftFormat = <FormattedMessage {...messages.flowTableDrift} />
          const chartFormat = <FormattedMessage {...messages.flowTableChart} />

          let strLog = ''
          if (record.streamType === 'flink') {
            strLog = (
              <Tooltip title="logs">
                <Button shape="circle" type="ghost" onClick={this.onShowLogs(record)}>
                  <i className="iconfont icon-log"></i>
                </Button>
              </Tooltip>
            )
          }
          const strEdit = record.disableActions.includes('modify')
            ? <Button icon="edit" shape="circle" type="ghost" disabled></Button>
            : <Button icon="edit" shape="circle" type="ghost" onClick={onShowEditFlow(record)}></Button>
          let strStart = ''
          let strChart = ''
          if (record.streamType === 'spark' || record.streamTypeOrigin === 'spark') {
            strStart = record.disableActions.includes('start')
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

            strChart = (
              <Tooltip title={chartFormat}>
                <Button icon="bar-chart" shape="circle" type="ghost" onClick={this.onShowPerformance(record)}></Button>
              </Tooltip>
            )
          } else if (record.streamType === 'flink' || record.streamTypeOrigin === 'flink') {
            strStart = record.disableActions.includes('start')
              ? <Button icon="caret-right" shape="circle" type="ghost" disabled></Button>
              : <Tooltip title={startFormat}>
                <Button icon="caret-right" shape="circle" type="ghost" onClick={this.onShowEditStart(record, 'start')}></Button>
              </Tooltip>
          }
          let strDrift = ''
          if ((record.streamType === 'spark' || record.streamTypeOrigin === 'spark') && (record.functionType === 'default' || record.functionTypeOrigin === 'default')) {
            strDrift = record.disableActions.includes('drift')
              ? <Button shape="circle" type="ghost" disabled>
                <i className="iconfont icon-sstransfer"></i>
              </Button>
              : <Tooltip title={driftFormat}>
                <Button shape="circle" type="ghost" onClick={this.onShowDrift(record)}>
                  <i className="iconfont icon-sstransfer"></i>
                </Button>
              </Tooltip>
          }
          let strStop = record.disableActions.includes('stop')
            ? (
              <Tooltip title={stopFormat}>
                <Button shape="circle" type="ghost" disabled>
                  <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
                </Button>
              </Tooltip>
            )
            : (
              <Popconfirm placement="bottom" title={sureStopFormat} okText="Yes" cancelText="No" onConfirm={record.streamType === 'spark' ? this.singleOpreateFlow(record, 'stop') : record.streamType === 'flink' ? this.stopFlinkFlowBtn(record) : null}>
                <Tooltip title={stopFormat}>
                  <Button shape="circle" type="ghost">
                    <i className="iconfont icon-8080pxtubiaokuozhan100"></i>
                  </Button>
                </Tooltip>
              </Popconfirm>
            )

          let strRenew = record.disableActions.includes('renew')
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
          let strDel = record.disableActions.includes('delete')
            ? (
              <Tooltip title={deleteFormat}>
                <Button icon="delete" shape="circle" type="ghost" disabled></Button>
              </Tooltip>
            )
            : (
              <Popconfirm placement="bottom" title={sureDeleteFormat} okText="Yes" cancelText="No" onConfirm={this.singleOpreateFlow(record, 'delete')}>
                <Tooltip title={deleteFormat}>
                  <Button icon="delete" shape="circle" type="ghost"></Button>
                </Tooltip>
              </Popconfirm>
            )

          if (record.hideActions) {
            if (record.hideActions.includes('start')) strStart = ''
            if (record.hideActions.includes('stop')) strStop = ''
            if (record.hideActions.includes('renew')) strRenew = ''
            if (record.hideActions.includes('delete')) strDel = ''
            if (record.hideActions.includes('drift')) strDrift = ''
          }
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
              {strDel}
              {strDrift}
              {strLog}
              {strChart}
            </span>
          )
        }

        let sinkConfigFinal = ''
        let flowDetailContent = ''
        if (!showFlowDetails.sinkConfig) {
          sinkConfigFinal = ''
        } else {
          const sinkJson = JSON.parse(showFlowDetails.sinkConfig)
          sinkConfigFinal = JSON.stringify(sinkJson['sink_specific_config'])
        }
        if (showFlowDetails) {
          const topicTemp = showFlowDetails.topicInfo && showFlowDetails.topicInfo.autoRegisteredTopics
          const topicUserTemp = showFlowDetails.topicInfo && showFlowDetails.topicInfo.userDefinedTopics
          let topicFinal = ''
          let topicUserFinal = ''
          if (topicTemp) {
            topicFinal = topicTemp.map(s => (
              <li key={s.name}>
                <strong>Topic Name：</strong>{s.name}
                <strong>；Consumed Latest Offset：</strong>{s.consumedLatestOffset}
                <strong>；Earliest Kafka Offset：</strong>{s.kafkaEarliestOffset}
                <strong>；Latest Kafka Offset：</strong>{s.kafkaLatestOffset}
                {/* <strong>；Rate：</strong>{s.rate} */}
              </li>
            ))
          }
          if (topicUserTemp) {
            topicUserFinal = topicUserTemp.map(s => (
              <li key={s.name}>
                <strong>Topic Name：</strong>{s.name}
                <strong>；Partition Offsets：</strong>{s.consumedLatestOffset}
                <strong>；Earliest Kafka Offset：</strong>{s.kafkaEarliestOffset}
                <strong>；Latest Kafka Offset：</strong>{s.kafkaLatestOffset}
                {/* <strong>；Rate：</strong>{s.rate} */}
              </li>
            ))
          }
          flowDetailContent = (
            <div className="flow-table-detail">
              <p className={flowClassHide}><strong>   Project Id：</strong>{showFlowDetails.projectId}</p>
              <p><strong>   Auto Registered Topics：</strong>{topicFinal}</p>
              <p><strong>   User Defined Topics：</strong>{topicUserFinal}</p>
              <p><strong>   Protocol：</strong>{showFlowDetails.consumedProtocol}</p>
              <p><strong>   Stream Name：</strong>{showFlowDetails.streamName}</p>
              <p><strong>   Sink Config：</strong>{sinkConfigFinal}</p>
              <p><strong>   Table Keys：</strong>{showFlowDetails.tableKeys}</p>
              <p><strong>   Transformation Config：</strong>{showFlowDetails.tranConfig}</p>
              <p><strong>   Create Time：</strong>{showFlowDetails.createTime}</p>
              <p><strong>   Update Time：</strong>{showFlowDetails.updateTime}</p>
              <p><strong>   Create By：</strong>{showFlowDetails.createBy}</p>
              <p><strong>   Update By：</strong>{showFlowDetails.updateBy}</p>
              <p><strong>   Disable Actions：</strong>{showFlowDetails.disableActions}</p>
              <p><strong>   Message：</strong>{showFlowDetails.msg}</p>
            </div>
          )
        }
        return (
          <span className="ant-table-action-column">
            <Tooltip title={<FormattedMessage {...messages.flowViewDetails} />}>
              <Popover
                placement="left"
                content={flowDetailContent}
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

    let rowSelection = null
    if (roleType === 'admin') {
      rowSelection = null
    } else if (roleType === 'user') {
      rowSelection = {
        selectedRowKeys,
        onChange: this.onSelectChange,
        getCheckboxProps: record => ({
          disabled: record.streamType === 'flink'
        })
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
    if (roleType === 'admin') {
      FlowAddOrNot = ''
    } else if (roleType === 'user') {
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

    const modalTitle = actionType === 'start'
    ? <FormattedMessage {...messages.flowSureStart} />
    : <FormattedMessage {...messages.flowSureRenew} />

    const modalOkBtn = actionType === 'start'
    ? <FormattedMessage {...messages.flowTableStart} />
    : <FormattedMessage {...messages.flowTableRenew} />

    const flowStartForm = startModalVisible
      ? (
        <FlowStartForm
          data={flowStartFormData}
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
          ref={(f) => { this.flowStartForm = f }}
        />
      )
      : ''

    const flowDrift = driftModalVisible
      ? (
        <Select
          dropdownClassName="ri-workbench-select-dropdown"
          onChange={(e) => this.onChangeDriftSel(e)}
          placeholder="Select a Drift Stream"
        >
          {
            driftList.length === 0 ? null : driftList.map(s => (<Option key={s.id} value={`${s.name}`}>{s.name}</Option>))
          }
        </Select>
      ) : ''
    const menuRange = (
      <Menu onClick={this.choosePerformanceRange}>
        {performanceRanges.map(v => (
          <Menu.Item key={v.value}>{v.label}</Menu.Item>
          ))}
      </Menu>
    )
    const menuRefresh = (
      <Menu onClick={this.choosePerformanceRefreshTime}>
        {performanceRefreshTime.map(v => (
          <Menu.Item key={v.value}>{v.label}</Menu.Item>
          ))}
      </Menu>
    )
    const performanceChart = performanceModalVisible
      ? (
        <div>
          <div style={{display: 'flex', justifyContent: 'flex-end', alignItems: 'center'}}>
            <span>Ranges</span>
            <Dropdown overlay={menuRange}>
              <Button style={{ marginLeft: 8 }}>
                {this.state.performanceMenuChosen.label} <Icon type="down" />
              </Button>
            </Dropdown>
            <span style={{marginLeft: '20px'}}>Refreshing</span>
            <Dropdown overlay={menuRefresh}>
              <Button style={{ marginLeft: 8 }}>
                {this.state.performanceMenuRefreshChosen.label} <Icon type="down" />
              </Button>
            </Dropdown>
          </div>
          <Line style={{marginBottom: '5px'}} id="throughput" options={this.state.throughputChartOpt} />
          <Line style={{marginBottom: '5px'}} id="records" options={this.state.recordsChartOpt} />
          <Line style={{height: '250px'}} id="latency" options={this.state.latencyChartOpt} />
        </div>
      ) : ''
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
              <FormattedMessage {...messages.flowModalView} /> Latest Offset
            </Button>,
            <Button
              className={`edit-topic-btn`}
              type="default"
              onClick={this.onChangeEditSelect}
              key="renewEdit"
              size="large">
              <FormattedMessage {...messages.flowModalReset} />
            </Button>,
            <Button
              key="cancel"
              size="large"
              onClick={this.handleEditStartCancel}
            >
              <FormattedMessage {...messages.flowModalCancel} />
            </Button>,
            <Button
              key="submit"
              size="large"
              type="primary"
              loading={flowStartModalLoading}
              onClick={this.handleEditStartOk}
            >
              {modalOkBtn}
            </Button>
          ]}
        >
          {flowStartForm}
        </Modal>
        <Modal
          title={'Drift'}
          visible={driftModalVisible}
          wrapClassName="ant-modal-small stream-start-renew-modal"
          onCancel={this.closeDriftDialog}
          onOk={this.submitDrift}
          confirmLoading={driftDialogConfirmLoading}
          afterClose={this.driftDialogClosed}
        >
          <Form className="ri-workbench-form workbench-flow-form">
            <FormItem
              label="Stream"
              labelCol={{span: 5}}
              wrapperCol={{span: 19}}
              help={driftVerifyTxt}
              validateStatus={driftVerifyStatus}
              hasFeedback
            >
              {flowDrift}
            </FormItem>
          </Form>
        </Modal>
        {/* NOTE: performance */}
        <Modal
          title={this.state.performanceModelTitle}
          visible={performanceModalVisible}
          onCancel={this.closePerformanceDialog}
          footer={null}
          width="90%"
        >
          {performanceChart}
        </Modal>
        <Modal
          title="Logs"
          visible={logsModalVisible}
          onCancel={this.handleLogsCancel}
          wrapClassName="ant-modal-xlarge ant-modal-no-footer"
          footer={<span></span>}
        >
          <FlowLogs
            logsContent={logsContent}
            refreshLogLoading={refreshLogLoading}
            refreshLogText={refreshLogText}
            onInitRefreshLogs={this.onInitRefreshLogs}
            logsProjectId={logsProjectId}
            logsFlowId={logsFlowId}
            ref={(f) => { this.streamLogs = f }}
          />
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
  roleType: PropTypes.string,
  locale: PropTypes.string,
  onLoadLastestOffset: PropTypes.func,
  onLoadSingleUdf: PropTypes.func,
  onLoadUdfs: PropTypes.func,
  flowStartModalLoading: PropTypes.bool,
  onStartFlinkFlow: PropTypes.func,
  onStopFlinkFlow: PropTypes.func,
  onLoadAdminLogsInfo: PropTypes.func,
  onLoadLogsInfo: PropTypes.func,
  onLoadDriftList: PropTypes.func,
  onSubmitDrift: PropTypes.func,
  onVerifyDrift: PropTypes.func,
  onSearchFlowPerformance: PropTypes.func,
  jumpStreamToFlowFilter: PropTypes.func
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
    onLoadSingleUdf: (projectId, roleType, resolve, type) => dispatch(loadSingleUdf(projectId, roleType, resolve, type)),
    onLoadLastestOffset: (projectId, streamId, resolve, type, topics, tabType) => dispatch(loadLastestOffset(projectId, streamId, resolve, type, topics, tabType)),
    onLoadUdfs: (projectId, streamId, roleType, tabType, resolve) => dispatch(loadUdfs(projectId, streamId, roleType, tabType, resolve)),
    onStartFlinkFlow: (projectId, id, topicResult, action, resolve, reject) => dispatch(startFlinkFlow(projectId, id, topicResult, action, resolve, reject)),
    onStopFlinkFlow: (projectId, id, topicResult, action, resolve, reject) => dispatch(stopFlinkFlow(projectId, id, topicResult, action, resolve, reject)),
    onLoadAdminLogsInfo: (projectId, flowId, resolve) => dispatch(loadAdminLogsInfo(projectId, flowId, resolve)),
    onLoadLogsInfo: (projectId, flowId, resolve) => dispatch(loadLogsInfo(projectId, flowId, resolve)),
    onLoadDriftList: (projectId, flowId, resolve) => dispatch(loadDriftList(projectId, flowId, resolve)),
    onSubmitDrift: (projectId, flowId, streamId, resolve) => dispatch(postDriftList(projectId, flowId, streamId, resolve)),
    onVerifyDrift: (projectId, flowId, streamId, resolve) => dispatch(verifyDrift(projectId, flowId, streamId, resolve)),
    onSearchFlowPerformance: (projectId, flowId, startTime, endTime, resolve) => dispatch(postFlowPerformance(projectId, flowId, startTime, endTime, resolve)),
    jumpStreamToFlowFilter: (streamFilterId) => dispatch(jumpStreamToFlowFilter(streamFilterId))
  }
}

const mapStateToProps = createStructuredSelector({
  flows: selectFlows(),
  error: selectError(),
  roleType: selectRoleType(),
  locale: selectLocale(),
  flowStartModalLoading: selectFlowStartModalLoading(),
  streamFilterId: selectStreamFilterId()
})

export default connect(mapStateToProps, mapDispatchToProps)(Flow)

