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
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import Button from 'antd/lib/button'
import Tabs from 'antd/lib/tabs'
import Modal from 'antd/lib/modal'
const TabPane = Tabs.TabPane
import Steps from 'antd/lib/steps'
const Step = Steps.Step
import message from 'antd/lib/message'
import Spin from 'antd/lib/spin'
import Moment from 'moment'

import {
  preProcessSql, formatString, isJSON, operateLanguageSuccessMessage,
  operateLanguageSourceToSink, operateLanguageSinkConfig, operateLanguageSql
} from '../../utils/util'
import {
  generateSourceSinkNamespaceHierarchy, generateTransformSinkNamespaceHierarchy, showSinkConfigMsg
} from './workbenchFunction'
import CodeMirror from 'codemirror'
require('../../../node_modules/codemirror/addon/display/placeholder')
require('../../../node_modules/codemirror/mode/javascript/javascript')
require('../../../node_modules/codemirror/mode/sql/sql')

import Flow from '../Flow'
import Manager from '../Manager'
import Job from '../Job'
import Namespace from '../Namespace'
import User from '../User'
import Udf from '../Udf'
import Resource from '../Resource'

import WorkbenchFlowForm from './WorkbenchFlowForm'
import WorkbenchStreamForm from './WorkbenchStreamForm'
import WorkbenchJobForm from './WorkbenchJobForm'
import FlowEtpStrategyForm from './FlowEtpStrategyForm'
import FlowTransformForm from './FlowTransformForm'
import JobTransformForm from './JobTransformForm'
import StreamConfigForm from './StreamConfigForm'
// import StreamDagModal from './StreamDagModal'
// import FlowDagModal from './FlowDagModal'

import {
  loadUserAllFlows, loadAdminSingleFlow, loadSelectStreamKafkaTopic, loadSourceSinkTypeNamespace,
  loadSinkTypeNamespace, loadTranSinkTypeNamespace, loadSourceToSinkExist, addFlow, editFlow, queryFlow, loadLookupSql
} from '../Flow/action'

import {
  loadUserStreams, loadAdminSingleStream, loadStreamNameValue, loadKafka,
  loadStreamConfigJvm, loadStreamConfigSpark, loadStreamConfigs, addStream, loadStreamDetail, editStream, jumpStreamToFlowFilter
} from '../Manager/action'
import { loadSelectNamespaces, loadUserNamespaces } from '../Namespace/action'
import { loadUserUsers, loadSelectUsers } from '../User/action'
import { loadResources } from '../Resource/action'
import { loadSingleUdf } from '../Udf/action'
import { loadJobSourceToSinkExist, addJob, queryJob, editJob, loadJobBackfillTopic, getSourceNsVersion } from '../Job/action'

import { selectFlows, selectFlowSubmitLoading } from '../Flow/selectors'
import { selectStreams, selectStreamSubmitLoading } from '../Manager/selectors'
import { selectProjectNamespaces, selectNamespaces } from '../Namespace/selectors'
import { selectUsers } from '../User/selectors'
import { selectResources } from '../Resource/selectors'
import { selectRoleType } from '../App/selectors'
import { selectLocale } from '../LanguageProvider/selectors'
import { selectActiveKey } from './selectors'
import { changeTabs } from './action'

export class Workbench extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      projectId: '',
      isWormhole: true,
      flowMode: '',
      streamMode: '',
      transformMode: '',
      jobMode: '',
      formStep: 0,
      tabPanelKey: '',
      flowSubPanelKey: 'spark',
      flowFunctionType: 'default',
      flowPatternCepDataSource: [],

      // all and parts of flow/stream/namespace/user
      userClassHide: 'hide',
      namespaceClassHide: 'hide',
      flowClassHide: 'hide',
      streamClassHide: 'hide',
      jobClassHide: 'hide',
      udfClassHide: 'hide',

      transformModalVisible: false,
      etpStrategyModalVisible: false,
      sinkConfigModalVisible: false,
      flowSpecialConfigModalVisible: false,

      // Flow Modal Transform
      flowFormTranTableSource: [],
      cepPropData: {},
      transformTagClassName: '',
      transformTableClassName: 'hide',
      transformValue: '',
      transConnectClass: 'hide',
      flowTransNsData: [],
      hasPattern: true,
      outputType: 'detail',
      outputFieldList: [
        // {
        //   function_type: 'max',
        //   field_name: '1',
        //   alias_name: '1',
        //   _id: 0
        // }
      ],
      step2SinkNamespace: '',
      step2SourceNamespace: '',

      etpStrategyCheck: false,
      streamConfigModalVisible: false,
      sparkConfigModalVisible: false,
      streamConfigCheck: false,
      sparkConfigCheck: false,

      kafkaValues: [],
      kafkaInstanceId: 0,
      flowKafkaInstanceValue: '',
      flowKafkaTopicValue: '',

      streamConfigValues: {},
      streamQueryValues: {},
      streamSubPanelKey: 'spark',

      // streamDagModalShow: 'hide',
      // flowDagModalShow: 'hide',

      selectStreamKafkaTopicValue: [],
      transformSinkTypeNamespaceData: [],
      transformSinkNamespaceArray: [],

      // request data
      resultFiledsOutput: {},
      dataframeShowOrNot: {},
      etpStrategyRequestValue: {},
      transformTableRequestValue: {},
      pushdownConnectRequestValue: '',

      // add flow confirmation data
      resultFieldsValue: 'all',
      dataframeShowNumValue: 'false',
      etpStrategyConfirmValue: '',
      transformTableConfirmValue: '',
      timeCharacteristic: '',

      etpStrategyResponseValue: '',
      topicEditValues: [],
      sinkConfigMsg: '',

      responseTopicInfo: [],
      fieldSelected: 'hide',
      dataframeShowSelected: 'hide',

      singleFlowResult: {},
      streamDiffType: 'default',
      pipelineStreamId: 0,
      hdfsSinkNsValue: '',
      routingSinkNsValue: '',
      flowSourceResult: [],

      // job
      jobStepSourceNs: '',
      jobStepSinkNs: '',
      jobSparkConfigValues: {},
      jobSourceNsData: [],
      jobSinkConfigMsg: '',
      jobResultFiledsOutput: {},
      jobResultFieldsValue: 'all',

      jobTransModalVisible: false,
      jobFormTranTableSource: [],
      jobTranTagClassName: '',
      jobTranTableClassName: 'hide',
      JobTranValue: '',
      JobTranConnectClass: 'hide',
      jobTransValue: '',
      jobTranTableRequestValue: '',
      jobTranTableConfirmValue: '',
      singleJobResult: {},
      startTsVal: '',
      endTsVal: '',
      jobTransformMode: '',
      jobSpecialConfigModalVisible: false,
      jobSinkConfigModalVisible: false,
      jobTranConfigConfirmValue: '',
      jobDiffType: 'default',
      backfillTopicValueProp: '',

      routingSinkTypeNsData: [],
      routingSourceNsValue: '',
      transConfigConfirmValue: '',
      sinkConfigCopy: '',
      sinkDSCopy: '',
      backfillSinkNsValue: '',
      mapJobType: {
        'default': '1',
        'backfill': '2',
        '1': 'default',
        '2': 'backfill'
      },
      flowSourceNsSys: '',
      jobSourceNsSys: '',
      sourceNsVersionList: [],
      globalLoading: false
    }
  }

  componentWillMount () {
    const projectId = this.props.router.params.projectId
    this.loadData(projectId)
    this.setState({ tabPanelKey: 'flow' })
  }

  componentWillReceiveProps (props) {
    const projectId = props.router.params.projectId
    if (projectId !== this.state.projectId) {
      this.loadData(projectId)
    }
    setTimeout(() => {
      this.setState({tabPanelKey: this.props.activeKey})
    }, 20)
  }
  componentWillUnmount () {
    this.props.onChangeTabs('flow')
  }
  loadData (projectId) {
    this.setState({ projectId: projectId })
  }

  changeTag = (key) => {
    const { projectId } = this.state
    const { onLoadAdminSingleFlow, onLoadUserAllFlows, onLoadAdminSingleStream, onLoadUserStreams, roleType } = this.props
    const { onLoadSelectNamespaces, onLoadUserNamespaces, onLoadSelectUsers, onLoadUserUsers, onLoadResources, onLoadSingleUdf } = this.props
    this.props.onChangeTabs(key)
    this.props.jumpStreamToFlowFilter('')
    switch (key) {
      case 'flow':
        if (roleType === 'admin') {
          onLoadAdminSingleFlow(projectId, () => {})
        } else if (roleType === 'user') {
          onLoadUserAllFlows(projectId, () => {})
        }
        break
      case 'stream':
        if (roleType === 'admin') {
          onLoadAdminSingleStream(projectId, () => {})
        } else if (roleType === 'user') {
          onLoadUserStreams(projectId, () => {})
        }
        break
      case 'namespace':
        if (roleType === 'admin') {
          onLoadSelectNamespaces(projectId, () => {})
        } else if (roleType === 'user') {
          onLoadUserNamespaces(projectId, () => {})
        }
        break
      case 'user':
        if (roleType === 'admin') {
          onLoadSelectUsers(projectId, () => {})
        } else if (roleType === 'user') {
          onLoadUserUsers(projectId, () => {})
        }
        break
      case 'resource':
        if (roleType === 'admin') {
          onLoadResources(projectId, 'admin')
        } else if (roleType === 'user') {
          onLoadResources(projectId, 'user')
        }
        break
      case 'udf':
        if (roleType === 'admin') {
          onLoadSingleUdf(projectId, 'admin', () => {})
        } else if (roleType === 'user') {
          onLoadSingleUdf(projectId, 'user', () => {})
        }
        break
    }
    this.setState({ tabPanelKey: key })
  }

  getDataSystem = (value) => {
    this.setState({
      flowSourceNsSys: value
    })
  }
  initialDefaultCascader = (value, selectedOptions) => {
    if (selectedOptions && selectedOptions.length > 0) {
      this.setState({flowSourceNsSys: selectedOptions[selectedOptions.length - 1].nsSys})
    }
  }

  initialHdfslogCascader = (value, selectedOptions) => {
    this.setState({
      hdfsSinkNsValue: value.join('.'),
      flowSourceNsSys: selectedOptions[selectedOptions.length - 1].nsSys
    })
  }

  initialBackfillCascader = (value, selectedOptions) => {
    const { projectId } = this.state
    if (Object.prototype.toString.call(selectedOptions) !== '[object Array]') return
    if (selectedOptions.length === 0) return
    if (selectedOptions[selectedOptions.length - 1]) {
      let id = selectedOptions[selectedOptions.length - 1].id
      // if (this.state.namespaceId !== id) {
        // setTimeout(() => {
      this.setState({namespaceId: id})
      this.props.onLoadJobBackfillTopic(projectId, id, (result) => {
        this.setState({backfillTopicValueProp: result})
      })
        // }, 20)
      // }
    }
    const jobSourceNsSys = selectedOptions[selectedOptions.length - 1].nsSys
    this.setState({jobSourceNsSys})
    if (this.state.jobDiffType === 'backfill') {
      this.setState({ backfillSinkNsValue: value.join('.'), jobSourceNsSys })
    }
    this.setState({singleJobResult: {
      sourceNs: `${jobSourceNsSys}.${value.join('.')}`
    }})
  }
  initialSourceNsVersion = () => {
    const { singleJobResult, sourceNsVersionList } = this.state
    if (sourceNsVersionList.length > 0) return
    if (!singleJobResult.sourceNs) {
      message.warn('请先选择namespace')
      return
    }
    const namespace = singleJobResult.sourceNs
    const { projectId } = this.state
    this.setState({globalLoading: true})
    this.props.onLoadSourceNsVersion(projectId, namespace, result => {
      this.setState({globalLoading: false})
      if (!result) {
        message.warn('hdfs没有该namespace的数据')
        return
      }
      const arr = result.split(',')
      arr.sort((a, b) => a - b)
      // this.workbenchJobForm.setFieldsValue({sourceNamespaceVersion: arr[arr.length - 1]})
      this.setState({sourceNsVersionList: arr})
    })
  }
  initialRoutingCascader = (value, selectedOptions) => {
    const { projectId, pipelineStreamId, routingSourceNsValue } = this.state

    this.setState({
      routingSourceNsValue: value.join('.'),
      flowSourceNsSys: this.state.flowSourceNsSys
    }, () => {
      // 调显示 routing sink namespace 下拉框数据的接口
      this.props.onLoadSinkTypeNamespace(projectId, pipelineStreamId, 'kafka', 'sinkType', (result) => {
        const exceptValue = result.filter(s => [s.nsInstance, s.nsDatabase, s.nsTable].join('.') !== routingSourceNsValue)

        this.setState({
          routingSinkTypeNsData: generateSourceSinkNamespaceHierarchy('kafka', exceptValue)
        })
      })
    })
  }

  initialRoutingSinkCascader = (value) => this.setState({ routingSinkNsValue: value.join('.') })

  onInitSinkTypeNamespace = (value) => {
    const { sinkDSCopy, sinkConfigCopy } = this.state

    this.setState({
      sinkConfigCopy: sinkDSCopy === value ? sinkConfigCopy : '',
      sinkConfigMsg: showSinkConfigMsg(value)
    }, () => {
      this.workbenchFlowForm.setFieldsValue({
        'sinkConfig': sinkConfigCopy
      })
    })
  }

  // 新增Job时，获取 sink namespace 下拉框数据
  onInitJobSinkNs = (value) => {
    this.setState({
      jobSinkConfigMsg: showSinkConfigMsg(value)
    })
  }

  // 新增Flow时，获取 default transformation sink namespace 下拉框
  onInitTransformSinkTypeNamespace = (projectId, value, type) => {
    const { pipelineStreamId } = this.state

    this.setState({ transformSinkTypeNamespaceData: [] })

    if (pipelineStreamId !== 0) {
      this.props.onLoadTranSinkTypeNamespace(projectId, pipelineStreamId, value, type, (result) => {
        this.setState({
          transformSinkNamespaceArray: result,
          transformSinkTypeNamespaceData: generateTransformSinkNamespaceHierarchy(value, result)
        })
      })
    }
  }

  initResultFieldClass = (value) => this.setState({ fieldSelected: value === 'all' ? 'hide' : '' })
  initDataShowClass = (value) => this.setState({ dataframeShowSelected: value === 'true' ? '' : 'hide' })
  /**
   * 新建 Flow
   */
  showAddFlowWorkbench = () => {
    this.workbenchFlowForm.resetFields()
    this.setState({
      flowMode: 'add',
      formStep: 0,
      flowFormTranTableSource: [],
      transformTagClassName: '',
      transformTableClassName: 'hide',
      transConnectClass: 'hide',
      fieldSelected: 'hide',
      etpStrategyCheck: false,
      dataframeShowSelected: 'hide',
      resultFieldsValue: 'all',
      etpStrategyConfirmValue: '',
      etpStrategyRequestValue: {},
      cepPropData: {},
      outputType: 'detail',
      transformMode: '',
      flowSubPanelKey: 'spark'
    }, () => {
      this.workbenchFlowForm.setFieldsValue({
        resultFields: 'all',
        dataframeShow: 'false',
        dataframeShowNum: 10
      })
      this.onInitStreamTypeSelect(this.state.flowFunctionType)
    })
  }

  onInitStreamTypeSelect = (val) => {
    const { projectId, flowSubPanelKey } = this.state
    const { locale } = this.props
    if (flowSubPanelKey === 'flink') {
      val = 'default'
    }
    this.setState({ streamDiffType: val })

    // 显示 Stream 信息
    this.props.onLoadSelectStreamKafkaTopic(projectId, flowSubPanelKey, val, (result) => {
      const resultFinal = result.map(s => {
        const responseResult = {
          id: s.id,
          maxParallelism: s.maxParallelism,
          topicInfo: s.topicInfo,
          instance: s.kafkaInstance,
          name: s.name
          // disableActions: s.disableActions,
          // connUrl: s.kafkaInfo.connUrl,
          // projectName: s.projectName,
          // currentUdf: s.currentUdf,
          // usingUdf: s.usingUdf
        }
        responseResult.key = responseResult.id
        return responseResult
      })

      this.setState({
        selectStreamKafkaTopicValue: resultFinal,
        hdfsSinkNsValue: ''
      })
      if (result.length === 0) {
        message.warning(locale === 'en' ? 'Please create a Stream with corresponding type first!' : '请先新建相应类型的 Stream！', 3)
        this.setState({
          pipelineStreamId: 0,
          flowKafkaInstanceValue: '',
          flowKafkaTopicValue: ''
        })
      } else {
        const { topicInfo, id, instance, name } = resultFinal[0]

        this.setState({
          pipelineStreamId: id,
          flowKafkaInstanceValue: instance,
          flowKafkaTopicValue: topicInfo && topicInfo.length > 0 ? topicInfo.join(',') : ''
        })
        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: id,
          streamName: name
        })
      }
    })
    this.workbenchFlowForm.setFieldsValue({
      sourceDataSystem: '',
      hdfsNamespace: undefined
    })
  }

  onInitStreamNameSelect = (valName) => {
    const { streamDiffType, selectStreamKafkaTopicValue } = this.state

    const selName = selectStreamKafkaTopicValue.find(s => s.name === valName)
    const { topicInfo, id, instance } = selName
    this.setState({
      pipelineStreamId: Number(id),
      flowKafkaInstanceValue: instance,
      flowKafkaTopicValue: topicInfo && topicInfo.length > 0 ? topicInfo.join(',') : ''
    })

    switch (streamDiffType) {
      case 'default':
        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: Number(id),
          sourceDataSystem: '',
          sinkDataSystem: '',
          sourceNamespace: undefined,
          sinkNamespace: undefined
        })
        break
      case 'hdfslog':
      case 'hdfscsv':
        this.setState({
          hdfsSinkNsValue: ''
        })
        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: Number(id),
          sourceDataSystem: '',
          hdfsNamespace: undefined
        })
        break
      case 'routing':
        this.setState({
          routingSourceNsValue: '',
          routingSinkNsValue: ''
        })
        this.workbenchFlowForm.setFieldsValue({ flowStreamId: Number(id) })
        break
    }
  }

  onInitJobTypeSelect = (val) => {
    this.setState({ jobDiffType: val })
  }
  showCopyFlowWorkbench = (flow) => {
    this.setState({
      flowMode: 'copy',
      fieldSelected: 'hide'
    })

    new Promise((resolve) => {
      resolve(flow)
      this.workbenchFlowForm.resetFields()
      this.props.onLoadSelectStreamKafkaTopic(this.state.projectId, flow.streamType, flow.functionTYpe, (result) => {
        const resultFinal = result.map(s => {
          const responseResult = {
            id: s.id,
            maxParallelism: s.maxParallelism,
            topicInfo: s.topicInfo,
            instance: s.kafkaInstance,
            name: s.name
            // connUrl: s.kafkaInfo.connUrl,
            // projectName: s.projectName,
            // currentUdf: s.currentUdf,
            // usingUdf: s.usingUdf
          }
          responseResult.key = responseResult.id
          return responseResult
        })
        this.setState({
          selectStreamKafkaTopicValue: resultFinal
        })
      })
    })
      .then((flow) => {
        this.queryFlowInfo(flow)
      })
  }

  showEditJobWorkbench = (job) => () => {
    this.workbenchJobForm.resetFields()
    this.setState({sourceNsVersionList: []})
    const { mapJobType } = this.state
    this.setState({ jobMode: 'edit', jobDiffType: job.jobType })

    new Promise((resolve) => {
      const requestData = {
        projectId: job.projectId,
        jobId: job.id
      }
      this.props.onQueryJob(requestData, (result) => {
        const resultFinal = result.job
        resolve(resultFinal)
        const sourceConfigTemp = resultFinal.sourceConfig
        this.workbenchJobForm.setFieldsValue({
          protocol: JSON.parse(sourceConfigTemp).protocol,
          jobName: resultFinal.name,
          type: mapJobType[resultFinal.jobType],
          eventStartTs: resultFinal.eventTsStart === '' ? null : Moment(formatString(resultFinal.eventTsStart)),
          eventEndTs: resultFinal.eventTsEnd === '' ? null : Moment(formatString(resultFinal.eventTsEnd)),
          sinkProtocol: resultFinal.sinkConfig.indexOf('snapshot') > -1,
          tableKeys: resultFinal.tableKeys
        })
        const { sparkConfig, startConfig, id, name, projectId, sourceNs, sinkNs, jobType,
          sparkAppid, logPath, startedTime, stoppedTime, status, userTimeInfo } = resultFinal

        const jobResultSinkNsArr = resultFinal.sinkNs.split('.')
        const jobResultSinkNsFinal = [jobResultSinkNsArr[1], jobResultSinkNsArr[2], jobResultSinkNsArr[3]].join('.')
        this.setState({
          formStep: 0,
          backfillSinkNsValue: jobResultSinkNsFinal,
          backfillTopicValueProp: result.topic,
          jobSparkConfigValues: {
            sparkConfig: sparkConfig,
            startConfig: startConfig
          },
          singleJobResult: {
            id: id,
            name: name,
            projectId: projectId,
            sourceNs: sourceNs,
            sinkNs: sinkNs,
            jobType: jobType,
            sparkAppid: sparkAppid,
            logPath: logPath,
            startedTime: startedTime,
            stoppedTime: stoppedTime,
            status: status,
            userTimeInfo
          }
        })
      })
    })
      .then((resultFinal) => {
        if (resultFinal.tranConfig !== '') {
          if (resultFinal.tranConfig.includes('action')) {
            const tranConfigVal = JSON.parse(JSON.parse(JSON.stringify(resultFinal.tranConfig)))

            const tranActionArr = tranConfigVal.action.split(';')
            tranActionArr.splice(tranActionArr.length - 1, 1)

            this.state.jobFormTranTableSource = tranActionArr.map((i, index) => {
              const tranTableSourceTemp = {}
              let tranConfigInfoTemp = ''
              let tranTypeTepm = ''

              if (i.includes('spark_sql')) {
                const sparkAfterPart = i.substring(i.indexOf('=') + 1)
                const sparkAfterPartTepm = sparkAfterPart.replace(/(^\s*)|(\s*$)/g, '')

                tranConfigInfoTemp = `${sparkAfterPartTepm};`
                tranTypeTepm = 'sparkSql'
              }

              if (i.includes('custom_class')) {
                const sparkAfterPart = i.substring(i.indexOf('=') + 1)
                const sparkAfterPartTepm = sparkAfterPart.replace(/(^\s*)|(\s*$)/g, '')

                tranConfigInfoTemp = sparkAfterPartTepm
                tranTypeTepm = 'transformClassName'
              }

              tranTableSourceTemp.order = index + 1
              tranTableSourceTemp.transformConfigInfo = tranConfigInfoTemp
              tranTableSourceTemp.transformConfigInfoRequest = `${i};`
              tranTableSourceTemp.transformType = tranTypeTepm
              return tranTableSourceTemp
            })

            this.setState({
              jobTranTagClassName: 'hide',
              jobTranTableClassName: ''
            })
          } else {
            this.editJobFinal()
          }
        } else {
          this.editJobFinal()
        }

        let sinkConfigShow = ''
        let maxRecordShow = 5000
        let sinkProtocolShow = ''
        let resultFieldsVal = ''
        if (resultFinal.sinkConfig !== '') {
          const sinkConfigVal = JSON.parse(resultFinal.sinkConfig)
          sinkConfigShow = sinkConfigVal.sink_specific_config ? sinkConfigVal.sink_specific_config : ''
          maxRecordShow = sinkConfigVal.maxRecordPerPartitionProcessed ? sinkConfigVal.maxRecordPerPartitionProcessed : 5000
          sinkProtocolShow = sinkConfigVal.sink_protocol && sinkConfigVal.sink_protocol.indexOf('snapshot') > -1

          if (!resultFinal.sinkConfig.includes('output')) {
            resultFieldsVal = 'all'
            this.setState({
              fieldSelected: 'hide'
            }, () => {
              this.workbenchJobForm.setFieldsValue({
                resultFieldsSelected: '',
                resultFields: 'all'
              })
            })
          } else {
            resultFieldsVal = 'selected'
            this.setState({
              fieldSelected: ''
            }, () => {
              this.workbenchJobForm.setFieldsValue({
                resultFieldsSelected: sinkConfigVal.sink_output,
                resultFields: 'selected'
              })
            })
          }
        } else {
          sinkConfigShow = ''
          maxRecordShow = 5000
          resultFieldsVal = 'all'
          this.setState({
            fieldSelected: 'hide'
          }, () => {
            this.workbenchJobForm.setFieldsValue({
              resultFieldsSelected: '',
              resultFields: 'all'
            })
          })
        }

        const sourceNsArr = resultFinal.sourceNs.split('.')
        const sinkNsArr = resultFinal.sinkNs.split('.')
        const jobSpecialConfigVal = resultFinal.tranConfig !== ''
          ? JSON.stringify(JSON.parse(resultFinal.tranConfig).swifts_specific_config)
          : ''
        this.setState({jobSourceNsSys: sourceNsArr[0]})
        this.workbenchJobForm.setFieldsValue({
          sourceDataSystem: sourceNsArr[0],
          sourceNamespace: [sourceNsArr[1], sourceNsArr[2], sourceNsArr[3]],
          sourceNamespaceVersion: sourceNsArr[4],
          sinkDataSystem: sinkNsArr[0],
          sinkNamespace: [sinkNsArr[1], sinkNsArr[2], sinkNsArr[3]],

          sinkConfig: sinkConfigShow,
          sinkProtocol: sinkProtocolShow,
          maxRecordPerPartitionProcessed: maxRecordShow,
          resultFields: resultFieldsVal,
          jobSpecialConfig: jobSpecialConfigVal
        })
      })
  }

  editJobFinal () {
    this.setState({
      jobTranTagClassName: '',
      jobTranTableClassName: 'hide'
    })
  }

  showEditFlowWorkbench = (flow) => () => {
    this.setState({
      flowMode: 'edit',
      fieldSelected: 'hide',
      flowSubPanelKey: flow.streamType
    }, () => {
      this.onInitStreamTypeSelect(this.state.flowFunctionType)
    })

    new Promise((resolve) => {
      resolve(flow)
      this.workbenchFlowForm.resetFields()
    })
      .then((flow) => {
        this.queryFlowInfo(flow)
      })
  }

  // Flow 调单条查询的接口，回显数据
  queryFlowInfo = (flow) => {
    this.setState({
      streamDiffType: flow.functionType,
      transformTableConfirmValue: '',
      transConfigConfirmValue: '',
      flowFormTranTableSource: [],
      etpStrategyConfirmValue: '',
      timeCharacteristic: ''
    }, () => {
      const { streamDiffType } = this.state
      if (flow.streamType === 'spark' || flow.streamTypeOrigin === 'spark') {
        switch (streamDiffType) {
          case 'default':
            this.queryFlowDefault(flow)
            break
          case 'hdfslog':
          case 'hdfscsv':
            this.queryFlowHdfslog(flow)
            break
          case 'routing':
            this.queryFlowRouting(flow)
            break
        }
      } else if (flow.streamType === 'flink' || flow.streamTypeOrigin === 'flink') {
        this.queryFlowDefault(flow)
      }
    })
  }

  queryFlowDefault (flow) {
    new Promise((resolve) => {
      const requestData = {
        projectId: flow.projectId,
        streamId: typeof (flow.streamId) === 'object' ? flow.streamIdOrigin : flow.streamId,
        id: flow.id
      }
      this.props.onQueryFlow(requestData, (result) => {
        resolve(result)
        const { tranConfig, streamId, streamName, streamType, consumedProtocol, flowName, tableKeys, config } = result
        let parallelism, checkpoint, isCheckpoint
        let tranConfigParse = {}
        try {
          const configParse = JSON.parse(config)
          parallelism = configParse.parallelism
          checkpoint = configParse.checkpoint
          isCheckpoint = checkpoint.enable
        } catch (error) {
          console.error('TCL: Workbench -> queryFlowDefault -> error', error)
        }
        try {
          tranConfigParse = JSON.parse(tranConfig)
        } catch (error) {
          console.error('warn: parse error')
        }
        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: streamId,
          streamName: streamName,
          streamType: streamType,
          protocol: consumedProtocol.split(','),
          flowName,
          tableKeys,
          parallelism,
          checkpoint: isCheckpoint,
          time_characteristic: tranConfigParse.time_characteristic || ''
        })

        const { id, projectId, sourceNs, sinkNs, status, active,
          createTime, createBy, updateTime, updateBy, startedTime, stoppedTime } = result
        this.setState({
          formStep: 0,
          pipelineStreamId: streamId,
          // flowKafkaInstanceValue: kafka,
          // flowKafkaTopicValue: topics,
          singleFlowResult: {
            id: id,
            projectId: projectId,
            streamId: streamId,
            sourceNs: sourceNs,
            sinkNs: sinkNs,
            status: status,
            active: active,
            createTime: createTime,
            createBy: createBy,
            updateTime: updateTime,
            updateBy: updateBy,
            startedTime: startedTime,
            stoppedTime: stoppedTime
          }
        })
      })
    })
      .then((result) => {
        const sourceNsArr = result.sourceNs.split('.')
        const sinkNsArr = result.sinkNs.split('.')

        let dataframeShowVal = ''
        if (result.tranConfig !== '') {
          if (result.tranConfig.includes('action')) {
            const temp = JSON.parse(JSON.stringify(result.tranConfig))
            const tt = temp.replace(/\n/g, ' ')
            const tranConfigVal = JSON.parse(tt)

            let validityTemp = tranConfigVal.validity
            const { tranConfig } = result
            if (tranConfig.includes('validity') && tranConfig.includes('check_columns') &&
              tranConfig.includes('check_rule') && tranConfig.includes('rule_mode') &&
              tranConfig.includes('rule_params') && tranConfig.includes('against_action')) {
              const { check_columns, check_rule, rule_mode, rule_params, against_action } = validityTemp
              const requestTempJson = {
                check_columns: check_columns,
                check_rule: check_rule,
                rule_mode: rule_mode,
                rule_params: rule_params,
                against_action: against_action
              }

              this.setState({
                etpStrategyCheck: true,
                etpStrategyResponseValue: validityTemp,
                etpStrategyRequestValue: {'validity': requestTempJson},
                etpStrategyConfirmValue: JSON.stringify(requestTempJson)
              })
            } else {
              this.setState({
                etpStrategyCheck: false,
                etpStrategyRequestValue: {},
                etpStrategyResponseValue: ''
              })
            }
            if (result.streamType === 'spark') {
              if (result.tranConfig.includes('dataframe_show_num')) {
                dataframeShowVal = 'true'
                this.setState({
                  dataframeShowSelected: ''
                }, () => {
                  this.workbenchFlowForm.setFieldsValue({ dataframeShowNum: tranConfigVal.dataframe_show_num })
                })
              } else {
                dataframeShowVal = 'false'
                this.setState({ dataframeShowSelected: 'hide' })
                this.workbenchFlowForm.setFieldsValue({
                  dataframeShow: 'false',
                  dataframeShowNum: 10
                })
              }
            }
            let tranActionArr = []
            if (result.streamType === 'spark') {
              tranActionArr = tranConfigVal.action.split(';')
              tranActionArr.splice(tranActionArr.length - 1, 1)
            } else if (result.streamType === 'flink') {
              tranActionArr = tranConfigVal.action.split(';')
              if (!tranActionArr[tranActionArr.length - 1]) {
                tranActionArr.splice(tranActionArr.length - 1, 1)
              }
            }

            this.state.flowFormTranTableSource = tranActionArr.map((i, index) => {
              const tranTableSourceTemp = {}
              let tranConfigInfoTemp = ''
              let tranConfigInfoSqlTemp = ''
              let tranTypeTepm = ''
              let pushdownConTepm = {}

              if (i.includes('pushdown_sql')) {
                const iTmp = i.includes('left join') ? i.replace('left join', 'leftJoin') : i
                const lookupBeforePart = iTmp.substring(0, i.indexOf('=') - 1)
                const lookupAfterPart = iTmp.substring(i.indexOf('=') + 1)
                const lookupBeforePartTemp = (lookupBeforePart.replace(/(^\s*)|(\s*$)/g, '')).split(' ')
                const lookupAfterPartTepmTemp = lookupAfterPart.replace(/(^\s*)|(\s*$)/g, '') // 去字符串前后的空白；sql语句回显
                const lookupAfterPartTepm = preProcessSql(lookupAfterPartTepmTemp)

                const tranConfigInfoTempTemp = [lookupBeforePartTemp[1], lookupBeforePartTemp[3], lookupAfterPartTepm].join('.')
                tranConfigInfoTemp = `${tranConfigInfoTempTemp};`
                tranConfigInfoSqlTemp = `${lookupAfterPartTepm};`
                tranTypeTepm = 'lookupSql'

                const tmpObj = tranConfigVal.pushdown_connection.find(g => g.name_space === lookupBeforePartTemp[3])
                const pushdownConTepmJson = {
                  name_space: tmpObj.name_space,
                  jdbc_url: tmpObj.jdbc_url,
                  username: tmpObj.username,
                  password: tmpObj.password
                }
                pushdownConTepm = pushdownConTepmJson
              } else if (/^parquet_sql/.test(i)) {
                let imp = ''
                if (i.includes('left join')) {
                  imp = i.replace('left join', 'leftJoin')
                } else if (i.includes('inner join')) {
                  imp = i.replace('inner join', 'innerJoin')
                } else {
                  imp = i
                }

                const streamJoinBeforePart = imp.substring(0, i.indexOf('=') - 1)
                const streamJoinAfterPart = imp.substring(i.indexOf('=') + 1)
                const streamJoinBeforePartTemp = streamJoinBeforePart.replace(/(^\s*)|(\s*$)/g, '').split(' ')
                const streamJoinAfterPartTepmTemp = streamJoinAfterPart.replace(/(^\s*)|(\s*$)/g, '')
                const streamJoinAfterPartTepm = preProcessSql(streamJoinAfterPartTepmTemp)

                const iTemp3Temp = streamJoinBeforePartTemp[3].substring(streamJoinBeforePartTemp[3].indexOf('(') + 1)
                const iTemp3Val = iTemp3Temp.substring(0, iTemp3Temp.indexOf(')'))
                const tranConfigInfoTempTemp = [streamJoinBeforePartTemp[1], iTemp3Val, streamJoinAfterPartTepm].join('.')
                tranConfigInfoTemp = `${tranConfigInfoTempTemp};`
                tranConfigInfoSqlTemp = `${streamJoinAfterPartTepm};`
                tranTypeTepm = 'streamJoinSql'
                pushdownConTepm = {}
              } else if (/^spark_sql/.test(i)) {
                const sparkAfterPart = i.substring(i.indexOf('=') + 1)
                const sparkAfterPartTepmTemp = sparkAfterPart.replace(/(^\s*)|(\s*$)/g, '')
                const sparkAfterPartTepm = preProcessSql(sparkAfterPartTepmTemp)

                tranConfigInfoTemp = `${sparkAfterPartTepm};`
                tranConfigInfoSqlTemp = `${sparkAfterPartTepm};`
                tranTypeTepm = 'sparkSql'
                pushdownConTepm = {}
              } else if (/^flink_sql/.test(i)) {
                const sparkAfterPart = i.substring(i.indexOf('=') + 1)
                const sparkAfterPartTepmTemp = sparkAfterPart.replace(/(^\s*)|(\s*$)/g, '')
                const sparkAfterPartTepm = preProcessSql(sparkAfterPartTepmTemp)

                tranConfigInfoTemp = `${sparkAfterPartTepm};`
                tranConfigInfoSqlTemp = `${sparkAfterPartTepm};`
                tranTypeTepm = 'flinkSql'
                pushdownConTepm = {}
              } else if (/^custom_class/.test(i)) {
                const classAfterPart = i.substring(i.indexOf('=') + 1)
                const classAfterPartTepmTemp = classAfterPart.replace(/(^\s*)|(\s*$)/g, '')
                const classAfterPartTepm = preProcessSql(classAfterPartTepmTemp)

                tranConfigInfoTemp = classAfterPartTepm
                tranConfigInfoSqlTemp = classAfterPartTepm
                tranTypeTepm = 'transformClassName'
                pushdownConTepm = {}
              } else if (/^cep/.test(i)) {
                const classAfterPartTepm = i.split('=')[1]

                tranConfigInfoTemp = classAfterPartTepm
                tranConfigInfoSqlTemp = classAfterPartTepm
                tranTypeTepm = 'cep'
                pushdownConTepm = {}
                // this.workbenchFlowForm.setFieldsValue({
                //   time_characteristic: tranConfigVal.time_characteristic || '',
                //   parallelism: result.parallelism || 0
                // })
              }
              tranTableSourceTemp.order = index + 1
              tranTableSourceTemp.transformConfigInfo = tranConfigInfoTemp
              tranTableSourceTemp.tranConfigInfoSql = tranConfigInfoSqlTemp
              tranTableSourceTemp.transformConfigInfoRequest = `${i};`
              tranTableSourceTemp.transformType = tranTypeTepm
              tranTableSourceTemp.pushdownConnection = pushdownConTepm
              return tranTableSourceTemp
            })

            this.setState({
              transformTagClassName: 'hide',
              transformTableClassName: '',
              transConnectClass: ''
            })
          } else {
            this.qureryFlowDefaultFinal()
          }
        } else {
          this.qureryFlowDefaultFinal()
        }

        let sinkConfigShow = ''
        let resultFieldsVal = ''
        if (result.sinkConfig !== '') {
          const sinkConfigVal = JSON.parse(JSON.parse(JSON.stringify(result.sinkConfig)))
          sinkConfigShow = sinkConfigVal.sink_specific_config ? JSON.stringify(sinkConfigVal.sink_specific_config) : ''

          if (!result.sinkConfig.includes('output')) {
            resultFieldsVal = 'all'
            this.setState({
              fieldSelected: 'hide'
            }, () => {
              this.workbenchFlowForm.setFieldsValue({
                resultFieldsSelected: '',
                resultFields: 'all'
              })
            })
          } else {
            resultFieldsVal = 'selected'
            this.setState({
              fieldSelected: ''
            }, () => {
              this.workbenchFlowForm.setFieldsValue({
                resultFieldsSelected: sinkConfigVal.sink_output,
                resultFields: 'selected'
              })
            })
          }
        } else {
          sinkConfigShow = ''
          resultFieldsVal = 'all'
          this.setState({
            fieldSelected: 'hide'
          }, () => {
            this.workbenchFlowForm.setFieldsValue({
              resultFieldsSelected: '',
              resultFields: 'all'
            })
          })
        }

        const flowSpecialConfigVal = result.tranConfig !== ''
          ? JSON.stringify(JSON.parse(result.tranConfig).swifts_specific_config)
          : ''

        this.setState({
          sinkConfigCopy: sinkConfigShow,
          sinkDSCopy: sinkNsArr[0]
        }, () => {
          this.workbenchFlowForm.setFieldsValue({
            sourceDataSystem: sourceNsArr[0],
            sourceNamespace: [sourceNsArr[1], sourceNsArr[2], sourceNsArr[3]],
            sinkDataSystem: sinkNsArr[0],
            sinkNamespace: [sinkNsArr[1], sinkNsArr[2], sinkNsArr[3]],

            sinkConfig: this.state.sinkConfigCopy,
            resultFields: resultFieldsVal,
            flowSpecialConfig: flowSpecialConfigVal
          })
          if (result.streamType === 'spark') {
            this.workbenchFlowForm.setFieldsValue({
              dataframeShow: dataframeShowVal
            })
          }
        })
      })
  }

  qureryFlowDefaultFinal () {
    this.setState({
      transformTagClassName: '',
      transformTableClassName: 'hide',
      transConnectClass: 'hide',
      etpStrategyCheck: false,
      etpStrategyRequestValue: {},
      dataframeShowSelected: 'hide'
    })
  }

  queryFlowHdfslog (flow) {
    new Promise((resolve) => {
      const requestData = {
        projectId: flow.projectId,
        streamId: typeof (flow.streamId) === 'object' ? flow.streamIdOrigin : flow.streamId,
        id: flow.id
      }
      this.props.onQueryFlow(requestData, (result) => {
        resolve(result)

        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: result.streamId,
          streamName: result.streamName,
          streamType: result.streamType,
          flowName: result.flowName,
          tableKeys: result.tableKeys
        })

        const resultSinkNsArr = result.sinkNs.split('.')
        const resultSinkNsFinal = [resultSinkNsArr[1], resultSinkNsArr[2], resultSinkNsArr[3]].join('.')

        this.setState({
          formStep: 0,
          pipelineStreamId: result.streamId,
          hdfsSinkNsValue: this.state.flowMode === 'copy' ? '' : resultSinkNsFinal,
          flowKafkaInstanceValue: result.kafka,
          flowKafkaTopicValue: result.topics,
          singleFlowResult: {
            id: result.id,
            projectId: result.projectId,
            streamId: result.streamId,
            sourceNs: result.sourceNs,
            sinkNs: result.sinkNs,
            status: result.status,
            active: result.active,
            createTime: result.createTime,
            createBy: result.createBy,
            updateTime: result.updateTime,
            updateBy: result.updateBy,
            startedTime: result.startedTime,
            stoppedTime: result.stoppedTime
          }
        })
      })
    })
      .then((result) => {
        const sourceNsArr = result.sourceNs.split('.')

        this.workbenchFlowForm.setFieldsValue({
          sourceDataSystem: sourceNsArr[0],
          hdfsNamespace: [
            sourceNsArr[1],
            sourceNsArr[2],
            sourceNsArr[3]
          ],
          sinkConfig: result.sinkConfig
        })
      })
  }

  queryFlowRouting (flow) {
    new Promise((resolve) => {
      const { flowMode } = this.state
      const requestData = {
        projectId: flow.projectId,
        streamId: typeof (flow.streamId) === 'object' ? flow.streamIdOrigin : flow.streamId,
        id: flow.id
      }
      this.props.onQueryFlow(requestData, (result) => {
        resolve(result)

        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: result.streamId,
          streamName: result.streamName,
          streamType: result.streamType,
          flowName: result.flowName,
          tableKeys: result.tableKeys
        })
        const sourceNsArr = result.sourceNs.split('.')
        const showSourceNs = [sourceNsArr[0], sourceNsArr[1], sourceNsArr[2]].join('.')
        const sinkNsArr = result.sinkNs.split('.')
        const showSinkNs = [sinkNsArr[0], sinkNsArr[1], sinkNsArr[2]].join('.')

        this.setState({
          formStep: 0,
          pipelineStreamId: result.streamId,
          routingSourceNsValue: flowMode === 'copy' ? '' : showSourceNs,
          routingSinkNsValue: flowMode === 'copy' ? '' : showSinkNs,
          flowKafkaInstanceValue: result.kafka,
          flowKafkaTopicValue: result.topics,
          singleFlowResult: {
            id: result.id,
            projectId: result.projectId,
            streamId: result.streamId,
            sourceNs: result.sourceNs,
            sinkNs: result.sinkNs,
            status: result.status,
            active: result.active,
            createTime: result.createTime,
            createBy: result.createBy,
            updateTime: result.updateTime,
            updateBy: result.updateBy,
            startedTime: result.startedTime,
            stoppedTime: result.stoppedTime
          }
        })
      })
    })
      .then((result) => {
        const { projectId, pipelineStreamId } = this.state

        const sourceNsArr = result.sourceNs.split('.')
        const sinkNsArr = result.sinkNs.split('.')

        this.props.onLoadSinkTypeNamespace(projectId, pipelineStreamId, 'kafka', 'sinkType', (resultRespone) => {
          const exceptValue = resultRespone.filter(s => [s.nsInstance, s.nsDatabase, s.nsTable].join('.') !== [sourceNsArr[0], sourceNsArr[1], sourceNsArr[2]].join('.'))
          this.setState({
            routingSinkTypeNsData: generateSourceSinkNamespaceHierarchy('kafka', exceptValue)
          })
        })

        this.workbenchFlowForm.setFieldsValue({
          sourceDataSystem: sourceNsArr[0],
          routingNamespace: [sourceNsArr[1], sourceNsArr[2], sourceNsArr[3]],
          routingSinkNs: [sinkNsArr[1], sinkNsArr[2], sinkNsArr[3]],
          sinkConfig: result.sinkConfig
        })
      })
  }
  /**
   * Flow streamType 切换
   */
  changeStreamType = (panel) => e => {
    let value = e.target.value
    switch (panel) {
      case 'flow':
        this.workbenchFlowForm.resetFields()
        this.setState({
          flowMode: 'add',
          formStep: 0,
          flowFormTranTableSource: [],
          transformTagClassName: '',
          transformTableClassName: 'hide',
          transConnectClass: 'hide',
          fieldSelected: 'hide',
          etpStrategyCheck: false,
          dataframeShowSelected: 'hide',
          resultFieldsValue: 'all',
          etpStrategyConfirmValue: '',
          etpStrategyRequestValue: {},
          cepPropData: {},
          outputType: 'detail',
          transformMode: '',
          flowSubPanelKey: value
        }, () => {
          this.workbenchFlowForm.setFieldsValue({
            resultFields: 'all',
            dataframeShow: 'false',
            dataframeShowNum: 10
          })
          this.onInitStreamTypeSelect(this.state.flowFunctionType)
        })
        break
      case 'stream':
        this.setState({
          streamSubPanelKey: value
        }, this.showAddStreamWorkbench)
        break
    }
  }
  getFlowFunctionType = (flowFunctionType) => {
    this.setState({flowFunctionType})
  }
  showAddStreamWorkbench = () => {
    this.workbenchStreamForm.resetFields()
    // 显示 jvm 数据，从而获得初始的 sparkConfig
    this.setState({
      streamMode: 'add',
      streamConfigCheck: false
    })
    const streamType = this.state.streamSubPanelKey
    this.workbenchStreamForm.setFieldsValue({streamType})
    Promise.all(this.loadConfig(streamType)).then((values) => {
      let startConfigJson = {}
      let launchConfigJson = {}

      if (streamType === 'spark') {
        const { driverCores, driverMemory, executorNums, perExecutorMemory, perExecutorCores, durations, partitions, maxRecords } = values[0].spark

        startConfigJson = {
          driverCores,
          driverMemory,
          executorNums,
          perExecutorMemory,
          perExecutorCores
        }
        launchConfigJson = {
          durations,
          partitions,
          maxRecords
        }
      } else if (streamType === 'flink') {
        const { jobManagerMemoryGB, taskManagersNumber, perTaskManagerSlots, perTaskManagerMemoryGB } = values[0].flink

        startConfigJson = {
          jobManagerMemoryGB,
          taskManagersNumber,
          perTaskManagerSlots,
          perTaskManagerMemoryGB
        }
        launchConfigJson = ''
      }

      this.setState({
        streamConfigValues: {
          JVMDriverConfig: values[0].JVMDriverConfig,
          JVMExecutorConfig: values[0].JVMExecutorConfig,
          othersConfig: values[0].othersConfig,
          // streamConfig: `${values[0].jvm},${values[0].others}`,
          startConfig: `${JSON.stringify(startConfigJson)}`,
          launchConfig: launchConfigJson !== '' ? `${JSON.stringify(launchConfigJson)}` : ''
        }
      })
    })

    // 显示 Kafka
    this.props.onLoadkafka(this.state.projectId, 'kafka', (result) => this.setState({ kafkaValues: result }))
  }

  showEditStreamWorkbench = (stream) => () => {
    const { projectId } = this.state
    this.setState({
      streamMode: 'edit',
      streamConfigCheck: true
    })
    this.workbenchStreamForm.resetFields()

    this.props.onLoadStreamDetail(projectId, stream.id, 'user', (result) => {
      const { stream, disableActions, topicInfo, projectName, currentUdf, usingUdf } = result
      const resultVal = Object.assign(stream, {
        disableActions: disableActions,
        topicInfo: topicInfo,
        instance: result.kafkaInfo.instance,
        connUrl: result.kafkaInfo.connUrl,
        projectName: projectName,
        currentUdf: currentUdf,
        usingUdf: usingUdf
      })
      const { name, streamType, functionType, desc, instance, JVMDriverConfig, JVMExecutorConfig, othersConfig, startConfig, launchConfig, id, projectId, specialConfig } = resultVal
      this.workbenchStreamForm.setFieldsValue({
        streamType,
        streamName: name,
        type: functionType,
        desc: desc,
        kafka: instance,
        specialConfig
      })

      this.setState({
        streamConfigValues: {
          JVMDriverConfig,
          JVMExecutorConfig,
          othersConfig,
          startConfig,
          launchConfig
        },

        streamQueryValues: {
          id: id,
          projectId: projectId
        }
      })
    })
  }

  // Stream Config Modal
  onShowConfigModal = () => {
    let { streamConfigValues, streamSubPanelKey: streamType, streamConfigCheck } = this.state
    streamType = this.workbenchStreamForm.getFieldValue('streamType')
    this.setState({
      streamSubPanelKey: streamType,
      streamConfigModalVisible: true
    }, () => {
      if (!streamConfigCheck) this.streamConfigForm.resetFields()

      // 点击 config 按钮时，回显数据。 有且只有2条 jvm 配置
      // const streamConArr = streamConfigValues.streamConfig.split(',')
      // const streamConArr = [streamConfigValues.jvmConfig]
      // if (streamConfigValues.othersConfig) {
      //   streamConArr.push(streamConfigValues.othersConfig)
      // }
      // const tempJvmArr = []
      // const tempOthersArr = []
      // for (let i = 0; i < streamConArr.length; i++) {
      //   // 是否是 jvm
      //   streamConArr[i].includes('extraJavaOptions') ? tempJvmArr.push(streamConArr[i]) : tempOthersArr.push(streamConArr[i])
      // }

      const jvmTempValue = [streamConfigValues.JVMDriverConfig, streamConfigValues.JVMExecutorConfig]
      const personalConfTempValue = streamConfigValues.othersConfig

      const startConfigTemp = JSON.parse(streamConfigValues.startConfig)
      const launchConfigTemp = streamConfigValues.launchConfig && JSON.parse(streamConfigValues.launchConfig)

      if (streamType === 'spark') {
        const { driverCores, driverMemory, executorNums, perExecutorCores, perExecutorMemory } = startConfigTemp
        const { durations, partitions, maxRecords } = launchConfigTemp

        this.streamConfigForm.setFieldsValue({
          // jvm: jvmTempValue,
          JVMDriverConfig: jvmTempValue[0],
          JVMExecutorConfig: jvmTempValue[1],
          driverCores: driverCores,
          driverMemory: driverMemory,
          executorNums: executorNums,
          perExecutorCores: perExecutorCores,
          perExecutorMemory: perExecutorMemory,

          durations: durations,
          partitions: partitions,
          maxRecords: maxRecords,
          personalConf: personalConfTempValue
        })
      } else if (streamType === 'flink') {
        const { jobManagerMemoryGB, taskManagersNumber, perTaskManagerSlots, perTaskManagerMemoryGB } = startConfigTemp
        this.streamConfigForm.setFieldsValue({
          jobManagerMemoryGB,
          taskManagersNumber,
          perTaskManagerSlots,
          perTaskManagerMemoryGB
        })
      }
    })
  }

  // Spark Config Modal
  onShowSparkConfigModal = () => {
    const { jobSparkConfigValues } = this.state

    this.setState({
      sparkConfigModalVisible: true
    }, () => {
      const sparkConArr = [jobSparkConfigValues.sparkConfig.JVMDriverConfig || '', jobSparkConfigValues.sparkConfig.JVMExecutorConfig || '', jobSparkConfigValues.sparkConfig.othersConfig || '']

      const jobTempJvmArr = []
      const jobTempOthersArr = []
      for (let i = 0; i < sparkConArr.length; i++) {
        sparkConArr[i].includes('extraJavaOptions') ? jobTempJvmArr.push(sparkConArr[i]) : jobTempOthersArr.push(sparkConArr[i])
      }

      const jvmTempValue = jobTempJvmArr
      const personalConfTempValue = jobTempOthersArr.join('\n')
      const startConfigTemp = JSON.parse(jobSparkConfigValues.startConfig)

      const { driverCores, driverMemory, executorNums, perExecutorCores, perExecutorMemory, durations, partitions, maxRecords } = startConfigTemp
      this.streamConfigForm.setFieldsValue({
        JVMDriverConfig: jvmTempValue[0],
        JVMExecutorConfig: jvmTempValue[1],
        driverCores: driverCores,
        driverMemory: driverMemory,
        executorNums: executorNums,
        perExecutorCores: perExecutorCores,
        perExecutorMemory: perExecutorMemory,
        personalConf: personalConfTempValue,
        durations: durations,
        partitions: partitions,
        maxRecords: maxRecords
      })
    })
  }

  hideConfigModal = () => this.setState({ streamConfigModalVisible: false })
  hideSparkConfigModal = () => this.setState({ sparkConfigModalVisible: false })

  onConfigModalOk = () => {
    const { locale } = this.props
    const { streamSubPanelKey } = this.state
    this.streamConfigForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        let startConfigJson = {}
        let launchConfigJson = {}
        let jvmConfig = {}
        let othersConfig = ''

        if (streamSubPanelKey === 'spark') {
          values.personalConf = values.personalConf.trim()
          values.JVMDriverConfig = values.JVMDriverConfig.trim()
          values.JVMExecutorConfig = values.JVMExecutorConfig.trim()

          let nJvm = (values.JVMDriverConfig.split('extraJavaOptions')).length + (values.JVMExecutorConfig.split('extraJavaOptions')).length - 2
          let jvmValTemp = {}
          if (nJvm === 2) {
            jvmValTemp = {
              JVMDriverConfig: values.JVMDriverConfig,
              JVMExecutorConfig: values.JVMExecutorConfig
            }

            if (!values.personalConf) {
              jvmConfig = jvmValTemp
            } else {
              // const nOthers = (values.jvm.split('=')).length - 1

              // const personalConfTemp = nOthers === 1
              //   ? values.personalConf
              //   : values.personalConf.replace(/\n/g, ',')

              jvmConfig = jvmValTemp
              othersConfig = values.personalConf
            }
            const { driverCores, driverMemory, executorNums, perExecutorMemory, perExecutorCores } = values
            startConfigJson = {
              driverCores: driverCores,
              driverMemory: driverMemory,
              executorNums: executorNums,
              perExecutorMemory: perExecutorMemory,
              perExecutorCores: perExecutorCores
            }

            const { durations, partitions, maxRecords } = values
            launchConfigJson = {
              durations: durations,
              partitions: partitions,
              maxRecords: maxRecords
            }
          } else {
            message.warning(locale === 'en' ? 'Please configure JVM correctly!' : '请正确配置 JVM！', 3)
            return
          }
        } else if (streamSubPanelKey === 'flink') {
          const { jobManagerMemoryGB, perTaskManagerMemoryGB, perTaskManagerSlots, taskManagersNumber } = values
          startConfigJson = {
            jobManagerMemoryGB,
            perTaskManagerMemoryGB,
            perTaskManagerSlots,
            taskManagersNumber
          }
          launchConfigJson = ''
        }
        this.setState({
          streamConfigCheck: true,
          streamConfigValues: {
            // streamConfig: streamConfigValue,
            // jvmConfig,
            JVMDriverConfig: jvmConfig.JVMDriverConfig,
            JVMExecutorConfig: jvmConfig.JVMExecutorConfig,
            othersConfig,
            startConfig: JSON.stringify(startConfigJson),
            launchConfig: launchConfigJson && JSON.stringify(launchConfigJson)
          }
        })
        this.hideConfigModal()
      }
    })
  }

  onSparkConfigModalOk = () => {
    const { locale } = this.props
    this.streamConfigForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        values.personalConf = values.personalConf.trim()
        values.JVMDriverConfig = values.JVMDriverConfig.trim()
        values.JVMExecutorConfig = values.JVMExecutorConfig.trim()
        let jvm = `${values.JVMDriverConfig},${values.JVMExecutorConfig}`
        const nJvm = (jvm.split('extraJavaOptions')).length - 1
        // let jvmValTemp = ''
        if (nJvm === 2) {
          // jvmValTemp = jvm.replace(/\n/g, ',')

          // let sparkConfigVal = ''
          // if (!values.personalConf) {
          //   sparkConfigVal = jvmValTemp
          // } else {
          //   const nOthers = (jvm.split('=')).length - 1

          //   const personalConfTemp = nOthers === 1
          //     ? values.personalConf
          //     : values.personalConf.replace(/\n/g, ',')

          //   sparkConfigVal = `${jvmValTemp},${personalConfTemp}`
          // }

          const { driverCores, driverMemory, executorNums, perExecutorMemory, perExecutorCores, JVMDriverConfig, JVMExecutorConfig, personalConf } = values
          const startConfigJson = {
            driverCores: driverCores,
            driverMemory: driverMemory,
            executorNums: executorNums,
            perExecutorMemory: perExecutorMemory,
            perExecutorCores: perExecutorCores
          }

          this.setState({
            sparkConfigCheck: true,
            jobSparkConfigValues: {
              sparkConfig: {
                JVMDriverConfig,
                JVMExecutorConfig,
                othersConfig: personalConf
              },
              startConfig: JSON.stringify(startConfigJson)
            }
          })
          this.hideSparkConfigModal()
        } else {
          message.warning(locale === 'en' ? 'Please configure JVM correctly!' : '请正确配置 JVM！', 3)
        }
      }
    })
  }

  hideFlowWorkbench = () => this.setState({ flowMode: '' })
  hideStreamWorkbench = () => this.setState({ streamMode: '' })
  hideJobWorkbench = () => this.setState({ jobMode: '' })

  forwardStep = () => {
    const { tabPanelKey, streamDiffType, jobDiffType } = this.state
    switch (tabPanelKey) {
      // FIXED: 修复 从stream点id跳转过来后，‘下一步’点击无效的bug，由于stream暂无‘下一步’，所以暂时在此处fix一下。
      case 'stream':
      case 'flow':
        if (streamDiffType === 'default') {
          this.handleForwardDefault()
        } else if (streamDiffType === 'hdfslog' || streamDiffType === 'hdfscsv' || streamDiffType === 'routing') {
          this.handleForwardHdfslogOrRouting()
        }
        break
      case 'job':
        if (jobDiffType === 'default') {
          this.handleForwardJob()
        } else if (jobDiffType === 'backfill') {
          this.handleForwardJobBackfill()
        }
        break
    }
  }

  loadSTSExit (values) {
    const { flowMode, flowSourceNsSys } = this.state

    if (flowMode === 'add' || flowMode === 'copy') {
      // 新增flow时验证source to sink 是否存在
      const sourceInfo = [flowSourceNsSys, values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2], '*', '*', '*'].join('.')
      const sinkInfo = [values.sinkDataSystem, values.sinkNamespace[0], values.sinkNamespace[1], values.sinkNamespace[2], '*', '*', '*'].join('.')

      this.props.onLoadSourceToSinkExist(this.state.projectId, sourceInfo, sinkInfo, () => {
        this.setState({
          formStep: this.state.formStep + 1,
          step2SourceNamespace: [flowSourceNsSys, values.sourceNamespace.join('.')].join('.'),
          step2SinkNamespace: [values.sinkDataSystem, values.sinkNamespace.join('.')].join('.')
        })
      }, () => {
        message.error(operateLanguageSourceToSink(), 3)
      })
    } else if (flowMode === 'edit') {
      this.setState({
        formStep: this.state.formStep + 1,
        step2SourceNamespace: [flowSourceNsSys, values.sourceNamespace.join('.')].join('.'),
        step2SinkNamespace: [values.sinkDataSystem, values.sinkNamespace.join('.')].join('.')
      })
    }
  }

  handleForwardDefault () {
    const { formStep, flowFormTranTableSource, streamDiffType, flowSubPanelKey } = this.state
    const { locale } = this.props

    let tranRequestTempArr = []
    flowFormTranTableSource.map(i => {
      let isCep = i.transformConfigInfoRequest.split('=')[0].indexOf('cep') > -1
      if (isCep) {
        tranRequestTempArr.push(`${i.transformConfigInfoRequest}`)
      } else {
        tranRequestTempArr.push(preProcessSql(i.transformConfigInfoRequest))
      }
    })
    const tranRequestTempString = tranRequestTempArr.join('')
    if (flowSubPanelKey === 'flink') {
      let timeCharacteristic = this.workbenchFlowForm.getFieldsValue(['time_characteristic'])
      this.setState({
        transformTableRequestValue: tranRequestTempString === '' ? {} : Object.assign({'action': tranRequestTempString}, timeCharacteristic),
        transformTableConfirmValue: tranRequestTempString === '' ? '' : `"${tranRequestTempString}"`,
        timeCharacteristic: timeCharacteristic.time_characteristic
      })
    } else if (flowSubPanelKey === 'spark') {
      this.setState({
        transformTableRequestValue: tranRequestTempString === '' ? {} : {'action': tranRequestTempString},
        transformTableConfirmValue: tranRequestTempString === '' ? '' : `"${tranRequestTempString}"`
      })
    }

    // 只有 lookup sql 才有 pushdownConnection
    let tempSource = flowFormTranTableSource.filter(s => s.pushdownConnection['name_space'])

    let pushConnTemp = []
    for (let item of tempSource) {
      pushConnTemp.push(item.pushdownConnection)
    }

    this.setState({
      pushdownConnectRequestValue: pushConnTemp === '' ? {} : {'pushdown_connection': pushConnTemp}
    })

    this.workbenchFlowForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        switch (formStep) {
          case 0:
            if (!values.sinkConfig) {
              const dataText = locale === 'en' ? 'When Data System is ' : 'Data System 为 '
              const emptyText = locale === 'en' ? ', Sink Config cannot be empty!' : ' 时，Sink Config 不能为空！'
              values.sinkDataSystem === 'hbase'
                ? message.error(`${dataText}${values.sinkDataSystem}${emptyText}`, 3)
                : this.loadSTSExit(values)
            } else {
              // json 校验
              isJSON(values.sinkConfig)
                ? this.loadSTSExit(values)
                : message.error(locale === 'en' ? 'Sink Config should be JSON format!' : 'Sink Config 应为 JSON格式！', 3)
            }

            const rfSelect = this.workbenchFlowForm.getFieldValue('resultFields')
            if (rfSelect === 'all') {
              this.setState({
                resultFiledsOutput: {},
                resultFieldsValue: 'all'
              })
            } else if (rfSelect === 'selected') {
              const rfSelectSelected = this.workbenchFlowForm.getFieldValue('resultFieldsSelected')
              this.setState({
                resultFiledsOutput: { sink_output: rfSelectSelected },
                resultFieldsValue: rfSelectSelected
              })
            }
            break
          case 1:
            switch (streamDiffType) {
              case 'default':
                const dataframeShowSelect = this.workbenchFlowForm.getFieldValue('dataframeShow')
                if (dataframeShowSelect === 'true') {
                  const dataframeShowNum = this.workbenchFlowForm.getFieldValue('dataframeShowNum')
                  this.setState({
                    dataframeShowOrNot: {'dataframe_show': 'true', 'dataframe_show_num': dataframeShowNum},
                    dataframeShowNumValue: `true; Number is ${dataframeShowNum}`
                  })
                } else {
                  this.setState({
                    dataframeShowOrNot: {'dataframe_show': 'false'},
                    dataframeShowNumValue: 'false'
                  })
                }
                this.setState({
                  formStep: this.state.formStep + 1,
                  transConfigConfirmValue: values.flowSpecialConfig
                })
                break
              case 'hdfslog':
                this.setState({ formStep: this.state.formStep + 2 })
                break
            }
            break
        }
      }
    })
  }

  handleForwardHdfslogOrRouting () {
    const { flowMode, projectId, formStep, streamDiffType, flowSourceNsSys } = this.state

    this.workbenchFlowForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (flowMode === 'add' || flowMode === 'copy') {
          // 新增flow时验证source to sink 是否存在
          const sourceInfo = streamDiffType === 'hdfslog' || streamDiffType === 'hdfscsv'
            ? [flowSourceNsSys, values.hdfsNamespace[0], values.hdfsNamespace[1], values.hdfsNamespace[2], '*', '*', '*'].join('.')
            : [flowSourceNsSys, values.routingNamespace[0], values.routingNamespace[1], values.routingNamespace[2], '*', '*', '*'].join('.')

          const sinkInfo = streamDiffType === 'hdfslog' || streamDiffType === 'hdfscsv'
            ? sourceInfo
            : ['kafka', values.routingSinkNs[0], values.routingSinkNs[1], values.routingSinkNs[2], '*', '*', '*'].join('.')

          this.props.onLoadSourceToSinkExist(projectId, sourceInfo, sinkInfo, () => {
            this.setState({ formStep: formStep + 2 })
          }, () => {
            message.error(operateLanguageSourceToSink(), 3)
          })
        } else if (flowMode === 'edit') {
          this.setState({ formStep: formStep + 2 })
        }
      }
    })
  }

  loadJobSTSExit (values, step = 1) {
    const { jobMode, formStep, projectId, jobDiffType, jobSourceNsSys } = this.state
    let jobStepSourceNs = [jobSourceNsSys, values.sourceNamespace.join('.')].join('.')
    let jobStepSinkNs = jobDiffType === 'backfill' ? jobStepSourceNs : [values.sinkDataSystem, values.sinkNamespace.join('.')].join('.')
    switch (jobMode) {
      case 'add':
        // 新增 Job 时验证 source to sink 是否存在
        const sourceInfo = [jobSourceNsSys, values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2], '*', '*', '*'].join('.')
        const sinkInfo = jobDiffType === 'backfill'
          ? sourceInfo
          : [values.sinkDataSystem, values.sinkNamespace[0], values.sinkNamespace[1], values.sinkNamespace[2], '*', '*', '*'].join('.')

        this.props.onLoadJobSourceToSinkExist(projectId, sourceInfo, sinkInfo, () => {
          this.setState({
            formStep: formStep + step,
            jobStepSourceNs,
            jobStepSinkNs
          })
        }, () => {
          message.error(operateLanguageSourceToSink(), 3)
        })
        break
      case 'edit':
        this.setState({
          formStep: formStep + step,
          jobStepSourceNs,
          jobStepSinkNs
        })
        break
    }
  }

  handleForwardJob () {
    const { formStep, jobFormTranTableSource } = this.state
    const { locale } = this.props

    this.workbenchJobForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        switch (formStep) {
          case 0:
            if (!values.sinkConfig) {
              const dataText = locale === 'en' ? 'When Data System is ' : 'Data System 为 '
              const emptyText = locale === 'en' ? ', Sink Config cannot be empty!' : ' 时，Sink Config 不能为空！'
              values.sinkDataSystem === 'hbase'
                ? message.error(`${dataText}${values.sinkDataSystem}${emptyText}`, 3)
                : this.loadJobSTSExit(values)
            } else {
              isJSON(values.sinkConfig)
                ? this.loadJobSTSExit(values)
                : message.error(locale === 'en' ? 'Sink Config should be JSON format!' : 'Sink Config 应为 JSON格式！', 3)
            }

            const rfSelect = this.workbenchJobForm.getFieldValue('resultFields')
            if (rfSelect === 'all') {
              this.setState({
                jobResultFiledsOutput: {},
                jobResultFieldsValue: 'all'
              })
            } else if (rfSelect === 'selected') {
              const rfSelectSelected = this.workbenchJobForm.getFieldValue('resultFieldsSelected')
              this.setState({
                jobResultFiledsOutput: { sink_output: rfSelectSelected },
                jobResultFieldsValue: rfSelectSelected
              })
            }
            break
          case 1:
            let tranRequestTempArr = []
            jobFormTranTableSource.map(i => tranRequestTempArr.push(i.transformConfigInfoRequest))
            const tranRequestTempString = tranRequestTempArr.join('')

            let tempRequestVal = {}
            tempRequestVal = tranRequestTempString === '' ? {} : {'action': tranRequestTempString}

            this.setState({
              formStep: formStep + 1,
              jobTranTableRequestValue: tempRequestVal,
              jobTranTableConfirmValue: tranRequestTempString === '' ? '' : `"${tranRequestTempString}"`,
              jobTranConfigConfirmValue: values.jobSpecialConfig
            })
            break
        }
      }
    })
  }

  handleForwardJobBackfill () {
    const { formStep } = this.state
    this.workbenchJobForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (formStep === 0) {
          this.loadJobSTSExit(values, 2)
        }
      }
    })
  }

  backwardStep = () => {
    const { streamDiffType, formStep, tabPanelKey, jobDiffType } = this.state

    switch (tabPanelKey) {
      case 'flow':
        this.setState({
          formStep: streamDiffType === 'default' ? formStep - 1 : formStep - 2
        })
        break
      case 'job':
        this.setState({ formStep: jobDiffType === 'default' ? formStep - 1 : formStep - 2 })
        break
    }
  }

  generateStepButtons = () => {
    const { tabPanelKey, formStep } = this.state
    const { flowSubmitLoading, jobSubmitLoading } = this.props

    switch (formStep) {
      case 0:
        return (
          <div className="ri-workbench-step-button-area">
            <Button type="primary" className="next" onClick={this.forwardStep}>
              <FormattedMessage {...messages.workbenchNext} />
            </Button>
          </div>
        )
      case 1:
        return (
          <div className="ri-workbench-step-button-area">
            <Button type="ghost" onClick={this.backwardStep}>
              <FormattedMessage {...messages.workbenchBack} />
            </Button>
            <Button type="primary" className="next" onClick={this.forwardStep}>
              <FormattedMessage {...messages.workbenchNext} />
            </Button>
          </div>
        )
      case 2:
        return (
          <div className="ri-workbench-step-button-area">
            <Button type="ghost" onClick={this.backwardStep}>
              <FormattedMessage {...messages.workbenchBack} />
            </Button>
            <Button
              type="primary"
              className="next"
              loading={tabPanelKey === 'flow' ? flowSubmitLoading : jobSubmitLoading}
              onClick={tabPanelKey === 'flow' ? this.submitFlowForm : this.submitJobForm}>
              <FormattedMessage {...messages.workbenchSubmit} />
            </Button>
          </div>
        )
      default:
        return ''
    }
  }

  initStartTS = (val) => {
    // 将 YYYY-MM-DD HH:mm:ss 转换成 YYYYMMDDHHmmss 格式
    this.setState({ startTsVal: val.replace(/-| |:/g, '') })
  }

  initEndTS = (val) => {
    this.setState({ endTsVal: val.replace(/-| |:/g, '') })
  }

  submitJobForm = () => {
    const values = this.workbenchJobForm.getFieldsValue()

    const { projectId, jobMode, startTsVal, endTsVal, singleJobResult, jobDiffType, mapJobType, jobSourceNsSys, jobResultFiledsOutput, jobTranTableRequestValue, jobSparkConfigValues } = this.state
    const { locale } = this.props

    const maxRecordJson = { maxRecordPerPartitionProcessed: values.maxRecordPerPartitionProcessed }
    const maxRecord = JSON.stringify(maxRecordJson)
    const maxRecordAndResult = JSON.stringify(Object.assign(maxRecordJson, jobResultFiledsOutput))

    const obj1 = {
      maxRecordPerPartitionProcessed: Number(values.maxRecordPerPartitionProcessed),
      sink_protocol: 'snapshot'
    }
    const obj2 = {
      maxRecordPerPartitionProcessed: Number(values.maxRecordPerPartitionProcessed),
      sink_specific_config: values.sinkConfig
    }
    const obj3 = {
      maxRecordPerPartitionProcessed: Number(values.maxRecordPerPartitionProcessed),
      sink_protocol: 'snapshot',
      sink_specific_config: values.sinkConfig
    }

    let sinkConfigRequest = ''
    if (jobDiffType === 'default') {
      if (values.resultFields === 'all') {
        if (!values.sinkConfig) {
          sinkConfigRequest = values.sinkProtocol ? JSON.stringify(obj1) : maxRecord
        } else {
          sinkConfigRequest = values.sinkProtocol ? JSON.stringify(obj3) : JSON.stringify(obj2)
        }
      } else {
        const obg4 = { sink_output: values.resultFieldsSelected }
        if (!values.sinkConfig) {
          sinkConfigRequest = values.sinkProtocol ? JSON.stringify(Object.assign(obj1, obg4)) : maxRecordAndResult
        } else {
          sinkConfigRequest = values.sinkProtocol ? JSON.stringify(Object.assign(obj3, obg4)) : JSON.stringify(Object.assign(obj2, obg4))
        }
      }
    } else if (jobDiffType === 'backfill') {
      sinkConfigRequest = ''
    }

    let tranConfigRequest = {}
    if (!jobTranTableRequestValue['action']) {
      tranConfigRequest = ''
    } else {
      const tranConfigRequestTemp = !values.jobSpecialConfig
        ? jobTranTableRequestValue
        : Object.assign(jobTranTableRequestValue, { 'swifts_specific_config': JSON.parse(values.jobSpecialConfig) })
      tranConfigRequest = JSON.stringify(tranConfigRequestTemp)
    }

    const requestCommon = {
      eventTsStart: values.eventStartTs ? startTsVal : '',
      eventTsEnd: values.eventEndTs ? endTsVal : '',
      sinkConfig: sinkConfigRequest,
      tranConfig: tranConfigRequest,
      tableKeys: values.tableKeys,
      desc: ''
    }

    const sourceDataInfo = [jobSourceNsSys, values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2], values.sourceNamespaceVersion, '*', '*'].join('.')
    if (jobMode === 'add') {
      // source data system 选择log后，根据接口返回的nsSys值，拼接 sourceDataInfo
      const sinkDataInfo = jobDiffType === 'backfill' ? sourceDataInfo : [values.sinkDataSystem, values.sinkNamespace[0], values.sinkNamespace[1], values.sinkNamespace[2], '*', '*', '*'].join('.')

      const submitJobData = {
        name: values.jobName,
        sourceNs: sourceDataInfo,
        sinkNs: sinkDataInfo,
        jobType: mapJobType[values.type],
        sourceConfig: `{"protocol":"${values.protocol}"}`
      }

      this.props.onAddJob(Number(projectId), Object.assign(submitJobData, jobSparkConfigValues, requestCommon), () => {
        message.success(locale === 'en' ? 'Job is created successfully!' : 'Job 添加成功！', 3)
      }, () => {
        this.hideJobSubmit()
      })
    } else if (jobMode === 'edit') {
      const sourceNsData = {
        sourceNs: sourceDataInfo
      }
      this.props.onEditJob(Object.assign(singleJobResult, jobSparkConfigValues, requestCommon, sourceNsData, {
        sourceConfig: `{"protocol":"${values.protocol}"}`
      }), () => {
        message.success(locale === 'en' ? 'Job is modified successfully!' : 'Job 修改成功！', 3)
      }, () => {
        this.hideJobSubmit()
      })
    }
  }

  hideJobSubmit = () => {
    this.workbenchJobForm.resetFields()
    this.setState({
      jobMode: '',
      jobTranTagClassName: '',
      jobTranTableClassName: 'hide',
      fieldSelected: 'hide',
      jobFormTranTableSource: [],
      sourceNsVersionList: []
    })
  }

  submitFlowForm = () => {
    const { streamDiffType } = this.state

    switch (streamDiffType) {
      case 'default':
        this.handleSubmitFlowDefault()
        break
      case 'hdfslog':
      case 'hdfscsv':
        this.handleSubmitFlowHdfslog()
        break
      case 'routing':
        this.handleSubmitFlowRouting()
        break
    }
  }

  handleSubmitFlowDefault () {
    const values = this.workbenchFlowForm.getFieldsValue()
    const { projectId, flowMode, singleFlowResult, flowSubPanelKey } = this.state
    const { resultFiledsOutput, dataframeShowOrNot, etpStrategyRequestValue, transformTableRequestValue, pushdownConnectRequestValue, flowSourceNsSys } = this.state
    const { locale } = this.props

    let sinkConfigRequest = ''
    if (values.resultFields === 'all') {
      sinkConfigRequest = values.sinkConfig
        ? `{"sink_specific_config":${values.sinkConfig}}`
        : ''
    } else {
      sinkConfigRequest = values.sinkConfig
        ? `{"sink_specific_config":${values.sinkConfig},"sink_output":"${values.resultFieldsSelected}"}`
        : JSON.stringify(resultFiledsOutput)
    }

    let tranConfigRequest = {}
    if (!transformTableRequestValue['action']) {
      tranConfigRequest = ''
    } else {
      const objectTemp = Object.assign(etpStrategyRequestValue, transformTableRequestValue, pushdownConnectRequestValue, dataframeShowOrNot)
      const tranConfigRequestTemp = values.flowSpecialConfig
        ? Object.assign(objectTemp, {'swifts_specific_config': JSON.parse(values.flowSpecialConfig)})
        : objectTemp
      tranConfigRequest = JSON.stringify(tranConfigRequestTemp)
    }
    const isCheckpoint = flowSubPanelKey === 'spark' ? null : flowSubPanelKey === 'flink' ? values.checkpoint : null
    const checkpoint = { enable: isCheckpoint, checkpoint_interval_ms: 300000, stateBackend: 'hdfs://flink-checkpoint' }
    if (flowMode === 'add' || flowMode === 'copy') {
      const sourceDataInfo = [flowSourceNsSys, values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2], '*', '*', '*'].join('.')
      const sinkDataInfo = [values.sinkDataSystem, values.sinkNamespace[0], values.sinkNamespace[1], values.sinkNamespace[2], '*', '*', '*'].join('.')
      const parallelism = flowSubPanelKey === 'spark' ? null : flowSubPanelKey === 'flink' ? values.parallelism : null
      const config = {
        parallelism,
        checkpoint
      }
      const submitFlowData = {
        projectId: Number(projectId),
        streamId: Number(values.flowStreamId),
        sourceNs: sourceDataInfo,
        sinkNs: sinkDataInfo,
        consumedProtocol: values.protocol.join(','),
        sinkConfig: `${sinkConfigRequest}`,
        tranConfig: tranConfigRequest,
        config: JSON.stringify(config),
        flowName: values.flowName,
        tableKeys: values.tableKeys,
        desc: null
      }

      this.props.onAddFlow(submitFlowData, () => {
        if (flowMode === 'add') {
          message.success(locale === 'en' ? 'Flow is created successfully!' : 'Flow 添加成功！', 3)
        } else if (flowMode === 'copy') {
          message.success(locale === 'en' ? 'Flow is copied successfully!' : 'Flow 复制成功！', 3)
        }
      }, () => {
        this.hideFlowSubmit()
        this.hideFlowDefaultSubmit()
      })
    } else if (flowMode === 'edit') {
      const editData = {
        sinkConfig: `${sinkConfigRequest}`,
        tranConfig: tranConfigRequest,
        consumedProtocol: values.protocol.join(','),
        flowName: values.flowName,
        tableKeys: values.tableKeys,
        desc: null
      }
      const config = {}
      if (values.parallelism != null) {
        config.parallelism = values.parallelism
      }
      config.checkpoint = checkpoint
      editData.config = JSON.stringify(config)
      this.props.onEditFlow(Object.assign(editData, singleFlowResult), () => {
        message.success(locale === 'en' ? 'Flow is modified successfully!' : 'Flow 修改成功！', 3)
      }, () => {
        this.hideFlowSubmit()
        this.hideFlowDefaultSubmit()
      })
    }
  }
  hideFlowDefaultSubmit () {
    this.setState({
      transformTagClassName: '',
      transformTableClassName: 'hide',
      transConnectClass: 'hide',
      fieldSelected: 'hide',
      etpStrategyCheck: false,
      etpStrategyRequestValue: {},
      dataframeShowSelected: 'hide',
      flowFormTranTableSource: [],
      flowPatternCepDataSource: []
    })
  }

  handleSubmitFlowHdfslog () {
    const { flowMode, projectId, singleFlowResult, flowSourceNsSys } = this.state

    this.workbenchFlowForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (flowMode === 'add' || flowMode === 'copy') {
          const sourceDataInfo = [flowSourceNsSys, values.hdfsNamespace[0], values.hdfsNamespace[1], values.hdfsNamespace[2], '*', '*', '*'].join('.')
          // const parallelism = flowSubPanelKey === 'spark' ? null : flowSubPanelKey === 'flink' ? values.parallelism : null

          const submitFlowData = {
            projectId: Number(projectId),
            streamId: Number(values.flowStreamId),
            sourceNs: sourceDataInfo,
            sinkNs: sourceDataInfo,
            consumedProtocol: 'all',
            sinkConfig: '',
            tranConfig: '',
            flowName: values.flowName,
            tableKeys: values.tableKeys,
            desc: null
            // parallelism
          }

          this.props.onAddFlow(submitFlowData, (result) => {
            if (result.length === 0) {
              message.success(operateLanguageSuccessMessage('Flow', 'existed'), 3)
            } else if (flowMode === 'add') {
              message.success(operateLanguageSuccessMessage('Flow', 'create'), 3)
            } else if (flowMode === 'copy') {
              message.success(operateLanguageSuccessMessage('Flow', 'copy'), 3)
            }
          }, () => {
            this.hideFlowSubmit()
          })
        } else if (flowMode === 'edit') {
          const editData = {
            sinkConfig: '',
            tranConfig: '',
            consumedProtocol: 'all'
          }

          this.props.onEditFlow(Object.assign(editData, singleFlowResult), () => {
            message.success(operateLanguageSuccessMessage('Flow', 'modify'), 3)
          }, () => {
            this.hideFlowSubmit()
          })
        }
      }
    })
  }

  handleSubmitFlowRouting () {
    const { flowMode, projectId, singleFlowResult, flowSourceNsSys } = this.state

    this.workbenchFlowForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (flowMode === 'add' || flowMode === 'copy') {
          const sourceDataInfo = [flowSourceNsSys, values.routingNamespace[0], values.routingNamespace[1], values.routingNamespace[2], '*', '*', '*'].join('.')
          const sinkDataInfo = ['kafka', values.routingSinkNs[0], values.routingSinkNs[1], values.routingSinkNs[2], '*', '*', '*'].join('.')
          // const parallelism = flowSubPanelKey === 'spark' ? null : flowSubPanelKey === 'flink' ? values.parallelism : null

          const submitFlowData = {
            projectId: Number(projectId),
            streamId: Number(values.flowStreamId),
            sourceNs: sourceDataInfo,
            sinkNs: sinkDataInfo,
            consumedProtocol: 'all',
            sinkConfig: '',
            tranConfig: '',
            flowName: values.flowName,
            tableKeys: values.tableKeys,
            desc: null
            // parallelism
          }

          this.props.onAddFlow(submitFlowData, (result) => {
            if (result.length === 0) {
              message.success(operateLanguageSuccessMessage('Flow', 'existed'), 3)
            } else if (flowMode === 'add') {
              message.success(operateLanguageSuccessMessage('Flow', 'create'), 3)
            } else if (flowMode === 'copy') {
              message.success(operateLanguageSuccessMessage('Flow', 'copy'), 3)
            }
          }, () => {
            this.hideFlowSubmit()
          })
        } else if (flowMode === 'edit') {
          const editData = {
            sinkConfig: '',
            tranConfig: '',
            consumedProtocol: 'all'
          }

          this.props.onEditFlow(Object.assign(editData, singleFlowResult), () => {
            message.success(operateLanguageSuccessMessage('Flow', 'modify'), 3)
          }, () => {
            this.hideFlowSubmit()
          })
        }
      }
    })
  }

  hideFlowSubmit = () => this.setState({ flowMode: '' })

  submitStreamForm = () => {
    const { projectId, streamMode, streamConfigValues, streamConfigCheck, streamQueryValues } = this.state

    this.workbenchStreamForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const specialConfig = values.specialConfig
        if (specialConfig && !isJSON(specialConfig)) {
          message.error('Special Config Format Error, Must be JSON', 3)
          return
        }
        switch (streamMode) {
          case 'add':
            const requestValues = {
              name: values.streamName,
              desc: values.desc,
              instanceId: Number(values.kafka),
              functionType: values.type,
              streamType: values.streamType,
              specialConfig
            }

            this.props.onAddStream(projectId, Object.assign(requestValues, streamConfigValues), () => {
              message.success(operateLanguageSuccessMessage('Stream', 'create'), 3)
              this.setState({
                streamMode: ''
              })
              this.workbenchStreamForm.resetFields()
              if (streamConfigCheck) {
                this.streamConfigForm.resetFields()
              }
              this.hideStreamSubmit()
            })
            break
          case 'edit':
            const editValues = { desc: values.desc, specialConfig }
            const requestEditValues = Object.assign(editValues, streamQueryValues, streamConfigValues)

            this.props.onEditStream(requestEditValues, () => {
              message.success(operateLanguageSuccessMessage('Stream', 'modify'), 3)
              this.setState({ streamMode: '' })
              this.hideStreamSubmit()
            })
            break
        }
      }
    })
  }

  hideStreamSubmit = () => {
    this.setState({
      isWormhole: true,
      streamConfigCheck: false
    })
  }

  onShowTransformModal = () => this.setState({ transformModalVisible: true })
  onShowJobTransModal = () => this.setState({ jobTransModalVisible: true })

  // flow transformation type 显示不同的内容
  onInitTransformValue = (value) => {
    this.setState({
      transformValue: value
    }, () => {
      if (this.state.transformValue !== 'transformClassName') {
        this.makeSqlCodeMirrorInstance(value)

        switch (this.state.transformValue) {
          case 'lookupSql':
            this.cmLookupSql.doc.setValue(this.cmLookupSql.doc.getValue() || '')
            break
          case 'sparkSql':
            this.cmSparkSql.doc.setValue(this.cmSparkSql.doc.getValue() || '')
            break
          case 'streamJoinSql':
            this.cmStreamJoinSql.doc.setValue(this.cmStreamJoinSql.doc.getValue() || '')
            this.loadTransNs()
            break
          case 'flinkSql':
            this.cmFlinkSql.doc.setValue(this.cmFlinkSql.doc.getValue() || '')
            break
        }
      }
    })
  }

  // namespace 下拉框内容
  loadTransNs () {
    const { projectId, pipelineStreamId } = this.state
    const flowValues = this.workbenchFlowForm.getFieldsValue()
    const { sourceNamespace } = flowValues
    this.props.onLoadSourceSinkTypeNamespace(projectId, pipelineStreamId, 'kafka', 'instanceType', (result) => {
      const resultFinal = result.filter((i) => {
        const temp = [i.nsInstance, i.nsDatabase, i.nsTable]
        if (temp.join(',') !== sourceNamespace.join(',')) {
          return i
        } else {
          return
        }
      })
      this.setState({
        flowTransNsData: resultFinal
      })
    })
  }

  makeSqlCodeMirrorInstance (value) {
    switch (value) {
      case 'lookupSql':
        if (!this.cmLookupSql) {
          const temp = document.getElementById('lookupSqlTextarea')
          this.cmLookupSql = CodeMirror.fromTextArea(temp, {
            lineNumbers: true,
            matchBrackets: true,
            autoCloseBrackets: true,
            mode: 'text/x-sql',
            lineWrapping: true
          })
          this.cmLookupSql.setSize('100%', '208px')
        }
        break
      case 'sparkSql':
        if (!this.cmSparkSql) {
          const temp = document.getElementById('sparkSqlTextarea')
          this.cmSparkSql = CodeMirror.fromTextArea(temp, {
            lineNumbers: true,
            matchBrackets: true,
            autoCloseBrackets: true,
            mode: 'text/x-sql',
            lineWrapping: true
          })
          this.cmSparkSql.setSize('100%', '238px')
        }
        break
      case 'streamJoinSql':
        if (!this.cmStreamJoinSql) {
          const temp = document.getElementById('streamJoinSqlTextarea')
          this.cmStreamJoinSql = CodeMirror.fromTextArea(temp, {
            lineNumbers: true,
            matchBrackets: true,
            autoCloseBrackets: true,
            mode: 'text/x-sql',
            lineWrapping: true
          })
          this.cmStreamJoinSql.setSize('100%', '238px')
        }
        break
      case 'flinkSql':
        if (!this.cmFlinkSql) {
          const temp = document.getElementById('flinkSqlTextarea')
          this.cmFlinkSql = CodeMirror.fromTextArea(temp, {
            lineNumbers: true,
            matchBrackets: true,
            autoCloseBrackets: true,
            mode: 'text/x-sql',
            lineWrapping: true
          })
          this.cmFlinkSql.setSize('100%', '238px')
        }
        break
    }
  }

  // job transformation type 显示不同的内容
  onInitJobTransValue = (value) => {
    this.setState({
      jobTransValue: value
    }, () => {
      if (this.state.jobTransValue === 'sparkSql') {
        this.makeJobSqlCodeMirrorInstance()
        this.cmJobSparkSql.doc.setValue(this.cmJobSparkSql.doc.getValue() || '')
      }
    })
  }

  makeJobSqlCodeMirrorInstance () {
    if (!this.cmJobSparkSql) {
      const temp = document.getElementById('jobSparkSqlTextarea')
      this.cmJobSparkSql = CodeMirror.fromTextArea(temp, {
        lineNumbers: true,
        matchBrackets: true,
        autoCloseBrackets: true,
        mode: 'text/x-sql',
        lineWrapping: true
      })
      this.cmJobSparkSql.setSize('100%', '238px')
    }
  }

  onEditTransform = (record) => (e) => {
    // 加隐藏字段获得 record.transformType
    if (record.transformType === 'cep') {
      this.setState({
        cepPropData: this.state.flowFormTranTableSource[record.order - 1],
        transformMode: 'edit',
        transformValue: record.transformType
      }, () => {
        let cepFormData = typeof record.tranConfigInfoSql === 'string' && JSON.parse(record.tranConfigInfoSql.split(';')[0])
        let outputFieldList = this.state.outputFieldList
        if (cepFormData.output) {
          if (cepFormData.output.type === 'agg' || cepFormData.output.type === 'filteredRow') {
            outputFieldList = cepFormData.output && cepFormData.output.field_list
            outputFieldList.forEach((v, i) => {
              if (!v._id) {
                v._id = Date.now() - i
              }
            })
          }
        }
        const outputType = cepFormData.output && cepFormData.output.type
        this.setState({
          outputType,
          outputFieldList,
          transformModalVisible: true
        }, () => {
          this.flowTransformForm.setFieldsValue({
            windowTime: cepFormData.max_interval_seconds || '',
            strategy: cepFormData.strategy,
            keyBy: cepFormData.key_by_fields,
            output: cepFormData.output && cepFormData.output.type,
            editTransformId: record.order,
            transformation: record.transformType
          })
        })
      })
    } else {
      this.setState({
        transformMode: 'edit',
        transformModalVisible: true,
        transformValue: record.transformType
      }, () => {
        this.flowTransformForm.setFieldsValue({
          editTransformId: record.order,
          transformation: record.transformType
        })

        if (record.transformType !== 'transformClassName') {
          this.makeSqlCodeMirrorInstance(record.transformType)
        }

        switch (record.transformType) {
          case 'lookupSql':
            // 以"." 为分界线(注：sql语句中可能会出现 ".")
            const tranLookupVal1 = record.transformConfigInfo.substring(record.transformConfigInfo.indexOf('.') + 1) // 去除第一项后的字符串
            const tranLookupVal2 = tranLookupVal1.substring(tranLookupVal1.indexOf('.') + 1)  // 去除第二项后的字符串
            const tranLookupVal3 = tranLookupVal2.substring(tranLookupVal2.indexOf('.') + 1)  // 去除第三项后的字符串
            const tranLookupVal4 = tranLookupVal3.substring(tranLookupVal3.indexOf('.') + 1)  // 去除第四项后的字符串

            this.flowTransformForm.setFieldsValue({
              lookupSqlType: record.transformConfigInfo.substring(0, record.transformConfigInfo.indexOf('.')),
              transformSinkDataSystem: tranLookupVal1.substring(0, tranLookupVal1.indexOf('.'))
            })
            this.cmLookupSql.doc.setValue(tranLookupVal4)

            setTimeout(() => {
              this.flowTransformForm.setFieldsValue({
                transformSinkNamespace: [
                  tranLookupVal2.substring(0, tranLookupVal2.indexOf('.')),
                  tranLookupVal3.substring(0, tranLookupVal3.indexOf('.'))
                ]
              })
            }, 50)
            break
          case 'sparkSql':
            this.cmSparkSql.doc.setValue(record.transformConfigInfo)
            break
          case 'flinkSql':
            this.cmFlinkSql.doc.setValue(record.transformConfigInfo)
            break
          case 'streamJoinSql':
            // 以"."为分界线
            const tranStreamJoinVal1 = record.transformConfigInfo.substring(record.transformConfigInfo.indexOf('.') + 1) // 去除第一项后的字符串
            const tranStreamJoinVal2 = tranStreamJoinVal1.substring(tranStreamJoinVal1.indexOf('.') + 1)  // 去除第二项后的字符串

            const tempArr = record.transformConfigInfoRequest.split(' ')
            const selectedNsArr = tempArr[4].split(',').map((i) => i.substring(0, i.indexOf('(')))

            this.flowTransformForm.setFieldsValue({
              streamJoinSqlType: record.transformConfigInfo.substring(0, record.transformConfigInfo.indexOf('.')),
              timeout: tranStreamJoinVal1.substring(0, tranStreamJoinVal1.indexOf('.')),
              streamJoinSqlNs: selectedNsArr
            })

            this.loadTransNs()
            this.cmStreamJoinSql.doc.setValue(tranStreamJoinVal2)
            break
          case 'transformClassName':
            this.flowTransformForm.setFieldsValue({ transformClassName: record.transformConfigInfo })
            break
        }
      })
    }
  }

  onJobEditTransform = (record) => (e) => {
    // 加隐藏字段获得 record.transformType
    this.setState({
      jobTransformMode: 'edit',
      jobTransModalVisible: true,
      jobTransValue: record.transformType
    }, () => {
      this.jobTransformForm.setFieldsValue({
        editTransformId: record.order,
        transformation: record.transformType
      })

      const { jobTransValue } = this.state
      if (jobTransValue === 'sparkSql') {
        this.makeJobSqlCodeMirrorInstance()
        this.cmJobSparkSql.doc.setValue(record.transformConfigInfo)
      } else if (jobTransValue === 'transformClassName') {
        this.jobTransformForm.setFieldsValue({ transformClassName: record.transformConfigInfo })
      }
    })
  }

  onAddTransform = (record) => (e) => {
    this.setState({
      transformMode: 'add',
      transformValue: '',
      outputType: 'detail',
      cepPropData: {}
    }, () => {
      this.setState({transformModalVisible: true}, () => {
        this.flowTransformForm.resetFields()
        this.flowTransformForm.setFieldsValue({ editTransformId: record.order })
      })
    })
  }

  onJobAddTransform = (record) => (e) => {
    this.setState({
      jobTransformMode: 'add',
      jobTransModalVisible: true,
      jobTransValue: ''
    }, () => {
      this.jobTransformForm.resetFields()
      this.jobTransformForm.setFieldsValue({
        editTransformId: record.order
      })
    })
  }

  hideTransformModal = () => {
    this.setState({
      transformModalVisible: false,
      transformValue: ''
    })
    this.flowTransformForm.resetFields()
    if (this.cmLookupSql) {
      this.cmLookupSql.doc.setValue('')
    }
    if (this.cmSparkSql) {
      this.cmSparkSql.doc.setValue('')
    }
    if (this.cmStreamJoinSql) {
      this.cmStreamJoinSql.doc.setValue('')
    }
    if (this.cmFlinkSql) {
      this.cmFlinkSql.doc.setValue('')
    }
  }

  hideJobTransModal = () => {
    this.setState({
      jobTransModalVisible: false,
      jobTransValue: ''
    }, () => {
      this.jobTransformForm.resetFields()

      if (this.cmJobSparkSql) {
        this.cmJobSparkSql.doc.setValue('')
      }
    })
  }

  onJobTransModalOk = () => {
    const { jobTransformMode } = this.state

    this.jobTransformForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        let transformConfigInfoString = ''
        let transformConfigInfoRequestString = ''

        let num = -1
        let finalVal = ''

        switch (values.transformation) {
          case 'sparkSql':
            const cmJobSparkSqlVal = this.cmJobSparkSql.doc.getValue()
            if (!cmJobSparkSqlVal) {
              message.error(operateLanguageSql('fillIn'), 3)
            } else {
              const sparkSqlVal = cmJobSparkSqlVal.replace(/(^\s*)|(\s*$)/g, '') // 去掉字符串前后空格

              transformConfigInfoString = sparkSqlVal
              transformConfigInfoRequestString = `spark_sql = ${sparkSqlVal}`

              num = (sparkSqlVal.split(';')).length - 1
              finalVal = sparkSqlVal.substring(sparkSqlVal.length - 1)
            }
            break
          case 'transformClassName':
            const transformClassNameVal = values.transformClassName.replace(/(^\s*)|(\s*$)/g, '')

            num = (transformClassNameVal.split(';')).length - 1
            finalVal = transformClassNameVal.substring(transformClassNameVal.length - 1)

            if (finalVal === ';') {
              transformConfigInfoString = transformClassNameVal.substring(0, transformClassNameVal.length - 1)
              transformConfigInfoRequestString = `custom_class = ${transformClassNameVal}`
            } else {
              transformConfigInfoString = transformClassNameVal
              transformConfigInfoRequestString = `custom_class = ${transformClassNameVal};`
            }
            break
        }

        if (values.transformation === 'transformClassName') {
          if (num > 1) {
            message.warning(operateLanguageSql('className'), 3)
          } else if (num === 1 && finalVal !== ';') {
            message.warning(operateLanguageSql('className'), 3)
          } else if (num === 0 || (num === 1 && finalVal === ';')) {
            this.jobTransSetState(jobTransformMode, values, transformConfigInfoString, transformConfigInfoRequestString)
          }
        } else {
          if (num === 0) {
            message.warning(operateLanguageSql('unique'), 3)
          } else if (num > 1) {
            message.warning(operateLanguageSql('onlyOne'), 3)
          } else if (num === 1 && finalVal !== ';') {
            message.warning(operateLanguageSql('unique'), 3)
          } else if (num === 1 && finalVal === ';') {
            this.jobTransSetState(jobTransformMode, values, transformConfigInfoString, transformConfigInfoRequestString)
          }
        }
      }
    })
  }

  jobTransSetState (jobTransformMode, values, transformConfigInfoString, transformConfigInfoRequestString) {
    // 加隐藏字段 transformType, 获得每次选中的transformation type
    switch (jobTransformMode) {
      case '':
        // 第一次添加数据时
        this.state.jobFormTranTableSource.push({
          transformType: values.transformation,
          order: 1,
          transformConfigInfo: transformConfigInfoString,
          transformConfigInfoRequest: transformConfigInfoRequestString
        })
        break
      case 'edit':
        this.state.jobFormTranTableSource[values.editTransformId - 1] = {
          transformType: values.transformation,
          order: values.editTransformId,
          transformConfigInfo: transformConfigInfoString,
          transformConfigInfoRequest: transformConfigInfoRequestString
        }
        break
      case 'add':
        const tableSourceArr = this.state.jobFormTranTableSource
        // 当前插入的数据
        tableSourceArr.splice(values.editTransformId, 0, {
          transformType: values.transformation,
          order: values.editTransformId + 1,
          transformConfigInfo: transformConfigInfoString,
          transformConfigInfoRequest: transformConfigInfoRequestString
        })
        // 当前数据的下一条开始，order+1
        for (let i = values.editTransformId + 1; i < tableSourceArr.length; i++) {
          tableSourceArr[i].order = tableSourceArr[i].order + 1
        }
        // 重新setState数组
        this.setState({ jobFormTranTableSource: tableSourceArr })
        break
    }
    this.jobTranModalOkSuccess()
  }

  jobTranModalOkSuccess () {
    this.setState({
      jobTranTagClassName: 'hide',
      jobTranTableClassName: '',
      jobTranConnectClass: ''
    })
    this.hideJobTransModal()
  }

  setFlowTransformFormRef = el => {
    this.flowTransformForm = el
  }
  getCepSourceData = (cepDataSource) => {
    this.setState({
      flowPatternCepDataSource: cepDataSource
    })
  }
  /**
   * transformation 弹出框确认
   */
  onTransformModalOk = () => {
    const { transformMode, transformSinkNamespaceArray, flowPatternCepDataSource } = this.state
    this.flowTransformForm.validateFieldsAndScroll({force: true}, (err, values) => {
      if (!err) {
        let transformConfigInfoString = ''
        let tranConfigInfoSqlString = ''
        let transformConfigInfoRequestString = ''
        let pushdownConnectionJson = {}

        let num = -1
        let finalVal = ''

        switch (values.transformation) {
          case 'lookupSql':
            const cmLookupSqlVal = this.cmLookupSql.doc.getValue()
            if (!cmLookupSqlVal) {
              message.error(operateLanguageSql('fillIn'), 3)
            } else {
              // 去掉字符串前后的空格
              const lookupSqlValTemp = cmLookupSqlVal.replace(/(^\s*)|(\s*$)/g, '')
              const lookupSqlVal = preProcessSql(lookupSqlValTemp)

              let lookupSqlTypeOrigin = ''
              if (values.lookupSqlType === 'leftJoin') {
                lookupSqlTypeOrigin = 'left join'
              } else if (values.lookupSqlType === 'union') {
                lookupSqlTypeOrigin = 'union'
              }

              const sysInsDb = [values.transformSinkDataSystem, values.transformSinkNamespace[0], values.transformSinkNamespace[1]].join('.')
              transformConfigInfoString = `${values.lookupSqlType}.${values.transformSinkDataSystem}.${values.transformSinkNamespace.join('.')}.${lookupSqlVal}`
              tranConfigInfoSqlString = lookupSqlVal
              transformConfigInfoRequestString = `pushdown_sql ${lookupSqlTypeOrigin} with ${sysInsDb} = ${lookupSqlVal}`
              const tmp = transformSinkNamespaceArray.find(i => [i.nsSys, i.nsInstance, i.nsDatabase].join('.') === sysInsDb)

              const pushdownConnectJson = tmp.connection_config
                ? {
                  name_space: `${tmp.nsSys}.${tmp.nsInstance}.${tmp.nsDatabase}`,
                  jdbc_url: tmp.conn_url,
                  username: tmp.user,
                  password: tmp.pwd,
                  connection_config: tmp.connection_config
                }
                : {
                  name_space: `${tmp.nsSys}.${tmp.nsInstance}.${tmp.nsDatabase}`,
                  jdbc_url: tmp.conn_url,
                  username: tmp.user,
                  password: tmp.pwd
                }

              pushdownConnectionJson = pushdownConnectJson

              num = (lookupSqlVal.split(';')).length - 1
              finalVal = lookupSqlVal.substring(lookupSqlVal.length - 1)
            }
            break
          case 'sparkSql':
            const cmSparkSqlVal = this.cmSparkSql.doc.getValue()
            if (!cmSparkSqlVal) {
              message.error(operateLanguageSql('fillIn'), 3)
            } else {
              const sparkSqlValTemp = cmSparkSqlVal.replace(/(^\s*)|(\s*$)/g, '')
              const sparkSqlVal = preProcessSql(sparkSqlValTemp)

              transformConfigInfoString = sparkSqlVal
              tranConfigInfoSqlString = sparkSqlVal
              transformConfigInfoRequestString = `spark_sql = ${sparkSqlVal}`
              pushdownConnectionJson = {}

              num = (sparkSqlVal.split(';')).length - 1
              finalVal = sparkSqlVal.substring(sparkSqlVal.length - 1)
            }
            break
          case 'streamJoinSql':
            const cmStreamJoinSqlVal = this.cmStreamJoinSql.doc.getValue()
            if (!cmStreamJoinSqlVal) {
              message.error(operateLanguageSql('fillIn'), 3)
            } else {
              const streamJoinSqlValTemp = cmStreamJoinSqlVal.replace(/(^\s*)|(\s*$)/g, '')
              const streamJoinSqlVal = preProcessSql(streamJoinSqlValTemp)

              let streamJoinSqlTypeOrigin = ''
              if (values.streamJoinSqlType === 'leftJoin') {
                streamJoinSqlTypeOrigin = 'left join'
              } else if (values.streamJoinSqlType === 'innerJoin') {
                streamJoinSqlTypeOrigin = 'inner join'
              }

              const sqlArr = values.streamJoinSqlNs.map((i) => {
                const iTemp = i.replace(/,/g, '.')
                const iFinal = `${iTemp}(${values.timeout})`
                return iFinal
              })

              transformConfigInfoString = `${values.streamJoinSqlType}.${values.timeout}.${streamJoinSqlVal}`
              tranConfigInfoSqlString = streamJoinSqlVal
              transformConfigInfoRequestString = `parquet_sql ${streamJoinSqlTypeOrigin} with ${sqlArr.join(',')} = ${streamJoinSqlVal}`
              pushdownConnectionJson = {}

              num = (streamJoinSqlVal.split(';')).length - 1
              finalVal = streamJoinSqlVal.substring(streamJoinSqlVal.length - 1)
            }
            break
          case 'transformClassName':
            const transformClassNameValTemp = values.transformClassName.replace(/(^\s*)|(\s*$)/g, '')
            const transformClassNameVal = preProcessSql(transformClassNameValTemp)

            pushdownConnectionJson = {}

            num = (transformClassNameVal.split(';')).length - 1
            finalVal = transformClassNameVal.substring(transformClassNameVal.length - 1)

            if (finalVal === ';') {
              tranConfigInfoSqlString = transformClassNameVal.substring(0, transformClassNameVal.length - 1)
              transformConfigInfoString = transformClassNameVal.substring(0, transformClassNameVal.length - 1)
              transformConfigInfoRequestString = `custom_class = ${transformClassNameVal}`
            } else {
              tranConfigInfoSqlString = transformClassNameVal
              transformConfigInfoString = transformClassNameVal
              transformConfigInfoRequestString = `custom_class = ${transformClassNameVal};`
            }
            break
          case 'flinkSql':
            const cmFlinkSql = this.cmFlinkSql.doc.getValue()
            if (!cmFlinkSql) {
              message.error(operateLanguageSql('fillIn'), 3)
            } else {
              const flinkSqlValTemp = cmFlinkSql.replace(/(^\s*)|(\s*$)/g, '')
              const flinkSqlVal = preProcessSql(flinkSqlValTemp)

              transformConfigInfoString = flinkSqlVal
              tranConfigInfoSqlString = flinkSqlVal
              transformConfigInfoRequestString = `flink_sql = ${flinkSqlVal}`
              pushdownConnectionJson = {}

              num = (flinkSqlVal.split(';')).length - 1
              finalVal = flinkSqlVal.substring(flinkSqlVal.length - 1)
            }
            break
          case 'cep':
            let windowTime = values.windowTime == null ? -1 : values.windowTime
            let outputFieldList = []
            if (values.output === 'agg') {
              const rowKeysMap = {}
              Object.keys(values).forEach(v => {
                if (!v.includes('outputAggSelect') && !v.includes('outputAggFieldName') && !v.includes('outputAggRename')) return
                const flag = v.split('_')[1]
                if (!rowKeysMap[flag]) {
                  rowKeysMap[flag] = {}
                }
                if (v.includes('outputAggSelect')) {
                  rowKeysMap[flag].function_type = values[v]
                }
                if (v.includes('outputAggFieldName')) {
                  rowKeysMap[flag].field_name = values[v]
                }
                if (v.includes('outputAggRename')) {
                  rowKeysMap[flag].alias_name = values[v]
                }
              })
              Object.keys(rowKeysMap).forEach(v => {
                outputFieldList.push(rowKeysMap[v])
              })
            } else if (values.output === 'filteredRow') {
              let obj = {
                function_type: values[`outputFilteredRowSelect`],
                field_name: values[`outputFilteredRowSelectFieldName`] || ''
              }
              outputFieldList.push(obj)
            }
            if (flowPatternCepDataSource && flowPatternCepDataSource.length === 0) {
              this.setState({hasPattern: false})
              return
            }
            let partterSeq = flowPatternCepDataSource.slice()
            partterSeq.forEach(v => {
              v.quantifier = v.quantifier ? JSON.parse(v.quantifier) : JSON.stringify({})
            })
            let cep = {
              key_by_fields: values.keyBy,
              max_interval_seconds: windowTime,
              output: {
                type: values.output,
                field_list: outputFieldList
              },
              strategy: values.strategy,
              pattern_seq: partterSeq
            }
            tranConfigInfoSqlString = `${JSON.stringify(cep)};`
            transformConfigInfoRequestString = `cep = ${tranConfigInfoSqlString}`
            transformConfigInfoString = tranConfigInfoSqlString
        }

        if (values.transformation === 'transformClassName') {
          if (num > 1) {
            message.warning(operateLanguageSql('className'), 3)
          } else if (num === 1 && finalVal !== ';') {
            message.warning(operateLanguageSql('className'), 3)
          } else if (num === 0 || (num === 1 && finalVal === ';')) {
            this.flowTransSetState(transformMode, values, transformConfigInfoString, tranConfigInfoSqlString, transformConfigInfoRequestString, pushdownConnectionJson)
          }
        } else {
          if (num === 0) {
            message.warning(operateLanguageSql('unique'), 3)
          } else if (num > 1) {
            message.warning(operateLanguageSql('onlyOne'), 3)
          } else if (num === 1 && finalVal !== ';') {
            message.warning(operateLanguageSql('unique'), 3)
          } else if (num === 1 && finalVal === ';') {
            if (values.transformation === 'lookupSql') {
              const { projectId, pipelineStreamId } = this.state
              // 验证sql存在性
              const requestVal = {
                projectId: projectId,
                streamId: pipelineStreamId,
                sql: {
                  'sql': transformConfigInfoRequestString
                }
              }
              this.props.onLoadLookupSql(requestVal, () => {
                this.flowTransSetState(transformMode, values, transformConfigInfoString, tranConfigInfoSqlString, transformConfigInfoRequestString, pushdownConnectionJson)
              }, (result) => {
                message.error(result, 5)
              })
            } else {
              this.flowTransSetState(transformMode, values, transformConfigInfoString, tranConfigInfoSqlString, transformConfigInfoRequestString, pushdownConnectionJson)
            }
          } else {
            this.flowTransSetState(transformMode, values, transformConfigInfoString, tranConfigInfoSqlString, transformConfigInfoRequestString, pushdownConnectionJson)
          }
        }
      }
    })
  }

  flowTransSetState (transformMode, values, transformConfigInfoString, tranConfigInfoSqlString, transformConfigInfoRequestString, pushdownConnectionJson) {
    // 加隐藏字段 transformType, 获得每次选中的transformation type
    switch (transformMode) {
      case '':
        // 第一次添加数据时
        this.state.flowFormTranTableSource.push({
          transformType: values.transformation,
          order: 1,
          transformConfigInfo: transformConfigInfoString,
          tranConfigInfoSql: tranConfigInfoSqlString,
          transformConfigInfoRequest: transformConfigInfoRequestString,
          pushdownConnection: pushdownConnectionJson
        })

        this.setState({
          dataframeShowSelected: 'hide'
        }, () => {
          this.workbenchFlowForm.setFieldsValue({
            dataframeShow: 'false',
            dataframeShowNum: 10
          })
        })
        break
      case 'edit':
        this.state.flowFormTranTableSource[values.editTransformId - 1] = {
          transformType: values.transformation,
          order: values.editTransformId,
          transformConfigInfo: transformConfigInfoString,
          tranConfigInfoSql: tranConfigInfoSqlString,
          transformConfigInfoRequest: transformConfigInfoRequestString,
          pushdownConnection: pushdownConnectionJson
        }
        break
      case 'add':
        const tableSourceArr = this.state.flowFormTranTableSource
        // 当前插入的数据
        tableSourceArr.splice(values.editTransformId, 0, {
          transformType: values.transformation,
          order: values.editTransformId + 1,
          transformConfigInfo: transformConfigInfoString,
          tranConfigInfoSql: tranConfigInfoSqlString,
          transformConfigInfoRequest: transformConfigInfoRequestString,
          pushdownConnection: pushdownConnectionJson
        })
        // 当前数据的下一条开始，order+1
        for (let i = values.editTransformId + 1; i < tableSourceArr.length; i++) {
          tableSourceArr[i].order = tableSourceArr[i].order + 1
        }
        // 重新setState数组
        this.setState({ flowFormTranTableSource: tableSourceArr })
        break
    }
    this.tranModalOkSuccess()
  }

  tranModalOkSuccess () {
    this.setState({
      transformTagClassName: 'hide',
      transformTableClassName: '',
      transConnectClass: ''
    })
    this.hideTransformModal()
  }

  onDeleteSingleTransform = (record) => (e) => {
    const tableSourceArr = this.state.flowFormTranTableSource
    if (tableSourceArr.length === 1) {
      this.setState({
        transformTagClassName: '',
        transformTableClassName: 'hide',
        transConnectClass: 'hide',
        fieldSelected: 'hide',
        etpStrategyCheck: false,
        etpStrategyRequestValue: {},
        etpStrategyConfirmValue: '',
        dataframeShowSelected: 'hide',
        transformMode: '',
        transformValue: '',
        dataframeShowNumValue: ''
      }, () => {
        this.workbenchFlowForm.setFieldsValue({
          dataframeShow: 'false',
          dataframeShowNum: 10
        })
      })
    }

    tableSourceArr.splice(record.order - 1, 1)  // 删除当条数据

    // 当条下的数据 order-1
    for (let i = record.order - 1; i < tableSourceArr.length; i++) {
      tableSourceArr[i].order = tableSourceArr[i].order - 1
    }
    this.setState({ flowFormTranTableSource: tableSourceArr })
  }

  onJobDeleteSingleTransform = (record) => (e) => {
    const tableSourceArr = this.state.jobFormTranTableSource
    if (tableSourceArr.length === 1) {
      this.setState({
        jobTranTagClassName: '',
        jobTranTableClassName: 'hide',
        jobTranConnectClass: 'hide',
        fieldSelected: 'hide',
        jobTransformMode: '',
        jobTransValue: ''
      })
    }

    tableSourceArr.splice(record.order - 1, 1)  // 删除当条数据

    // 当条下的数据 order-1
    for (let i = record.order - 1; i < tableSourceArr.length; i++) {
      tableSourceArr[i].order = tableSourceArr[i].order - 1
    }
    this.setState({ jobFormTranTableSource: tableSourceArr })
  }

  onUpTransform = (record) => (e) => {
    const tableSourceArr = this.state.flowFormTranTableSource

    // 当前数据
    let currentInfo = [{
      transformType: record.transformType,
      order: record.order,
      transformConfigInfo: record.transformConfigInfo,
      tranConfigInfoSql: record.tranConfigInfoSql,
      transformConfigInfoRequest: record.transformConfigInfoRequest,
      pushdownConnection: record.pushdownConnection
    }]

    // 上一条数据
    let beforeArr = tableSourceArr.slice(record.order - 2, record.order - 1)

    currentInfo[0] = {
      transformType: beforeArr[0].transformType,
      order: record.order,
      transformConfigInfo: beforeArr[0].transformConfigInfo,
      tranConfigInfoSql: beforeArr[0].tranConfigInfoSql,
      transformConfigInfoRequest: beforeArr[0].transformConfigInfoRequest,
      pushdownConnection: beforeArr[0].pushdownConnection
    }

    beforeArr[0] = {
      transformType: record.transformType,
      order: record.order - 1,
      transformConfigInfo: record.transformConfigInfo,
      tranConfigInfoSql: record.tranConfigInfoSql,
      transformConfigInfoRequest: record.transformConfigInfoRequest,
      pushdownConnection: record.pushdownConnection
    }

    tableSourceArr.splice(record.order - 2, 2, beforeArr[0], currentInfo[0])

    this.setState({ flowFormTranTableSource: tableSourceArr })
  }

  onJobUpTransform = (record) => (e) => {
    const tableSourceArr = this.state.jobFormTranTableSource

    // 当前数据
    let currentInfo = [{
      transformType: record.transformType,
      order: record.order,
      transformConfigInfo: record.transformConfigInfo,
      tranConfigInfoSql: record.tranConfigInfoSql,
      transformConfigInfoRequest: record.transformConfigInfoRequest
    }]

    // 上一条数据
    let beforeArr = tableSourceArr.slice(record.order - 2, record.order - 1)

    currentInfo[0] = {
      transformType: beforeArr[0].transformType,
      order: record.order,
      transformConfigInfo: beforeArr[0].transformConfigInfo,
      tranConfigInfoSql: beforeArr[0].tranConfigInfoSql,
      transformConfigInfoRequest: beforeArr[0].transformConfigInfoRequest
    }

    beforeArr[0] = {
      transformType: record.transformType,
      order: record.order - 1,
      transformConfigInfo: record.transformConfigInfo,
      tranConfigInfoSql: record.tranConfigInfoSql,
      transformConfigInfoRequest: record.transformConfigInfoRequest
    }

    tableSourceArr.splice(record.order - 2, 2, beforeArr[0], currentInfo[0])

    this.setState({ jobFormTranTableSource: tableSourceArr })
  }

  onDownTransform = (record) => (e) => {
    const tableSourceArr = this.state.flowFormTranTableSource

    // 当前数据
    let currentInfo = [{
      transformType: record.transformType,
      order: record.order,
      transformConfigInfo: record.transformConfigInfo,
      tranConfigInfoSql: record.tranConfigInfoSql,
      transformConfigInfoRequest: record.transformConfigInfoRequest,
      pushdownConnection: record.pushdownConnection
    }]

    // 下一条数据
    let afterArr = tableSourceArr.slice(record.order, record.order + 1)

    currentInfo[0] = {
      transformType: afterArr[0].transformType,
      order: record.order,
      transformConfigInfo: afterArr[0].transformConfigInfo,
      tranConfigInfoSql: afterArr[0].tranConfigInfoSql,
      transformConfigInfoRequest: afterArr[0].transformConfigInfoRequest,
      pushdownConnection: afterArr[0].pushdownConnection
    }

    afterArr[0] = {
      transformType: record.transformType,
      order: record.order + 1,
      transformConfigInfo: record.transformConfigInfo,
      tranConfigInfoSql: record.tranConfigInfoSql,
      transformConfigInfoRequest: record.transformConfigInfoRequest,
      pushdownConnection: record.pushdownConnection
    }

    tableSourceArr.splice(record.order - 1, 2, currentInfo[0], afterArr[0])

    this.setState({ flowFormTranTableSource: tableSourceArr })
  }

  onJobDownTransform = (record) => (e) => {
    const tableSourceArr = this.state.jobFormTranTableSource

    // 当前数据
    let currentInfo = [{
      transformType: record.transformType,
      order: record.order,
      transformConfigInfo: record.transformConfigInfo,
      transformConfigInfoRequest: record.transformConfigInfoRequest
    }]

    // 下一条数据
    let afterArr = tableSourceArr.slice(record.order, record.order + 1)

    currentInfo[0] = {
      transformType: afterArr[0].transformType,
      order: record.order,
      transformConfigInfo: afterArr[0].transformConfigInfo,
      transformConfigInfoRequest: afterArr[0].transformConfigInfoRequest
    }

    afterArr[0] = {
      transformType: record.transformType,
      order: record.order + 1,
      transformConfigInfo: record.transformConfigInfo,
      transformConfigInfoRequest: record.transformConfigInfoRequest
    }

    tableSourceArr.splice(record.order - 1, 2, currentInfo[0], afterArr[0])
    this.setState({ jobFormTranTableSource: tableSourceArr })
  }

  // Flow ETP Strategy Modal
  onShowEtpStrategyModal = () => {
    const { etpStrategyCheck, etpStrategyResponseValue } = this.state

    this.setState({
      etpStrategyModalVisible: true
    }, () => {
      if (etpStrategyCheck) {
        const { check_columns, check_rule, rule_mode, rule_params, against_action } = etpStrategyResponseValue
        this.flowEtpStrategyForm.setFieldsValue({
          checkColumns: check_columns,
          checkRule: check_rule,
          ruleMode: rule_mode,
          ruleParams: rule_params,
          againstAction: against_action
        })
      } else {
        this.flowEtpStrategyForm.resetFields()
        this.setState({
          etpStrategyRequestValue: {}
        })
      }
    })
  }

  hideEtpStrategyModal = () => this.setState({ etpStrategyModalVisible: false })

  onEtpStrategyModalOk = () => {
    this.flowEtpStrategyForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const { checkColumns, checkRule, ruleMode, ruleParams, againstAction } = values
        const valueJson = {
          check_columns: checkColumns,
          check_rule: checkRule,
          rule_mode: ruleMode,
          rule_params: ruleParams,
          against_action: againstAction
        }

        this.setState({
          etpStrategyCheck: true,
          etpStrategyRequestValue: {'validity': valueJson},
          etpStrategyConfirmValue: JSON.stringify(valueJson)
        })
        this.hideEtpStrategyModal()
      }
    })
  }

  // Flow Sink Config Modal
  onShowSinkConfigModal = () => {
    this.setState({
      sinkConfigModalVisible: true
    }, () => {
      if (!this.cm) {
        this.cm = CodeMirror.fromTextArea(this.sinkConfigInput, {
          lineNumbers: true,
          matchBrackets: true,
          autoCloseBrackets: true,
          mode: 'application/ld+json',
          lineWrapping: true
        })
        this.cm.setSize('100%', '256px')
      }
      const { flowMode, sinkConfigCopy } = this.state
      flowMode === 'copy'
        ? this.cm.doc.setValue(sinkConfigCopy)
        : this.cm.doc.setValue(this.workbenchFlowForm.getFieldValue('sinkConfig') || '')
    })
  }

  // Flow Transformation Config Modal
  onShowSpecialConfigModal = () => {
    this.setState({
      flowSpecialConfigModalVisible: true
    }, () => {
      if (!this.cmFlowSpecial) {
        this.cmFlowSpecial = CodeMirror.fromTextArea(this.flowSpecialConfigInput, {
          lineNumbers: true,
          matchBrackets: true,
          autoCloseBrackets: true,
          mode: 'application/ld+json',
          lineWrapping: true
        })
        this.cmFlowSpecial.setSize('100%', '256px')
      }
      this.cmFlowSpecial.doc.setValue(this.workbenchFlowForm.getFieldValue('flowSpecialConfig') || '')
    })
  }

  // Job Transformation Config Modal
  onShowJobSpecialConfigModal = () => {
    this.setState({
      jobSpecialConfigModalVisible: true
    }, () => {
      if (!this.cmJobSpecial) {
        this.cmJobSpecial = CodeMirror.fromTextArea(this.jobSpecialConfigInput, {
          lineNumbers: true,
          matchBrackets: true,
          autoCloseBrackets: true,
          mode: 'application/ld+json',
          lineWrapping: true
        })
        this.cmJobSpecial.setSize('100%', '256px')
      }
      this.cmJobSpecial.doc.setValue(this.workbenchJobForm.getFieldValue('jobSpecialConfig') || '')
    })
  }

  hideSinkConfigModal = () => this.setState({ sinkConfigModalVisible: false })
  hideFlowSpecialConfigModal = () => this.setState({ flowSpecialConfigModalVisible: false })
  hideJobSpecialConfigModal = () => this.setState({ jobSpecialConfigModalVisible: false })

  onSinkConfigModalOk = () => {
    const cmValue = this.cm.doc.getValue()
    if (isJSON(cmValue)) {
      this.workbenchFlowForm.setFieldsValue({
        sinkConfig: cmValue
      })
      this.setState({
        sinkConfigCopy: cmValue
      })
      this.hideSinkConfigModal()
    } else {
      message.error(operateLanguageSinkConfig('Sink'), 3)
    }
  }

  onFlowSpecialConfigModalOk = () => {
    const cmValue = this.cmFlowSpecial.doc.getValue()
    if (isJSON(cmValue)) {
      this.workbenchFlowForm.setFieldsValue({ flowSpecialConfig: cmValue })
      this.hideFlowSpecialConfigModal()
    } else {
      message.error(operateLanguageSinkConfig('Transformation'), 3)
    }
  }

  onJobSpecialConfigModalOk = () => {
    const cmValue = this.cmJobSpecial.doc.getValue()
    if (isJSON(cmValue)) {
      this.workbenchJobForm.setFieldsValue({ jobSpecialConfig: cmValue })
      this.hideJobSpecialConfigModal()
    } else {
      message.error(operateLanguageSinkConfig('Transformation'), 3)
    }
  }

  // Job Sink Config Modal
  onShowJobSinkConfigModal = () => {
    this.setState({
      jobSinkConfigModalVisible: true
    }, () => {
      if (!this.cmJob) {
        this.cmJob = CodeMirror.fromTextArea(this.jobSinkConfigInput, {
          lineNumbers: true,
          matchBrackets: true,
          autoCloseBrackets: true,
          mode: 'application/ld+json',
          lineWrapping: true
        })
        this.cmJob.setSize('100%', '256px')
      }
      this.cmJob.doc.setValue(this.workbenchJobForm.getFieldValue('sinkConfig') || '')
    })
  }

  hideJobSinkConfigModal = () => this.setState({ jobSinkConfigModalVisible: false })

  onJobSinkConfigModalOk = () => {
    const cmValue = this.cmJob.doc.getValue()
    if (isJSON(cmValue)) {
      this.workbenchJobForm.setFieldsValue({ sinkConfig: this.cmJob.doc.getValue() })
      this.hideJobSinkConfigModal()
    } else {
      message.error(operateLanguageSinkConfig('Sink'), 3)
    }
  }

  loadConfig (type) {
    // let jvm = new Promise((resolve) => {
    //   this.props.onLoadStreamConfigJvm((result) => {
    //     resolve(result)
    //   })
    // })
    // let spark = new Promise((resolve) => {
    //   this.props.onLoadStreamConfigSpark((result) => {
    //     resolve(result)
    //   })
    // })
    // return [jvm, spark]
    let con = new Promise((resolve) => {
      this.props.onLoadStreamConfigs(type, result => {
        resolve(result)
      })
    })
    return [con]
  }

  clearSinkData = () => {
    this.setState({backfillTopicValueProp: '', backfillSinkNsValue: ''})
  }

  showAddJobWorkbench = () => {
    this.workbenchJobForm.resetFields()
    this.setState({
      jobMode: 'add',
      formStep: 0,
      sparkConfigCheck: false,
      jobFormTranTableSource: [],
      jobTranTagClassName: '',
      jobTranTableClassName: 'hide',
      fieldSelected: 'hide',
      resultFieldsValue: 'all',
      backfillSinkNsValue: '',
      jobDiffType: 'default',
      backfillTopicValueProp: '',
      sourceNsVersionList: []
    }, () => {
      this.workbenchJobForm.setFieldsValue({
        // type: 'default',
        resultFields: 'all'
      })
    })
    Promise.all(this.loadConfig('spark')).then((values) => {
      // NOTE: job => sparkConfig? 结构 待修改
      const { driverCores, driverMemory, executorNums, perExecutorMemory, perExecutorCores, durations, maxRecords, partitions } = values[0].spark
      const startConfigJson = {
        driverCores,
        driverMemory,
        executorNums,
        perExecutorMemory,
        perExecutorCores,
        durations,
        maxRecords,
        partitions
      }

      this.setState({
        jobSparkConfigValues: {
          sparkConfig: {
            JVMDriverConfig: values[0].JVMDriverConfig,
            JVMExecutorConfig: values[0].JVMExecutorConfig,
            othersConfig: values[0].othersConfig
          },
          startConfig: `${JSON.stringify(startConfigJson)}`
        }
      })
    })
  }

  /**
   * Dag 图
   * */
  // showStreamDagModal = () => {
  //   this.setState({
  //     streamDagModalShow: ''
  //   })
  // }
  //
  // hideStreamDagModal = () => {
  //   this.setState({
  //     streamDagModalShow: 'hide'
  //   })
  // }
  //
  // showFlowDagModal = () => {
  //   this.setState({
  //     flowDagModalShow: ''
  //   })
  // }
  //
  // hideFlowDagModal = () => {
  //   this.setState({
  //     flowDagModalShow: 'hide'
  //   })
  // }

  render () {
    const {
      flowMode, projectId, streamMode, jobMode, formStep, isWormhole, flowClassHide,
      flowFormTranTableSource, jobFormTranTableSource, namespaceClassHide, userClassHide,
      udfClassHide, flowSpecialConfigModalVisible, transformModalVisible, sinkConfigModalVisible,
      etpStrategyModalVisible, streamConfigModalVisible, sparkConfigModalVisible,
      jobSinkConfigModalVisible, jobTransModalVisible, jobSpecialConfigModalVisible, pipelineStreamId, cepPropData, transformMode, hasPattern,
      outputType, outputFieldList
    } = this.state
    const { streams, projectNamespaces, streamSubmitLoading, locale } = this.props

    const sidebarPrefixes = {
      add: locale === 'zh' ? '新增' : 'Create',
      edit: locale === 'zh' ? '修改' : 'Modify',
      copy: locale === 'zh' ? '复制' : 'Copy'
    }

    const stepButtons = this.generateStepButtons()

    const paneHeight = document.documentElement.clientHeight - 64 - 50 - 48

    return (
      <div className="workbench-main-body">
        <Helmet title="Workbench" />
        <Spin spinning={this.state.globalLoading}>
          <Tabs
            defaultActiveKey="flow"
            animated={false}
            activeKey={this.props.activeKey}
            className="ri-tabs"
            onChange={this.changeTag}
        >
            {/* Flow Panel */}
            <TabPane tab="Flow" key="flow" forceRender style={{height: `${paneHeight}px`}}>
              <div className="ri-workbench" style={{height: `${paneHeight}px`}}>
                <Flow
                  className={flowMode ? 'op-mode' : ''}
                  onShowAddFlow={this.showAddFlowWorkbench}
                  onShowEditFlow={this.showEditFlowWorkbench}
                  onShowCopyFlow={this.showCopyFlowWorkbench}
                  projectIdGeted={projectId}
                  flowClassHide={flowClassHide}
              />
                <div className={`ri-workbench-sidebar ri-common-block ${flowMode ? 'op-mode' : ''}`}>
                  <h3 className="ri-common-block-title">
                    {`${sidebarPrefixes[flowMode] || ''} Flow`}
                  </h3>
                  <div className="ri-common-block-tools">
                    <Button icon="arrow-left" type="ghost" onClick={this.hideFlowWorkbench}></Button>
                  </div>
                  <div className="ri-workbench-sidebar-container">
                    <Steps current={formStep}>
                      <Step title="Pipeline" />
                      <Step title="Transformation" />
                      <Step title="Confirmation" />
                    </Steps>
                    <WorkbenchFlowForm
                      step={formStep}
                      sourceNamespaces={projectNamespaces || []}
                      sinkNamespaces={projectNamespaces || []}
                      streams={streams || []}
                      flowMode={flowMode}
                      projectIdGeted={projectId}
                      streamId={pipelineStreamId}

                      onShowTransformModal={this.onShowTransformModal}
                      onShowEtpStrategyModal={this.onShowEtpStrategyModal}
                      onShowSinkConfigModal={this.onShowSinkConfigModal}
                      onShowSpecialConfigModal={this.onShowSpecialConfigModal}

                      transformTableSource={flowFormTranTableSource}
                      transformTagClassName={this.state.transformTagClassName}
                      transformTableClassName={this.state.transformTableClassName}
                      transConnectClass={this.state.transConnectClass}
                      onEditTransform={this.onEditTransform}
                      onAddTransform={this.onAddTransform}
                      onDeleteSingleTransform={this.onDeleteSingleTransform}
                      onUpTransform={this.onUpTransform}
                      onDownTransform={this.onDownTransform}

                      step2SinkNamespace={this.state.step2SinkNamespace}
                      step2SourceNamespace={this.state.step2SourceNamespace}

                      etpStrategyCheck={this.state.etpStrategyCheck}
                      initResultFieldClass={this.initResultFieldClass}
                      initDataShowClass={this.initDataShowClass}
                      fieldSelected={this.state.fieldSelected}
                      dataframeShowSelected={this.state.dataframeShowSelected}

                      onInitStreamTypeSelect={this.onInitStreamTypeSelect}
                      onInitStreamNameSelect={this.onInitStreamNameSelect}
                      selectStreamKafkaTopicValue={this.state.selectStreamKafkaTopicValue}
                      onInitSinkTypeNamespace={this.onInitSinkTypeNamespace}
                      routingSinkTypeNsData={this.state.routingSinkTypeNsData}

                      resultFieldsValue={this.state.resultFieldsValue}
                      dataframeShowNumValue={this.state.dataframeShowNumValue}
                      etpStrategyConfirmValue={this.state.etpStrategyConfirmValue}
                      transConfigConfirmValue={this.state.transConfigConfirmValue}
                      transformTableConfirmValue={this.state.transformTableConfirmValue}
                      timeCharacteristic={this.state.timeCharacteristic}

                      transformTableRequestValue={this.state.transformTableRequestValue}
                      streamDiffType={this.state.streamDiffType}
                      hdfsSinkNsValue={this.state.hdfsSinkNsValue}
                      routingSourceNsValue={this.state.routingSourceNsValue}
                      routingSinkNsValue={this.state.routingSinkNsValue}
                      initialDefaultCascader={this.initialDefaultCascader}
                      initialHdfslogCascader={this.initialHdfslogCascader}
                      initialRoutingCascader={this.initialRoutingCascader}
                      initialRoutingSinkCascader={this.initialRoutingSinkCascader}

                      flowKafkaInstanceValue={this.state.flowKafkaInstanceValue}
                      flowKafkaTopicValue={this.state.flowKafkaTopicValue}
                      sinkConfigCopy={this.state.sinkConfigCopy}
                      flowSourceNsSys={this.state.flowSourceNsSys}
                      emitDataSystem={this.getDataSystem}
                      changeStreamType={this.changeStreamType}
                      flowSubPanelKey={this.state.flowSubPanelKey}
                      emitFlowFunctionType={this.getFlowFunctionType}

                      ref={(f) => { this.workbenchFlowForm = f }}
                  />
                    {/* Flow Transform Modal */}
                    <Modal
                      title="Transformation"
                      okText="保存"
                      wrapClassName="transform-form-style"
                      visible={transformModalVisible}
                      onOk={this.onTransformModalOk}
                      onCancel={this.hideTransformModal}>
                      <FlowTransformForm
                        ref={this.setFlowTransformFormRef}
                        projectIdGeted={projectId}
                        tabPanelKey={this.state.tabPanelKey}
                        flowTransNsData={this.state.flowTransNsData}
                        sinkNamespaces={projectNamespaces || []}
                        onInitTransformValue={this.onInitTransformValue}
                        transformValue={this.state.transformValue}
                        step2SinkNamespace={this.state.step2SinkNamespace}
                        step2SourceNamespace={this.state.step2SourceNamespace}
                        onInitTransformSinkTypeNamespace={this.onInitTransformSinkTypeNamespace}
                        transformSinkTypeNamespaceData={this.state.transformSinkTypeNamespaceData}
                        flowSubPanelKey={this.state.flowSubPanelKey}
                        emitCepSourceData={this.getCepSourceData}
                        cepPropData={cepPropData}
                        transformModalVisible={transformModalVisible}
                        transformMode={transformMode}
                        hasPattern={hasPattern}
                        outputType={outputType}
                        outputFieldList={outputFieldList}
                    />
                    </Modal>
                    {/* Flow Sink Config Modal */}
                    <Modal
                      title="Sink Config"
                      okText="保存"
                      wrapClassName="ant-modal-large"
                      visible={sinkConfigModalVisible}
                      onOk={this.onSinkConfigModalOk}
                      onCancel={this.hideSinkConfigModal}>
                      <div>
                        <h4 className="sink-config-modal-class">{this.state.sinkConfigMsg}</h4>
                        <textarea
                          ref={(f) => { this.sinkConfigInput = f }}
                          placeholder="Paste your Sink Config JSON here."
                          className="ant-input ant-input-extra"
                          rows="5">
                        </textarea>
                      </div>
                    </Modal>
                    {/* Flow Transformation Config Modal */}
                    <Modal
                      title="Transformation Config"
                      okText="保存"
                      wrapClassName="ant-modal-large"
                      visible={flowSpecialConfigModalVisible}
                      onOk={this.onFlowSpecialConfigModalOk}
                      onCancel={this.hideFlowSpecialConfigModal}>
                      <div>
                        <textarea
                          ref={(f) => { this.flowSpecialConfigInput = f }}
                          placeholder="Paste your Transformation Config JSON here."
                          className="ant-input ant-input-extra"
                          rows="5">
                        </textarea>
                      </div>
                    </Modal>
                    {/* ETP Strategy Modal */}
                    <Modal
                      title="Event Time Processing Strategy"
                      okText="保存"
                      visible={etpStrategyModalVisible}
                      onOk={this.onEtpStrategyModalOk}
                      onCancel={this.hideEtpStrategyModal}>
                      <FlowEtpStrategyForm
                        ref={(f) => { this.flowEtpStrategyForm = f }}
                    />
                    </Modal>
                    {stepButtons}
                  </div>
                </div>
                {/* <div className={`ri-workbench-graph ri-common-block ${flowMode ? 'op-mode' : ''}`}>
                <h3 className="ri-common-block-title">Flow DAG</h3>
                <div className="ri-common-block-tools">
                   <Button icon="arrows-alt" type="ghost" onClick={this.showFlowDagModal}></Button>
                </div>
              </div> */}
                {/* <div className={this.state.flowDagModalShow}>
                <div className="dag-madal-mask"></div>
                <div className="dag-modal">
                  <Button icon="shrink" type="ghost" className="hide-dag-modal" onClick={this.hideFlowDagModal}></Button>
                  <FlowDagModal />
                </div>
              </div> */}
              </div>
            </TabPane>
            {/* Stream Panel */}
            <TabPane tab="Stream" key="stream" style={{height: `${paneHeight}px`}}>
              <div className="ri-workbench" style={{height: `${paneHeight}px`}}>
                <Manager
                  className={streamMode ? 'streamAndSink-op-mode' : ''}
                  projectIdGeted={projectId}
                  onShowAddStream={this.showAddStreamWorkbench}
                  onShowEditStream={this.showEditStreamWorkbench}
                  streamClassHide={this.state.streamClassHide}
                />
                <div className={`ri-workbench-sidebar ri-common-block ${streamMode ? 'streamAndSink-op-mode' : ''}`}>
                  <h3 className="ri-common-block-title">
                    {`${sidebarPrefixes[streamMode] || ''} Stream`}
                  </h3>
                  <div className="ri-common-block-tools">
                    <Button icon="arrow-left" type="ghost" onClick={this.hideStreamWorkbench}></Button>
                  </div>
                  <div className="ri-workbench-sidebar-container">
                    <WorkbenchStreamForm
                      isWormhole={isWormhole}
                      streamMode={this.state.streamMode}
                      projectId={projectId}
                      kafkaValues={this.state.kafkaValues}
                      streamSubPanelKey={this.state.streamSubPanelKey}

                      onShowConfigModal={this.onShowConfigModal}
                      streamConfigCheck={this.state.streamConfigCheck}
                      topicEditValues={this.state.topicEditValues}
                      changeStreamType={this.changeStreamType}

                      ref={(f) => { this.workbenchStreamForm = f }}
                    />
                    {/* Config Modal */}
                    <Modal
                      title="Configs"
                      okText="保存"
                      wrapClassName="ant-modal-large"
                      visible={streamConfigModalVisible}
                      onOk={this.onConfigModalOk}
                      onCancel={this.hideConfigModal}>
                      <StreamConfigForm
                        tabPanelKey={this.state.tabPanelKey}
                        streamSubPanelKey={this.state.streamSubPanelKey}
                        ref={(f) => { this.streamConfigForm = f }}
                      />
                    </Modal>
                    <div className="ri-workbench-step-button-area">
                      <Button
                        type="primary"
                        className="next"
                        onClick={this.submitStreamForm}
                        loading={streamSubmitLoading}
                      >
                        <FormattedMessage {...messages.workbenchSave} />
                      </Button>
                    </div>
                  </div>
                </div>
                {/* <div className={`ri-workbench-graph ri-common-block ${streamMode ? 'op-mode' : ''}`}>
                  <h3 className="ri-common-block-title">Stream DAG</h3>
                  <div className="ri-common-block-tools">
                    <Button icon="arrows-alt" type="ghost" onClick={this.showStreamDagModal}></Button>
                  </div>
                </div> */}
                {/* <div className={this.state.streamDagModalShow}>
                  <div className="dag-madal-mask"></div>
                  <div className="dag-modal">
                    <Button icon="shrink" type="ghost" className="hide-dag-modal" onClick={this.hideStreamDagModal}></Button>
                    <StreamDagModal />
                  </div>
                </div> */}
              </div>
            </TabPane>
            {/* Job Panel */}
            <TabPane tab="Job" key="job" style={{height: `${paneHeight}px`}}>
              <div className="ri-workbench" style={{height: `${paneHeight}px`}}>
                <Job
                  className={jobMode ? 'op-mode' : ''}
                  onShowAddJob={this.showAddJobWorkbench}
                  onShowEditJob={this.showEditJobWorkbench}
                  projectIdGeted={projectId}
                  jobClassHide={this.state.jobClassHide}
                />
                <div className={`ri-workbench-sidebar ri-common-block ${jobMode ? 'op-mode' : ''}`}>
                  <h3 className="ri-common-block-title">
                    {`${sidebarPrefixes[jobMode] || ''} Job`}
                  </h3>
                  <div className="ri-common-block-tools">
                    <Button icon="arrow-left" type="ghost" onClick={this.hideJobWorkbench}></Button>
                  </div>
                  <div className="ri-workbench-sidebar-container">
                    <Steps current={formStep}>
                      <Step title="Pipeline" />
                      <Step title="Transformation" />
                      <Step title="Confirmation" />
                    </Steps>
                    <WorkbenchJobForm
                      step={formStep}
                      projectIdGeted={projectId}
                      jobMode={this.state.jobMode}
                      sparkConfigCheck={this.state.sparkConfigCheck}
                      onShowSparkConfigModal={this.onShowSparkConfigModal}
                      fieldSelected={this.state.fieldSelected}
                      initResultFieldClass={this.initResultFieldClass}
                      onShowJobSinkConfigModal={this.onShowJobSinkConfigModal}
                      onInitJobNameValue={this.onInitJobNameValue}
                      onInitJobSinkNs={this.onInitJobSinkNs}
                      jobResultFieldsValue={this.state.jobResultFieldsValue}
                      initStartTS={this.initStartTS}
                      initEndTS={this.initEndTS}
                      onShowJobSpecialConfigModal={this.onShowJobSpecialConfigModal}

                      jobStepSourceNs={this.state.jobStepSourceNs}
                      jobStepSinkNs={this.state.jobStepSinkNs}

                      onShowJobTransModal={this.onShowJobTransModal}
                      jobTransTableSource={jobFormTranTableSource}
                      jobTranTagClassName={this.state.jobTranTagClassName}
                      jobTranTableClassName={this.state.jobTranTableClassName}
                      jobTranConfigConfirmValue={this.state.jobTranConfigConfirmValue}

                      onEditTransform={this.onJobEditTransform}
                      onJobAddTransform={this.onJobAddTransform}
                      onDeleteSingleTransform={this.onJobDeleteSingleTransform}
                      onUpTransform={this.onJobUpTransform}
                      onDownTransform={this.onJobDownTransform}
                      jobTranTableConfirmValue={this.state.jobTranTableConfirmValue}
                      initialBackfillCascader={this.initialBackfillCascader}
                      backfillSinkNsValue={this.state.backfillSinkNsValue}
                      onInitJobTypeSelect={this.onInitJobTypeSelect}
                      jobDiffType={this.state.jobDiffType}
                      backfillTopicValueProp={this.state.backfillTopicValueProp}
                      clearSinkData={this.clearSinkData}
                      jobSourceNsSys={this.state.jobSourceNsSys}
                      sourceNsVersionList={this.state.sourceNsVersionList}
                      initialSourceNsVersion={this.initialSourceNsVersion}
                      ref={(f) => { this.workbenchJobForm = f }}
                    />
                    <Modal
                      title="Configs"
                      okText="保存"
                      wrapClassName="ant-modal-large"
                      visible={sparkConfigModalVisible}
                      onOk={this.onSparkConfigModalOk}
                      onCancel={this.hideSparkConfigModal}>
                      <StreamConfigForm
                        tabPanelKey={this.state.tabPanelKey}
                        ref={(f) => { this.streamConfigForm = f }}
                      />
                    </Modal>
                    {/* Job Sink Config Modal */}
                    <Modal
                      title="Sink Config"
                      okText="保存"
                      wrapClassName="ant-modal-large"
                      visible={jobSinkConfigModalVisible}
                      onOk={this.onJobSinkConfigModalOk}
                      onCancel={this.hideJobSinkConfigModal}>
                      <div>
                        <h4 className="sink-config-modal-class">{this.state.jobSinkConfigMsg}</h4>
                        <textarea
                          ref={(f) => { this.jobSinkConfigInput = f }}
                          placeholder="Paste your Sink Config JSON here."
                          className="ant-input ant-input-extra"
                          rows="5">
                        </textarea>
                      </div>
                    </Modal>
                    {/* Job Transform Modal */}
                    <Modal
                      title="Transformation"
                      okText="保存"
                      wrapClassName="job-transform-form-style"
                      visible={jobTransModalVisible}
                      onOk={this.onJobTransModalOk}
                      onCancel={this.hideJobTransModal}>
                      <JobTransformForm
                        ref={(f) => { this.jobTransformForm = f }}
                        projectIdGeted={projectId}
                        tabPanelKey={this.state.tabPanelKey}
                        onInitJobTransValue={this.onInitJobTransValue}
                        transformValue={this.state.jobTransValue}
                        step2SinkNamespace={this.state.jobStepSinkNs}
                        step2SourceNamespace={this.state.jobStepSourceNs}
                      />
                    </Modal>
                    {/* Job Transformation Config Modal */}
                    <Modal
                      title="Transformation Config"
                      okText="保存"
                      wrapClassName="ant-modal-large"
                      visible={jobSpecialConfigModalVisible}
                      onOk={this.onJobSpecialConfigModalOk}
                      onCancel={this.hideJobSpecialConfigModal}>
                      <div>
                        <textarea
                          ref={(f) => { this.jobSpecialConfigInput = f }}
                          placeholder="Paste your Transformation Config JSON here."
                          className="ant-input ant-input-extra"
                          rows="5">
                        </textarea>
                      </div>
                    </Modal>
                    {stepButtons}
                  </div>
                </div>
              </div>
            </TabPane>
            {/* Namespace Panel */}
            <TabPane tab="Namespace" key="namespace" style={{height: `${paneHeight}px`}}>
              <div className="ri-workbench" style={{height: `${paneHeight}px`}} >
                <Namespace
                  projectIdGeted={projectId}
                  namespaceClassHide={namespaceClassHide}
                />
              </div>
            </TabPane>
            {/* User Panel */}
            <TabPane tab="User" key="user" style={{height: `${paneHeight}px`}}>
              <div className="ri-workbench" style={{height: `${paneHeight}px`}}>
                <User
                  projectIdGeted={projectId}
                  userClassHide={userClassHide}
                />
              </div>
            </TabPane>
            {/* Udf Panel */}
            <TabPane tab="UDF" key="udf" style={{height: `${paneHeight}px`}}>
              <div className="ri-workbench" style={{height: `${paneHeight}px`}}>
                <Udf
                  projectIdGeted={projectId}
                  udfClassHide={udfClassHide}
                />
              </div>
            </TabPane>
            {/* Resource Panel */}
            <TabPane tab="Resource" key="resource" style={{height: `${paneHeight}px`}}>
              <div className="ri-workbench" style={{height: `${paneHeight}px`}}>
                <Resource
                  projectIdGeted={projectId}
                />
              </div>
            </TabPane>
          </Tabs>
        </Spin>

      </div>
    )
  }
}

Workbench.propTypes = {
  streams: PropTypes.oneOfType([
    PropTypes.array,
    PropTypes.bool
  ]),
  streamSubmitLoading: PropTypes.bool,
  flowSubmitLoading: PropTypes.bool,
  projectNamespaces: PropTypes.oneOfType([
    PropTypes.array,
    PropTypes.bool
  ]),
  router: PropTypes.any,
  onAddStream: PropTypes.func,
  onEditStream: PropTypes.func,
  onAddFlow: PropTypes.func,
  onEditFlow: PropTypes.func,
  onQueryFlow: PropTypes.func,
  onLoadkafka: PropTypes.func,
  // onLoadStreamConfigJvm: PropTypes.func,
  // onLoadStreamConfigSpark: PropTypes.func,
  onLoadStreamDetail: PropTypes.func,
  onLoadSelectStreamKafkaTopic: PropTypes.func,
  onLoadSourceSinkTypeNamespace: PropTypes.func,
  onLoadSinkTypeNamespace: PropTypes.func,
  onLoadTranSinkTypeNamespace: PropTypes.func,
  onLoadSourceToSinkExist: PropTypes.func,
  onLoadJobSourceToSinkExist: PropTypes.func,

  onLoadAdminSingleFlow: PropTypes.func,
  onLoadUserAllFlows: PropTypes.func,
  onLoadAdminSingleStream: PropTypes.func,
  onLoadUserStreams: PropTypes.func,
  onLoadUserNamespaces: PropTypes.func,
  onLoadSelectNamespaces: PropTypes.func,
  onLoadUserUsers: PropTypes.func,
  onLoadSelectUsers: PropTypes.func,
  onLoadResources: PropTypes.func,
  onLoadSingleUdf: PropTypes.func,
  onAddJob: PropTypes.func,
  onQueryJob: PropTypes.func,
  onEditJob: PropTypes.func,
  onLoadLookupSql: PropTypes.func,
  jobSubmitLoading: PropTypes.bool,
  roleType: PropTypes.string,
  locale: PropTypes.string,
  onLoadJobBackfillTopic: PropTypes.func,
  onLoadStreamConfigs: PropTypes.func,
  activeKey: PropTypes.string,
  onChangeTabs: PropTypes.func,
  jumpStreamToFlowFilter: PropTypes.func,
  onLoadSourceNsVersion: PropTypes.func
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadUserAllFlows: (projectId, resolve) => dispatch(loadUserAllFlows(projectId, resolve)),
    onLoadAdminSingleFlow: (projectId, resolve) => dispatch(loadAdminSingleFlow(projectId, resolve)),
    onLoadUserStreams: (projectId, resolve) => dispatch(loadUserStreams(projectId, resolve)),
    onLoadAdminSingleStream: (projectId, resolve) => dispatch(loadAdminSingleStream(projectId, resolve)),
    onLoadUserNamespaces: (projectId, resolve) => dispatch(loadUserNamespaces(projectId, resolve)),
    onLoadSelectNamespaces: (projectId, resolve) => dispatch(loadSelectNamespaces(projectId, resolve)),
    onLoadUserUsers: (projectId, resolve) => dispatch(loadUserUsers(projectId, resolve)),
    onLoadSelectUsers: (projectId, resolve) => dispatch(loadSelectUsers(projectId, resolve)),
    onLoadResources: (projectId, roleType) => dispatch(loadResources(projectId, roleType)),
    onLoadSingleUdf: (projectId, roleType, resolve) => dispatch(loadSingleUdf(projectId, roleType, resolve)),
    onAddStream: (projectId, stream, resolve) => dispatch(addStream(projectId, stream, resolve)),
    onEditStream: (stream, resolve) => dispatch(editStream(stream, resolve)),
    onAddFlow: (values, resolve, final) => dispatch(addFlow(values, resolve, final)),
    onEditFlow: (values, resolve, final) => dispatch(editFlow(values, resolve, final)),
    onQueryFlow: (values, resolve) => dispatch(queryFlow(values, resolve)),
    onLoadkafka: (projectId, nsSys, resolve) => dispatch(loadKafka(projectId, nsSys, resolve)),
    onLoadStreamConfigJvm: (resolve) => dispatch(loadStreamConfigJvm(resolve)),
    onLoadStreamConfigSpark: (resolve) => dispatch(loadStreamConfigSpark(resolve)),
    onLoadStreamConfigs: (type, resolve) => dispatch(loadStreamConfigs(type, resolve)),
    onLoadStreamNameValue: (projectId, value, resolve, reject) => dispatch(loadStreamNameValue(projectId, value, resolve, reject)),
    onLoadStreamDetail: (projectId, streamId, roleType, resolve) => dispatch(loadStreamDetail(projectId, streamId, roleType, resolve)),
    onLoadSelectStreamKafkaTopic: (projectId, streamType, functionType, resolve) => dispatch(loadSelectStreamKafkaTopic(projectId, streamType, functionType, resolve)),

    onLoadSourceSinkTypeNamespace: (projectId, streamId, value, type, resolve) => dispatch(loadSourceSinkTypeNamespace(projectId, streamId, value, type, resolve)),
    onLoadSinkTypeNamespace: (projectId, streamId, value, type, resolve) => dispatch(loadSinkTypeNamespace(projectId, streamId, value, type, resolve)),
    onLoadTranSinkTypeNamespace: (projectId, streamId, value, type, resolve) => dispatch(loadTranSinkTypeNamespace(projectId, streamId, value, type, resolve)),
    onLoadSourceToSinkExist: (projectId, sourceNs, sinkNs, resolve, reject) => dispatch(loadSourceToSinkExist(projectId, sourceNs, sinkNs, resolve, reject)),
    onLoadJobSourceToSinkExist: (projectId, sourceNs, sinkNs, resolve, reject) => dispatch(loadJobSourceToSinkExist(projectId, sourceNs, sinkNs, resolve, reject)),
    onAddJob: (projectId, values, resolve, final) => dispatch(addJob(projectId, values, resolve, final)),
    onQueryJob: (values, resolve, final) => dispatch(queryJob(values, resolve, final)),
    onEditJob: (values, resolve, final) => dispatch(editJob(values, resolve, final)),
    onLoadLookupSql: (values, resolve, reject) => dispatch(loadLookupSql(values, resolve, reject)),
    onLoadJobBackfillTopic: (projectId, namespaceId, value, resolve) => dispatch(loadJobBackfillTopic(projectId, namespaceId, value, resolve)),
    onChangeTabs: (key) => dispatch(changeTabs(key)),
    jumpStreamToFlowFilter: (streamFilterId) => dispatch(jumpStreamToFlowFilter(streamFilterId)),
    onLoadSourceNsVersion: (projectId, namespace, resolve) => dispatch(getSourceNsVersion(projectId, namespace, resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  streams: selectStreams(),
  streamSubmitLoading: selectStreamSubmitLoading(),
  flows: selectFlows(),
  flowSubmitLoading: selectFlowSubmitLoading(),
  namespaces: selectNamespaces(),
  users: selectUsers(),
  resources: selectResources(),
  projectNamespaces: selectProjectNamespaces(),
  roleType: selectRoleType(),
  locale: selectLocale(),
  activeKey: selectActiveKey()
})

export default connect(mapStateToProps, mapDispatchToProps)(Workbench)

