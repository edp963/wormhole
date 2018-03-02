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
import { preProcessSql, formatString, isJSON, operateLanguageSuccessMessage,
  operateLanguageSourceToSink, operateLanguageNameExist, operateLanguageSinkConfig,
  operateLanguageSql } from '../../utils/util'
import { generateSourceSinkNamespaceHierarchy, generateHdfslogNamespaceHierarchy,
  generateTransformSinkNamespaceHierarchy, showSinkConfigMsg} from './workbenchFunction'
import CodeMirror from 'codemirror'
require('../../../node_modules/codemirror/addon/display/placeholder')
require('../../../node_modules/codemirror/mode/javascript/javascript')
require('../../../node_modules/codemirror/mode/sql/sql')

import { FormattedMessage } from 'react-intl'
import messages from './messages'

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

import Button from 'antd/lib/button'
import Tabs from 'antd/lib/tabs'
import Modal from 'antd/lib/modal'
const TabPane = Tabs.TabPane
import Steps from 'antd/lib/steps'
const Step = Steps.Step
import message from 'antd/lib/message'
import Moment from 'moment'

import { changeLocale } from '../../containers/LanguageProvider/actions'
import {loadUserAllFlows, loadAdminSingleFlow, loadSelectStreamKafkaTopic,
  loadSourceSinkTypeNamespace, loadSinkTypeNamespace, loadTranSinkTypeNamespace,
  loadSourceToSinkExist, addFlow, editFlow, queryFlow, loadLookupSql} from '../Flow/action'

import {loadUserStreams, loadAdminSingleStream, loadStreamNameValue, loadKafka,
  loadStreamConfigJvm, addStream, loadStreamDetail, editStream} from '../Manager/action'

import {loadSelectNamespaces, loadUserNamespaces} from '../Namespace/action'
import {loadUserUsers, loadSelectUsers} from '../User/action'
import {loadResources} from '../Resource/action'
import {loadSingleUdf} from '../Udf/action'
import {loadJobName, loadJobSourceNs, loadJobSinkNs, loadJobSourceToSinkExist, addJob,
  queryJob, editJob} from '../Job/action'

import { selectFlows, selectFlowSubmitLoading, selectSourceToSinkExited } from '../Flow/selectors'
import { selectStreams, selectStreamSubmitLoading, selectStreamNameExited } from '../Manager/selectors'
import { selectProjectNamespaces, selectNamespaces } from '../Namespace/selectors'
import { selectUsers } from '../User/selectors'
import { selectResources } from '../Resource/selectors'
import { selectJobNameExited, selectJobSourceToSinkExited } from '../Job/selectors'

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
      transformTagClassName: '',
      transformTableClassName: 'hide',
      transformValue: '',
      transConnectClass: 'hide',
      flowTransNsData: [],

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

      // streamDagModalShow: 'hide',
      // flowDagModalShow: 'hide',

      selectStreamKafkaTopicValue: [],
      sourceTypeNamespaceData: [],
      hdfslogNsData: [],
      routingNsData: [],
      sinkTypeNamespaceData: [],
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

      etpStrategyResponseValue: '',
      topicEditValues: [],
      sinkConfigMsg: '',

      responseTopicInfo: [],
      fieldSelected: 'hide',
      dataframeShowSelected: 'hide',

      singleFlowResult: {},
      streamDiffType: 'default',
      pipelineStreamId: 0,
      hdfslogSinkDataSysValue: '',
      hdfslogSinkNsValue: '',
      routingSinkNsValue: '',
      flowSourceResult: [],

      // job
      jobStepSourceNs: '',
      jobStepSinkNs: '',
      jobSparkConfigValues: {},
      jobSourceNsData: [],
      jobSinkNsData: [],
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
      jobSourceResult: [],

      routingSinkTypeNsData: [],
      routingSourceNsValue: '',
      transConfigConfirmValue: '',
      sinkConfigCopy: '',
      sinkDSCopy: ''
    }
  }

  componentWillMount () {
    const projectId = this.props.router.params.projectId
    this.loadData(projectId)
    this.setState({ tabPanelKey: 'flow' })
    this.props.onChangeLanguage(localStorage.getItem('preferredLanguage'))
  }

  componentWillReceiveProps (props) {
    const projectId = props.router.params.projectId
    if (projectId !== this.state.projectId) {
      this.loadData(projectId)
    }
  }

  loadData (projectId) {
    this.setState({ projectId: projectId })
  }

  changeTag = (key) => {
    const { projectId } = this.state
    const { onLoadAdminSingleFlow, onLoadUserAllFlows, onLoadAdminSingleStream, onLoadUserStreams } = this.props
    const { onLoadSelectNamespaces, onLoadUserNamespaces, onLoadSelectUsers, onLoadUserUsers, onLoadResources, onLoadSingleUdf } = this.props
    let roleTypeTemp = localStorage.getItem('loginRoleType')

    switch (key) {
      case 'flow':
        if (roleTypeTemp === 'admin') {
          onLoadAdminSingleFlow(projectId, () => {})
        } else if (roleTypeTemp === 'user') {
          onLoadUserAllFlows(projectId, () => {})
        }
        break
      case 'stream':
        if (roleTypeTemp === 'admin') {
          onLoadAdminSingleStream(projectId, () => {})
        } else if (roleTypeTemp === 'user') {
          onLoadUserStreams(projectId, () => {})
        }
        break
      case 'namespace':
        if (roleTypeTemp === 'admin') {
          onLoadSelectNamespaces(projectId, () => {})
        } else if (roleTypeTemp === 'user') {
          onLoadUserNamespaces(projectId, () => {})
        }
        break
      case 'user':
        if (roleTypeTemp === 'admin') {
          onLoadSelectUsers(projectId, () => {})
        } else if (roleTypeTemp === 'user') {
          onLoadUserUsers(projectId, () => {})
        }
        break
      case 'resource':
        if (roleTypeTemp === 'admin') {
          onLoadResources(projectId, 'admin')
        } else if (roleTypeTemp === 'user') {
          onLoadResources(projectId, 'user')
        }
        break
      case 'udf':
        if (roleTypeTemp === 'admin') {
          onLoadSingleUdf(projectId, 'admin', () => {})
        } else if (roleTypeTemp === 'user') {
          onLoadSingleUdf(projectId, 'user', () => {})
        }
        break
    }
    this.setState({ tabPanelKey: key })
  }

  // 新增Stream时，验证 stream name 是否存在
  onInitStreamNameValue = (value) => {
    this.props.onLoadStreamNameValue(this.state.projectId, value, () => {}, () => {
      this.workbenchStreamForm.setFields({
        streamName: {
          value: value,
          errors: [new Error(operateLanguageNameExist())]
        }
      })
    })
  }

  initialHdfslogCascader = (value) => this.setState({ hdfslogSinkNsValue: value.join('.') })

  initialRoutingCascader = (value) => {
    const { projectId, pipelineStreamId } = this.state

    this.setState({
      routingSourceNsValue: value.join('.')
    }, () => {
      // 调显示 routing sink namespace 下拉框数据的接口
      this.props.onLoadSinkTypeNamespace(projectId, pipelineStreamId, 'kafka', 'sinkType', (result) => {
        const exceptValue = result.filter(s => [s.nsInstance, s.nsDatabase, s.nsTable].join('.') !== this.state.routingSourceNsValue)

        this.setState({
          routingSinkTypeNsData: generateSourceSinkNamespaceHierarchy('kafka', exceptValue)
        })
      })
    })
  }

  initialRoutingSinkCascader = (value) => this.setState({ routingSinkNsValue: value.join('.') })

  /**
   * 新增Flow时，获取 default type source namespace 下拉框
   * */
  onInitSourceTypeNamespace = (projectId, value, type) => {
    const { flowMode, pipelineStreamId } = this.state

    this.setState({ sourceTypeNamespaceData: [] })

    if (pipelineStreamId !== 0) {
      this.props.onLoadSourceSinkTypeNamespace(projectId, pipelineStreamId, value, type, (result) => {
        this.setState({
          flowSourceResult: result,
          sourceTypeNamespaceData: generateSourceSinkNamespaceHierarchy(value, result)
        })
        // default source ns 和 sink ns 同时调同一个接口获得，保证两处的 placeholder 和单条数据回显都能正常
        if (flowMode === 'add' || flowMode === 'copy') {
          this.workbenchFlowForm.setFieldsValue({ sourceNamespace: undefined })
        }
      })
    }
  }

  /**
   * 新增Job时，获取 source namespace 下拉框数据
   * */
  onInitJobSourceNs = (projectId, value, type) => {
    const { jobMode } = this.state
    const languageText = localStorage.getItem('preferredLanguage')

    this.setState({ jobSourceNsData: [] })

    this.props.onLoadJobSourceNs(projectId, value, type, (result) => {
      this.setState({
        jobSourceResult: result,
        jobSourceNsData: generateSourceSinkNamespaceHierarchy(value, result)
      })
      if (jobMode === 'add') {
        this.workbenchJobForm.setFieldsValue({ sourceNamespace: undefined })
      }
    }, (result) => {
      message.error(`Source ${languageText === 'en' ? 'exception:' : '异常：'} ${result}`, 5)
    })
  }

  /**
   * 新增Flow时，获取 hdfslog type source namespace 下拉框数据
   * */
  onInitHdfslogNamespace = (projectId, value, type) => {
    const { pipelineStreamId } = this.state

    this.setState({
      hdfslogNsData: []
    })
    if (pipelineStreamId !== 0) {
      this.props.onLoadSourceSinkTypeNamespace(projectId, pipelineStreamId, value, type, (result) => {
        this.setState({
          flowSourceResult: result,
          hdfslogNsData: generateHdfslogNamespaceHierarchy(value, result),
          hdfslogSinkDataSysValue: value
        })
      })
    }
  }

  /**
   * 新增Flow时，获取 routing type source namespace 下拉框数据
   * */
  onInitRoutingNamespace = (projectId, value, type) => {
    const { pipelineStreamId, flowMode } = this.state

    this.setState({
      routingSourceNsValue: '',
      routingSinkNsValue: '',
      routingNsData: []
    })
    if (pipelineStreamId !== 0) {
      this.props.onLoadSourceSinkTypeNamespace(projectId, pipelineStreamId, value, type, (result) => {
        this.setState({
          flowSourceResult: result,
          routingNsData: generateSourceSinkNamespaceHierarchy(value, result),
          routingSinkDataSysValue: value
        })
        if (flowMode === 'add' || flowMode === 'copy') {
          this.workbenchFlowForm.setFieldsValue({
            routingNamespace: undefined,
            routingSinkNs: undefined
          })
        }
      })
    }
  }

  /**
   * 新增Flow时，获取 default type sink namespace 下拉框
   * */
  onInitSinkTypeNamespace = (projectId, value, type) => {
    const { flowMode, pipelineStreamId, sinkDSCopy } = this.state

    this.setState({ sinkConfigCopy: sinkDSCopy === value ? this.state.sinkConfigCopy : '' })

    this.setState({
      sinkTypeNamespaceData: [],
      sinkConfigMsg: showSinkConfigMsg(value)
    })
    if (pipelineStreamId !== 0) {
      this.props.onLoadSinkTypeNamespace(projectId, pipelineStreamId, value, type, (result) => {
        this.setState({
          sinkTypeNamespaceData: generateSourceSinkNamespaceHierarchy(value, result)
        })
        if (flowMode === 'add' || flowMode === 'copy') {
          this.workbenchFlowForm.setFieldsValue({ sinkNamespace: undefined })
        }
      })
    }
  }

  /**
   * 新增Job时，获取 sink namespace 下拉框数据
   * */
  onInitJobSinkNs = (projectId, value, type) => {
    const { jobMode } = this.state
    const languageText = localStorage.getItem('preferredLanguage')

    this.setState({
      jobSinkNsData: [],
      jobSinkConfigMsg: showSinkConfigMsg(value)
    })
    this.props.onLoadJobSinkNs(projectId, value, type, (result) => {
      this.setState({ jobSinkNsData: generateSourceSinkNamespaceHierarchy(value, result) })
      if (jobMode === 'add') {
        this.workbenchJobForm.setFieldsValue({ sinkNamespace: undefined })
      }
    }, (result) => {
      message.error(`Sink ${languageText === 'en' ? 'exception:' : '异常：'} ${result}`, 5)
    })
  }

  /**
   * 新增Flow时，获取 default transformation sink namespace 下拉框
   * */
  onInitTransformSinkTypeNamespace = (projectId, value, type) => {
    const { pipelineStreamId } = this.state

    this.setState({ transformSinkTypeNamespaceData: [] })

    if (pipelineStreamId !== 0) {
      this.props.onLoadTranSinkTypeNamespace(projectId, this.state.pipelineStreamId, value, type, (result) => {
        this.setState({
          transformSinkNamespaceArray: result,
          transformSinkTypeNamespaceData: generateTransformSinkNamespaceHierarchy(value, result)
        })
      })
    }
  }

  initResultFieldClass = (value) => this.setState({ fieldSelected: value === 'all' ? 'hide' : '' })
  initDataShowClass = (value) => this.setState({ dataframeShowSelected: value === 'true' ? '' : 'hide' })

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
      etpStrategyRequestValue: {}
    }, () => {
      this.workbenchFlowForm.setFieldsValue({
        resultFields: 'all',
        dataframeShow: 'false',
        dataframeShowNum: 10
      })
      this.onInitStreamTypeSelect('default')
    })
  }

  onInitStreamTypeSelect = (val) => {
    this.setState({ streamDiffType: val })
    const languageText = localStorage.getItem('preferredLanguage')

    // 显示 Stream 信息
    this.props.onLoadSelectStreamKafkaTopic(this.state.projectId, val, (result) => {
      const resultFinal = result.map(s => {
        const responseResult = Object.assign(s.stream, {
          disableActions: s.disableActions,
          topicInfo: s.topicInfo,
          instance: s.kafkaInfo.instance,
          connUrl: s.kafkaInfo.connUrl,
          projectName: s.projectName,
          currentUdf: s.currentUdf,
          usingUdf: s.usingUdf
        })
        responseResult.key = responseResult.id
        return responseResult
      })

      this.setState({
        selectStreamKafkaTopicValue: resultFinal,
        hdfslogSinkDataSysValue: '',
        hdfslogSinkNsValue: ''
      })
      if (result.length === 0) {
        message.warning(languageText === 'en' ? 'Please create a Stream with corresponding type first!' : '请先新建相应类型的 Stream！', 3)
        this.setState({
          pipelineStreamId: 0,
          flowKafkaInstanceValue: '',
          flowKafkaTopicValue: ''
        })
      } else {
        const topicTemp = resultFinal[0].topicInfo

        this.setState({
          pipelineStreamId: resultFinal[0].id,
          flowKafkaInstanceValue: resultFinal[0].instance,
          flowKafkaTopicValue: topicTemp.map(j => j.name).join(',')
        })
        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: resultFinal[0].id,
          streamName: resultFinal[0].name
        })
      }
    })
    this.workbenchFlowForm.setFieldsValue({
      sourceDataSystem: '',
      hdfslogNamespace: undefined
    })
  }

  onInitStreamNameSelect = (valName) => {
    const { streamDiffType, selectStreamKafkaTopicValue } = this.state

    const selName = selectStreamKafkaTopicValue.find(s => s.name === valName)
    const topicTemp = selName.topicInfo
    const valId = selName.id
    this.setState({
      pipelineStreamId: Number(valId),
      flowKafkaInstanceValue: selName.instance,
      flowKafkaTopicValue: topicTemp.map(j => j.name).join(',')
    })

    switch (streamDiffType) {
      case 'default':
        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: Number(valId),
          sourceDataSystem: '',
          sinkDataSystem: '',
          sourceNamespace: undefined,
          sinkNamespace: undefined
        })
        break
      case 'hdfslog':
        this.setState({
          hdfslogSinkDataSysValue: '',
          hdfslogSinkNsValue: ''
        })
        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: Number(valId),
          sourceDataSystem: '',
          hdfslogNamespace: undefined
        })
        break
      case 'routing':
        this.setState({
          routingSourceNsValue: '',
          routingSinkNsValue: ''
        })
        this.workbenchFlowForm.setFieldsValue({ flowStreamId: Number(valId) })
        break
    }
  }

  showCopyFlowWorkbench = (flow) => {
    this.setState({ flowMode: 'copy' })

    new Promise((resolve) => {
      resolve(flow)
      this.workbenchFlowForm.resetFields()
    })
      .then((flow) => {
        this.queryFlowInfo(flow)
      })
  }

  showEditJobWorkbench = (job) => () => {
    this.setState({ jobMode: 'edit' })

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
          type: resultFinal.sourceType,
          eventStartTs: resultFinal.eventTsStart === '' ? null : Moment(formatString(resultFinal.eventTsStart)),
          eventEndTs: resultFinal.eventTsEnd === '' ? null : Moment(formatString(resultFinal.eventTsEnd))
        })

        this.setState({
          formStep: 0,
          jobSparkConfigValues: {
            sparkConfig: resultFinal.sparkConfig,
            startConfig: resultFinal.startConfig
          },
          singleJobResult: {
            id: resultFinal.id,
            name: resultFinal.name,
            projectId: resultFinal.projectId,
            sourceNs: resultFinal.sourceNs,
            sinkNs: resultFinal.sinkNs,
            sourceType: resultFinal.sourceType,
            sparkAppid: resultFinal.sparkAppid,
            logPath: resultFinal.logPath,
            startedTime: resultFinal.startedTime,
            stoppedTime: resultFinal.stoppedTime,
            status: resultFinal.status,
            createTime: resultFinal.createTime,
            createBy: resultFinal.createBy,
            updateTime: resultFinal.updateTime,
            updateBy: resultFinal.updateBy
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
          sinkProtocolShow = sinkConfigVal.sink_protocol

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

        this.workbenchJobForm.setFieldsValue({
          sourceDataSystem: sourceNsArr[0],
          sourceNamespace: [sourceNsArr[1], sourceNsArr[2], sourceNsArr[3]],
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
    this.setState({ flowMode: 'edit' })

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
      streamDiffType: typeof (flow.streamType) === 'object' ? flow.streamTypeOrigin : flow.streamType
    }, () => {
      const { streamDiffType } = this.state
      switch (streamDiffType) {
        case 'default':
          this.queryFlowDefault(flow)
          break
        case 'hdfslog':
          this.queryFlowHdfslog(flow)
          break
        case 'routing':
          this.queryFlowRouting(flow)
          break
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

        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: result.streamId,
          streamName: result.streamName,
          streamType: result.streamType,
          protocol: result.consumedProtocol
        })

        this.setState({
          formStep: 0,
          pipelineStreamId: result.streamId,
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
        const sinkNsArr = result.sinkNs.split('.')

        let dataframeShowVal = ''
        if (result.tranConfig !== '') {
          if (result.tranConfig.includes('action')) {
            const temp = JSON.parse(JSON.stringify(result.tranConfig))
            const tt = temp.replace(/\n/g, ' ')
            const tranConfigVal = JSON.parse(tt)

            let validityTemp = tranConfigVal.validity
            if (result.tranConfig.includes('validity')) {
              const requestTempJson = {
                check_columns: validityTemp.check_columns,
                check_rule: validityTemp.check_rule,
                rule_mode: validityTemp.rule_mode,
                rule_params: validityTemp.rule_params,
                against_action: validityTemp.against_action
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
                etpStrategyResponseValue: ''
              })
            }

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

            const tranActionArr = tranConfigVal.action.split(';')
            tranActionArr.splice(tranActionArr.length - 1, 1)

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
              }

              if (i.includes('parquet_sql')) {
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
              }

              if (i.includes('spark_sql')) {
                const sparkAfterPart = i.substring(i.indexOf('=') + 1)
                const sparkAfterPartTepmTemp = sparkAfterPart.replace(/(^\s*)|(\s*$)/g, '')
                const sparkAfterPartTepm = preProcessSql(sparkAfterPartTepmTemp)

                tranConfigInfoTemp = `${sparkAfterPartTepm};`
                tranConfigInfoSqlTemp = `${sparkAfterPartTepm};`
                tranTypeTepm = 'sparkSql'
                pushdownConTepm = {}
              }

              if (i.includes('custom_class')) {
                const classAfterPart = i.substring(i.indexOf('=') + 1)
                const classAfterPartTepmTemp = classAfterPart.replace(/(^\s*)|(\s*$)/g, '')
                const classAfterPartTepm = preProcessSql(classAfterPartTepmTemp)

                tranConfigInfoTemp = classAfterPartTepm
                tranConfigInfoSqlTemp = classAfterPartTepm
                tranTypeTepm = 'transformClassName'
                pushdownConTepm = {}
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
        })

        this.workbenchFlowForm.setFieldsValue({
          sourceDataSystem: sourceNsArr[0],
          sourceNamespace: [sourceNsArr[1], sourceNsArr[2], sourceNsArr[3]],
          sinkDataSystem: sinkNsArr[0],
          sinkNamespace: [sinkNsArr[1], sinkNsArr[2], sinkNsArr[3]],

          sinkConfig: sinkConfigShow,
          resultFields: resultFieldsVal,
          dataframeShow: dataframeShowVal,
          flowSpecialConfig: flowSpecialConfigVal
        })
      })
  }

  qureryFlowDefaultFinal () {
    this.setState({
      transformTagClassName: '',
      transformTableClassName: 'hide',
      transConnectClass: 'hide',
      etpStrategyCheck: false,
      dataframeShowSelected: 'hide'
    })
  }

  queryFlowHdfslog (flow) {
    new Promise((resolve) => {
      const requestData = {
        projectId: flow.projectId,
        streamId: flow.streamId,
        id: flow.id
      }
      this.props.onQueryFlow(requestData, (result) => {
        resolve(result)

        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: result.streamId,
          streamName: result.streamName,
          streamType: result.streamType
        })

        const resultSinkNsArr = result.sinkNs.split('.')
        const resultSinkNsFinal = [resultSinkNsArr[1], resultSinkNsArr[2], resultSinkNsArr[3]].join('.')

        this.setState({
          formStep: 0,
          pipelineStreamId: result.streamId,
          hdfslogSinkNsValue: this.state.flowMode === 'copy' ? '' : resultSinkNsFinal,
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
          hdfslogNamespace: [
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
      const requestData = {
        projectId: flow.projectId,
        streamId: flow.streamId,
        id: flow.id
      }
      this.props.onQueryFlow(requestData, (result) => {
        resolve(result)

        this.workbenchFlowForm.setFieldsValue({
          flowStreamId: result.streamId,
          streamName: result.streamName,
          streamType: result.streamType
        })
        const sourceNsArr = result.sourceNs.split('.')
        const showSourceNs = [sourceNsArr[0], sourceNsArr[1], sourceNsArr[2]].join('.')
        const sinkNsArr = result.sinkNs.split('.')
        const showSinkNs = [sinkNsArr[0], sinkNsArr[1], sinkNsArr[2]].join('.')

        this.setState({
          formStep: 0,
          pipelineStreamId: result.streamId,
          routingSourceNsValue: this.state.flowMode === 'copy' ? '' : showSourceNs,
          routingSinkNsValue: this.state.flowMode === 'copy' ? '' : showSinkNs,
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

  showAddStreamWorkbench = () => {
    this.workbenchStreamForm.resetFields()
    // 显示 jvm 数据，从而获得初始的 sparkConfig
    this.setState({
      streamMode: 'add',
      streamConfigCheck: false
    })

    this.props.onLoadStreamConfigJvm((result) => {
      const othersInit = 'spark.locality.wait=10ms,spark.shuffle.spill.compress=false,spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec,spark.streaming.stopGracefullyOnShutdown=true,spark.scheduler.listenerbus.eventqueue.size=1000000,spark.sql.ui.retainedExecutions=3'

      const startConfigJson = {
        driverCores: 1,
        driverMemory: 2,
        executorNums: 6,
        perExecutorMemory: 2,
        perExecutorCores: 1
      }
      const launchConfigJson = {
        durations: 30,
        partitions: 6,
        maxRecords: 10
      }

      this.setState({
        streamConfigValues: {
          sparkConfig: `${result},${othersInit}`,
          startConfig: `${JSON.stringify(startConfigJson)}`,
          launchConfig: `${JSON.stringify(launchConfigJson)}`
        }
      })
    })

    // 显示 Kafka
    this.props.onLoadkafka(this.state.projectId, 'kafka', (result) => this.setState({ kafkaValues: result }))
  }

  showEditStreamWorkbench = (stream) => () => {
    this.setState({
      streamMode: 'edit',
      streamConfigCheck: true
    })
    this.workbenchStreamForm.resetFields()

    this.props.onLoadStreamDetail(this.state.projectId, stream.id, 'user', (result) => {
      const resultVal = Object.assign(result.stream, {
        disableActions: result.disableActions,
        topicInfo: result.topicInfo,
        instance: result.kafkaInfo.instance,
        connUrl: result.kafkaInfo.connUrl,
        projectName: result.projectName,
        currentUdf: result.currentUdf,
        usingUdf: result.usingUdf
      })

      this.workbenchStreamForm.setFieldsValue({
        streamName: resultVal.name,
        type: resultVal.streamType,
        desc: resultVal.desc,
        kafka: resultVal.instance
      })

      this.setState({
        streamConfigValues: {
          sparkConfig: resultVal.sparkConfig,
          startConfig: resultVal.startConfig,
          launchConfig: resultVal.launchConfig
        },

        streamQueryValues: {
          id: resultVal.id,
          projectId: resultVal.projectId
        }
      })
    })
  }

  /**
   * Stream Config Modal
   * */
  onShowConfigModal = () => {
    const { streamConfigValues } = this.state
    this.setState({
      streamConfigModalVisible: true
    }, () => {
      // 点击 config 按钮时，回显数据。 有且只有2条 jvm 配置
      const streamConArr = streamConfigValues.sparkConfig.split(',')

      const tempJvmArr = []
      const tempOthersArr = []
      for (let i = 0; i < streamConArr.length; i++) {
        // 是否是 jvm
        streamConArr[i].includes('extraJavaOptions') ? tempJvmArr.push(streamConArr[i]) : tempOthersArr.push(streamConArr[i])
      }

      const jvmTempValue = tempJvmArr.join('\n')
      const personalConfTempValue = tempOthersArr.join('\n')

      const startConfigTemp = JSON.parse(streamConfigValues.startConfig)
      const launchConfigTemp = JSON.parse(streamConfigValues.launchConfig)

      this.streamConfigForm.setFieldsValue({
        jvm: jvmTempValue,
        driverCores: startConfigTemp.driverCores,
        driverMemory: startConfigTemp.driverMemory,
        executorNums: startConfigTemp.executorNums,
        perExecutorCores: startConfigTemp.perExecutorCores,
        perExecutorMemory: startConfigTemp.perExecutorMemory,
        durations: launchConfigTemp.durations,
        partitions: launchConfigTemp.partitions,
        maxRecords: launchConfigTemp.maxRecords,
        personalConf: personalConfTempValue
      })
    })
  }

  /**
   * Spark Config Modal
   * */
  onShowSparkConfigModal = () => {
    const { jobSparkConfigValues } = this.state

    this.setState({
      sparkConfigModalVisible: true
    }, () => {
      const sparkConArr = jobSparkConfigValues.sparkConfig.split(',')

      const jobTempJvmArr = []
      const jobTempOthersArr = []
      for (let i = 0; i < sparkConArr.length; i++) {
        sparkConArr[i].includes('extraJavaOptions') ? jobTempJvmArr.push(sparkConArr[i]) : jobTempOthersArr.push(sparkConArr[i])
      }

      const jvmTempValue = jobTempJvmArr.join('\n')
      const personalConfTempValue = jobTempOthersArr.join('\n')
      const startConfigTemp = JSON.parse(jobSparkConfigValues.startConfig)

      this.streamConfigForm.setFieldsValue({
        jvm: jvmTempValue,
        driverCores: startConfigTemp.driverCores,
        driverMemory: startConfigTemp.driverMemory,
        executorNums: startConfigTemp.executorNums,
        perExecutorCores: startConfigTemp.perExecutorCores,
        perExecutorMemory: startConfigTemp.perExecutorMemory,
        personalConf: personalConfTempValue
      })
    })
  }

  hideConfigModal = () => this.setState({ streamConfigModalVisible: false })
  hideSparkConfigModal = () => this.setState({ sparkConfigModalVisible: false })

  onConfigModalOk = () => {
    const languageText = localStorage.getItem('preferredLanguage')
    this.streamConfigForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        values.personalConf = values.personalConf.trim()
        values.jvm = values.jvm.trim()

        const nJvm = (values.jvm.split('extraJavaOptions')).length - 1
        let jvmValTemp = ''
        if (nJvm === 2) {
          jvmValTemp = values.jvm.replace(/\n/g, ',')

          let sparkConfigValue = ''
          if (!values.personalConf) {
            sparkConfigValue = jvmValTemp
          } else {
            const nOthers = (values.jvm.split('=')).length - 1

            const personalConfTemp = nOthers === 1
              ? values.personalConf
              : values.personalConf.replace(/\n/g, ',')

            sparkConfigValue = `${jvmValTemp},${personalConfTemp}`
          }

          const startConfigJson = {
            driverCores: values.driverCores,
            driverMemory: values.driverMemory,
            executorNums: values.executorNums,
            perExecutorMemory: values.perExecutorMemory,
            perExecutorCores: values.perExecutorCores
          }

          const launchConfigJson = {
            durations: values.durations,
            partitions: values.partitions,
            maxRecords: values.maxRecords
          }

          this.setState({
            streamConfigCheck: true,
            streamConfigValues: {
              sparkConfig: sparkConfigValue,
              startConfig: JSON.stringify(startConfigJson),
              launchConfig: JSON.stringify(launchConfigJson)
            }
          })
          this.hideConfigModal()
        } else {
          message.warning(languageText === 'en' ? 'Please configure JVM correctly!' : '请正确配置 JVM！', 3)
        }
      }
    })
  }

  onSparkConfigModalOk = () => {
    const languageText = localStorage.getItem('preferredLanguage')
    this.streamConfigForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        values.personalConf = values.personalConf.trim()
        values.jvm = values.jvm.trim()

        const nJvm = (values.jvm.split('extraJavaOptions')).length - 1
        let jvmValTemp = ''
        if (nJvm === 2) {
          jvmValTemp = values.jvm.replace(/\n/g, ',')

          let sparkConfigVal = ''
          if (!values.personalConf) {
            sparkConfigVal = jvmValTemp
          } else {
            const nOthers = (values.jvm.split('=')).length - 1

            const personalConfTemp = nOthers === 1
              ? values.personalConf
              : values.personalConf.replace(/\n/g, ',')

            sparkConfigVal = `${jvmValTemp},${personalConfTemp}`
          }

          const startConfigJson = {
            driverCores: values.driverCores,
            driverMemory: values.driverMemory,
            executorNums: values.executorNums,
            perExecutorMemory: values.perExecutorMemory,
            perExecutorCores: values.perExecutorCores
          }

          this.setState({
            sparkConfigCheck: true,
            jobSparkConfigValues: {
              sparkConfig: sparkConfigVal,
              startConfig: JSON.stringify(startConfigJson)
            }
          })
          this.hideSparkConfigModal()
        } else {
          message.warning(languageText === 'en' ? 'Please configure JVM correctly!' : '请正确配置 JVM！', 3)
        }
      }
    })
  }

  hideFlowWorkbench = () => this.setState({ flowMode: '' })
  hideStreamWorkbench = () => this.setState({ streamMode: '' })
  hideJobWorkbench = () => this.setState({ jobMode: '' })

  forwardStep = () => {
    const { tabPanelKey, streamDiffType } = this.state

    if (tabPanelKey === 'flow') {
      if (streamDiffType === 'default') {
        this.handleForwardDefault()
      } else if (streamDiffType === 'hdfslog' || streamDiffType === 'routing') {
        this.handleForwardHdfslogOrRouting()
      }
    } else if (tabPanelKey === 'job') {
      this.handleForwardJob()
    }
  }

  loadSTSExit (values) {
    const { flowMode, flowSourceResult } = this.state
    const insDBTable = [values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2]]
    const sourceDataSys = flowSourceResult.filter(s => [s.nsInstance, s.nsDatabase, s.nsTable].join(',') === insDBTable.join(','))

    if (flowMode === 'add' || flowMode === 'copy') {
      // 新增flow时验证source to sink 是否存在
      const sourceInfo = [sourceDataSys[0].nsSys, values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2], '*', '*', '*'].join('.')
      const sinkInfo = [values.sinkDataSystem, values.sinkNamespace[0], values.sinkNamespace[1], values.sinkNamespace[2], '*', '*', '*'].join('.')

      this.props.onLoadSourceToSinkExist(this.state.projectId, sourceInfo, sinkInfo, () => {
        this.setState({
          formStep: this.state.formStep + 1,
          step2SourceNamespace: [sourceDataSys[0].nsSys, values.sourceNamespace.join('.')].join('.'),
          step2SinkNamespace: [values.sinkDataSystem, values.sinkNamespace.join('.')].join('.')
        })
      }, () => {
        message.error(operateLanguageSourceToSink(), 3)
      })
    } else if (flowMode === 'edit') {
      this.setState({
        formStep: this.state.formStep + 1,
        step2SourceNamespace: [sourceDataSys[0].nsSys, values.sourceNamespace.join('.')].join('.'),
        step2SinkNamespace: [values.sinkDataSystem, values.sinkNamespace.join('.')].join('.')
      })
    }
  }

  handleForwardDefault () {
    const { flowFormTranTableSource, streamDiffType } = this.state
    const languageText = localStorage.getItem('preferredLanguage')

    let tranRequestTempArr = []
    flowFormTranTableSource.map(i => tranRequestTempArr.push(preProcessSql(i.transformConfigInfoRequest)))
    const tranRequestTempString = tranRequestTempArr.join('')
    this.setState({
      transformTableRequestValue: tranRequestTempString === '' ? {} : {'action': tranRequestTempString},
      transformTableConfirmValue: tranRequestTempString === '' ? '' : `"${tranRequestTempString}"`
    })

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
        switch (this.state.formStep) {
          case 0:
            if (!values.sinkConfig) {
              const dataText = languageText === 'en' ? 'When Data System is ' : 'Data System 为 '
              const emptyText = languageText === 'en' ? ', Sink Config cannot be empty!' : ' 时，Sink Config 不能为空！'
              values.sinkDataSystem === 'hbase'
                ? message.error(`${dataText}${values.sinkDataSystem}${emptyText}`, 3)
                : this.loadSTSExit(values)
            } else {
              // json 校验
              isJSON(values.sinkConfig)
                ? this.loadSTSExit(values)
                : message.error(languageText === 'en' ? 'Sink Config should be JSON format!' : 'Sink Config 应为 JSON格式！', 3)
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
            if (streamDiffType === 'default') {
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
            } else if (streamDiffType === 'hdfslog') {
              this.setState({ formStep: this.state.formStep + 2 })
            }
            break
        }
      }
    })
  }

  handleForwardHdfslogOrRouting () {
    const { flowMode, projectId, streamDiffType } = this.state

    this.workbenchFlowForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (flowMode === 'add' || flowMode === 'copy') {
          // 新增flow时验证source to sink 是否存在
          const sourceInfo = streamDiffType === 'hdfslog'
            ? [values.sourceDataSystem, values.hdfslogNamespace[0], values.hdfslogNamespace[1], values.hdfslogNamespace[2], '*', '*', '*'].join('.')
            : [values.sourceDataSystem, values.routingNamespace[0], values.routingNamespace[1], values.routingNamespace[2], '*', '*', '*'].join('.')

          const sinkInfo = streamDiffType === 'hdfslog'
            ? sourceInfo
            : ['kafka', values.routingSinkNs[0], values.routingSinkNs[1], values.routingSinkNs[2], '*', '*', '*'].join('.')

          this.props.onLoadSourceToSinkExist(projectId, sourceInfo, sinkInfo, () => {
            this.setState({ formStep: this.state.formStep + 2 })
          }, () => {
            message.error(operateLanguageSourceToSink(), 3)
          })
        } else if (flowMode === 'edit') {
          this.setState({ formStep: this.state.formStep + 2 })
        }
      }
    })
  }

  loadJobSTSExit (values) {
    const { jobMode, formStep, projectId } = this.state

    if (jobMode === 'add') {
      // 新增 Job 时验证 source to sink 是否存在
      const sourceInfo = [values.sourceDataSystem, values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2], '*', '*', '*'].join('.')
      const sinkInfo = [values.sinkDataSystem, values.sinkNamespace[0], values.sinkNamespace[1], values.sinkNamespace[2], '*', '*', '*'].join('.')

      this.props.onLoadJobSourceToSinkExist(projectId, sourceInfo, sinkInfo, () => {
        this.setState({
          formStep: formStep + 1,
          jobStepSourceNs: [values.sourceDataSystem, values.sourceNamespace.join('.')].join('.'),
          jobStepSinkNs: [values.sinkDataSystem, values.sinkNamespace.join('.')].join('.')
        })
      }, () => {
        message.error(operateLanguageSourceToSink, 3)
      })
    } else if (jobMode === 'edit') {
      this.setState({
        formStep: formStep + 1,
        jobStepSourceNs: [values.sourceDataSystem, values.sourceNamespace.join('.')].join('.'),
        jobStepSinkNs: [values.sinkDataSystem, values.sinkNamespace.join('.')].join('.')
      })
    }
  }

  handleForwardJob () {
    const { formStep, jobFormTranTableSource } = this.state
    const { jobNameExited } = this.props
    const languageText = localStorage.getItem('preferredLanguage')

    this.workbenchJobForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        switch (formStep) {
          case 0:
            if (jobNameExited) {
              this.workbenchJobForm.setFields({
                jobName: {
                  value: values.jobName,
                  errors: [new Error(operateLanguageNameExist())]
                }
              })
              message.error(operateLanguageNameExist(), 3)
            } else {
              if (!values.sinkConfig) {
                const dataText = languageText === 'en' ? 'When Data System is ' : 'Data System 为 '
                const emptyText = languageText === 'en' ? ', Sink Config cannot be empty!' : ' 时，Sink Config 不能为空！'
                values.sinkDataSystem === 'hbase'
                  ? message.error(`${dataText}${values.sinkDataSystem}${emptyText}`, 3)
                  : this.loadJobSTSExit(values)
              } else {
                isJSON(values.sinkConfig)
                  ? this.loadJobSTSExit(values)
                  : message.error(languageText === 'en' ? 'Sink Config should be JSON format!' : 'Sink Config 应为 JSON格式！', 3)
              }
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
            tempRequestVal = tranRequestTempString === ''
              ? {}
              : {'action': tranRequestTempString}

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

  backwardStep = () => {
    const { streamDiffType, formStep, tabPanelKey } = this.state
    if (tabPanelKey === 'flow') {
      if (streamDiffType === 'default') {
        this.setState({ formStep: formStep - 1 })
      } else if (streamDiffType === 'hdfslog' || streamDiffType === 'routing') {
        this.setState({ formStep: formStep - 2 })
      }
    } else if (tabPanelKey === 'job') {
      this.setState({ formStep: formStep - 1 })
    }
  }

  generateStepButtons = () => {
    const { tabPanelKey } = this.state

    switch (this.state.formStep) {
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
              loading={tabPanelKey === 'flow' ? this.props.flowSubmitLoading : this.props.jobSubmitLoading}
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
    const startTs = val.replace(/-| |:/g, '')
    this.setState({ startTsVal: startTs })
  }

  initEndTS = (val) => {
    const endTs = val.replace(/-| |:/g, '')
    this.setState({ endTsVal: endTs })
  }

  submitJobForm = () => {
    const values = this.workbenchJobForm.getFieldsValue()
    const languageText = localStorage.getItem('preferredLanguage')

    const { projectId, jobMode, startTsVal, endTsVal, singleJobResult } = this.state
    const { jobResultFiledsOutput, jobTranTableRequestValue, jobSparkConfigValues } = this.state

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
    if (values.resultFields === 'all') {
      if (!values.sinkConfig) {
        sinkConfigRequest = !values.sinkProtocol ? maxRecord : JSON.stringify(obj1)
      } else {
        sinkConfigRequest = !values.sinkProtocol ? JSON.stringify(obj2) : JSON.stringify(obj3)
      }
    } else {
      const obg4 = { sink_output: values.resultFieldsSelected }
      if (!values.sinkConfig) {
        sinkConfigRequest = !values.sinkProtocol ? maxRecordAndResult : JSON.stringify(Object.assign(obj1, obg4))
      } else {
        sinkConfigRequest = !values.sinkProtocol ? JSON.stringify(Object.assign(obj2, obg4)) : JSON.stringify(Object.assign(obj3, obg4))
      }
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
      eventTsStart: (!values.eventStartTs) ? '' : startTsVal,
      eventTsEnd: (!values.eventEndTs) ? '' : endTsVal,
      sinkConfig: sinkConfigRequest,
      tranConfig: tranConfigRequest
    }

    if (jobMode === 'add') {
      const { jobSourceResult } = this.state
      // source data system 选择log后，根据接口返回的nsSys值，拼接 sourceDataInfo
      const InsDBTable = [values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2]]
      const sourceDataSys = jobSourceResult.filter(s => [s.nsInstance, s.nsDatabase, s.nsTable].join(',') === InsDBTable.join(','))
      const sourceDataInfo = [sourceDataSys[0].nsSys, values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2], '*', '*', '*'].join('.')
      const sinkDataInfo = [values.sinkDataSystem, values.sinkNamespace[0], values.sinkNamespace[1], values.sinkNamespace[2], '*', '*', '*'].join('.')

      const submitJobData = {
        projectId: Number(projectId),
        name: values.jobName,
        sourceNs: sourceDataInfo,
        sinkNs: sinkDataInfo,
        sourceType: values.type,
        sourceConfig: `{"protocol":"${values.protocol}"}`
      }

      this.props.onAddJob(Object.assign(submitJobData, jobSparkConfigValues, requestCommon), () => {
        message.success(languageText === 'en' ? 'Job is created successfully!' : 'Job 添加成功！', 3)
      }, () => {
        this.hideJobSubmit()
      })
    } else if (jobMode === 'edit') {
      this.props.onEditJob(Object.assign(singleJobResult, jobSparkConfigValues, requestCommon, {
        sourceConfig: `{"protocol":"${values.protocol}"}`
      }), () => {
        message.success(languageText === 'en' ? 'Job is modified successfully!' : 'Job 修改成功！', 3)
      }, () => {
        this.hideJobSubmit()
      })
    }
  }

  hideJobSubmit = () => {
    this.setState({
      jobMode: '',
      jobTranTagClassName: '',
      jobTranTableClassName: 'hide',
      fieldSelected: 'hide',
      jobFormTranTableSource: []
    })
  }

  submitFlowForm = () => {
    const { streamDiffType } = this.state

    switch (streamDiffType) {
      case 'default':
        this.handleSubmitFlowDefault()
        break
      case 'hdfslog':
        this.handleSubmitFlowHdfslog()
        break
      case 'routing':
        this.handleSubmitFlowRouting()
        break
    }
  }

  handleSubmitFlowDefault () {
    const values = this.workbenchFlowForm.getFieldsValue()
    const { projectId, flowMode, singleFlowResult } = this.state
    const { resultFiledsOutput, dataframeShowOrNot, etpStrategyRequestValue, transformTableRequestValue, pushdownConnectRequestValue } = this.state
    const languageText = localStorage.getItem('preferredLanguage')

    let sinkConfigRequest = ''
    if (values.resultFields === 'all') {
      sinkConfigRequest = (!values.sinkConfig)
        ? ''
        : `{"sink_specific_config":${values.sinkConfig}}`
    } else {
      sinkConfigRequest = (!values.sinkConfig)
        ? JSON.stringify(resultFiledsOutput)
        : `{"sink_specific_config":${values.sinkConfig},"sink_output":"${values.resultFieldsSelected}"}`
    }

    let tranConfigRequest = {}
    if (!transformTableRequestValue['action']) {
      tranConfigRequest = ''
    } else {
      const objectTemp = Object.assign(etpStrategyRequestValue, transformTableRequestValue, pushdownConnectRequestValue, dataframeShowOrNot)
      const tranConfigRequestTemp = !values.flowSpecialConfig
        ? objectTemp
        : Object.assign(objectTemp, {'swifts_specific_config': JSON.parse(values.flowSpecialConfig)})
      tranConfigRequest = JSON.stringify(tranConfigRequestTemp)
    }

    if (flowMode === 'add' || flowMode === 'copy') {
      const { flowSourceResult } = this.state

      const insDBTable = [values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2]]
      const sourceDataSys = flowSourceResult.filter(s => [s.nsInstance, s.nsDatabase, s.nsTable].join(',') === insDBTable.join(','))
      const sourceDataInfo = [sourceDataSys[0].nsSys, values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2], '*', '*', '*'].join('.')
      const sinkDataInfo = [values.sinkDataSystem, values.sinkNamespace[0], values.sinkNamespace[1], values.sinkNamespace[2], '*', '*', '*'].join('.')

      const submitFlowData = {
        projectId: Number(projectId),
        streamId: Number(values.flowStreamId),
        sourceNs: sourceDataInfo,
        sinkNs: sinkDataInfo,
        consumedProtocol: values.protocol,
        sinkConfig: `${sinkConfigRequest}`,
        tranConfig: tranConfigRequest
      }

      this.props.onAddFlow(submitFlowData, () => {
        if (flowMode === 'add') {
          message.success(languageText === 'en' ? 'Flow is created successfully!' : 'Flow 添加成功！', 3)
        } else if (flowMode === 'copy') {
          message.success(languageText === 'en' ? 'Flow is copied successfully!' : 'Flow 复制成功！', 3)
        }
      }, () => {
        this.hideFlowSubmit()
        this.hideFlowDefaultSubmit()
      })
    } else if (flowMode === 'edit') {
      const editData = {
        sinkConfig: `${sinkConfigRequest}`,
        tranConfig: tranConfigRequest,
        consumedProtocol: values.protocol
      }

      this.props.onEditFlow(Object.assign(editData, singleFlowResult), () => {
        message.success(languageText === 'en' ? 'Flow is modified successfully!' : 'Flow 修改成功！', 3)
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
      dataframeShowSelected: 'hide',
      flowFormTranTableSource: []
    })
  }

  handleSubmitFlowHdfslog () {
    const { flowMode, projectId, singleFlowResult } = this.state
    const { sourceToSinkExited } = this.props

    this.workbenchFlowForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (flowMode === 'add' || flowMode === 'copy') {
          const sourceDataInfo = [values.sourceDataSystem, values.hdfslogNamespace[0], values.hdfslogNamespace[1], values.hdfslogNamespace[2], '*', '*', '*'].join('.')

          const submitFlowData = {
            projectId: Number(projectId),
            streamId: Number(values.flowStreamId),
            sourceNs: sourceDataInfo,
            sinkNs: sourceDataInfo,
            consumedProtocol: 'all',
            sinkConfig: '',
            tranConfig: ''
          }

          if (sourceToSinkExited) {
            message.error(operateLanguageSourceToSink(), 3)
          } else {
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
          }
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
    const { flowMode, projectId, singleFlowResult } = this.state
    const { sourceToSinkExited } = this.props

    this.workbenchFlowForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (flowMode === 'add' || flowMode === 'copy') {
          const { flowSourceResult } = this.state
          const insDBTable = [values.routingNamespace[0], values.routingNamespace[1], values.routingNamespace[2]]
          const sourceDataSys = flowSourceResult.filter(s => [s.nsInstance, s.nsDatabase, s.nsTable].join(',') === insDBTable.join(','))
          const sourceDataInfo = [sourceDataSys[0].nsSys, values.routingNamespace[0], values.routingNamespace[1], values.routingNamespace[2], '*', '*', '*'].join('.')
          const sinkDataInfo = ['kafka', values.routingSinkNs[0], values.routingSinkNs[1], values.routingSinkNs[2], '*', '*', '*'].join('.')

          const submitFlowData = {
            projectId: Number(projectId),
            streamId: Number(values.flowStreamId),
            sourceNs: sourceDataInfo,
            sinkNs: sinkDataInfo,
            consumedProtocol: 'all',
            sinkConfig: '',
            tranConfig: ''
          }

          if (sourceToSinkExited) {
            message.error(operateLanguageSourceToSink(), 3)
          } else {
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
          }
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
    const { streamNameExited } = this.props

    this.workbenchStreamForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (streamMode === 'add') {
          const requestValues = {
            name: values.streamName,
            desc: values.desc,
            instanceId: Number(values.kafka),
            streamType: values.type
          }

          if (streamNameExited) {
            this.workbenchStreamForm.setFields({
              streamName: {
                value: values.streamName,
                errors: [new Error(operateLanguageNameExist())]
              }
            })
            this.hideStreamSubmit()
          } else {
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
          }
        } else if (streamMode === 'edit') {
          const editValues = { desc: values.desc }
          const requestEditValues = Object.assign(editValues, streamQueryValues, streamConfigValues)

          this.props.onEditStream(requestEditValues, () => {
            message.success(operateLanguageSuccessMessage('Stream', 'modify'), 3)
            this.setState({ streamMode: '' })
            this.hideStreamSubmit()
          })
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
        }
      }
    })
  }

  // namespace 下拉框内容
  loadTransNs () {
    const { projectId, pipelineStreamId } = this.state
    const flowValues = this.workbenchFlowForm.getFieldsValue()
    const sourceDSVal = flowValues.sourceDataSystem
    const sourceNsVal = flowValues.sourceNamespace
    this.props.onLoadSourceSinkTypeNamespace(projectId, pipelineStreamId, sourceDSVal, 'sourceType', (result) => {
      const resultFinal = result.filter((i) => {
        const temp = [i.nsInstance, i.nsDatabase, i.nsTable]
        if (temp.join(',') !== sourceNsVal.join(',')) {
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

      if (this.state.jobTransValue === 'sparkSql') {
        this.makeJobSqlCodeMirrorInstance()
        this.cmJobSparkSql.doc.setValue(record.transformConfigInfo)
      } else if (this.state.jobTransValue === 'transformClassName') {
        this.jobTransformForm.setFieldsValue({ transformClassName: record.transformConfigInfo })
      }
    })
  }

  onAddTransform = (record) => (e) => {
    this.setState({
      transformMode: 'add',
      transformModalVisible: true,
      transformValue: ''
    }, () => {
      this.flowTransformForm.resetFields()
      this.flowTransformForm.setFieldsValue({ editTransformId: record.order })
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

  onTransformModalOk = () => {
    const { transformMode, transformSinkNamespaceArray } = this.state
    this.flowTransformForm.validateFieldsAndScroll((err, values) => {
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

              const pushdownConnectJson = tmp.connection_config === null
                ? {
                  name_space: `${tmp.nsSys}.${tmp.nsInstance}.${tmp.nsDatabase}`,
                  jdbc_url: tmp.conn_url,
                  username: tmp.user,
                  password: tmp.pwd
                }
                : {
                  name_space: `${tmp.nsSys}.${tmp.nsInstance}.${tmp.nsDatabase}`,
                  jdbc_url: tmp.conn_url,
                  username: tmp.user,
                  password: tmp.pwd,
                  connection_config: tmp.connection_config
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

  /**
   * Flow ETP Strategy Modal
   * */
  onShowEtpStrategyModal = () => {
    const { etpStrategyCheck, etpStrategyResponseValue } = this.state

    this.setState({
      etpStrategyModalVisible: true
    }, () => {
      if (etpStrategyCheck) {
        this.flowEtpStrategyForm.setFieldsValue({
          checkColumns: etpStrategyResponseValue.check_columns,
          checkRule: etpStrategyResponseValue.check_rule,
          ruleMode: etpStrategyResponseValue.rule_mode,
          ruleParams: etpStrategyResponseValue.rule_params,
          againstAction: etpStrategyResponseValue.against_action
        })
      } else {
        this.flowEtpStrategyForm.resetFields()
      }
    })
  }

  hideEtpStrategyModal = () => this.setState({ etpStrategyModalVisible: false })

  onEtpStrategyModalOk = () => {
    this.flowEtpStrategyForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const valueJson = {
          check_columns: values.checkColumns,
          check_rule: values.checkRule,
          rule_mode: values.ruleMode,
          rule_params: values.ruleParams,
          against_action: values.againstAction
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

  /**
   * Flow Sink Config Modal
   * */
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

  /**
   * Flow Transformation Config Modal
   * */
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

  /**
   * Job Transformation Config Modal
   */
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
        sinkConfig: this.cm.doc.getValue()
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

  /**
   * Job Sink Config Modal
   * */
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
      resultFieldsValue: 'all'
    }, () => {
      this.workbenchJobForm.setFieldsValue({
        type: 'hdfs_txt',
        resultFields: 'all'
      })
    })

    this.props.onLoadStreamConfigJvm((result) => {
      const othersInit = 'spark.locality.wait=10ms,spark.shuffle.spill.compress=false,spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec,spark.streaming.stopGracefullyOnShutdown=true,spark.scheduler.listenerbus.eventqueue.size=1000000,spark.sql.ui.retainedExecutions=3,spark.sql.shuffle.partitions=18'

      const startConfigJson = {
        driverCores: 1,
        driverMemory: 2,
        executorNums: 6,
        perExecutorMemory: 2,
        perExecutorCores: 1
      }

      this.setState({
        jobSparkConfigValues: {
          sparkConfig: `${result},${othersInit}`,
          startConfig: `${JSON.stringify(startConfigJson)}`
        }
      })
    })
  }

  // 验证 Job name 是否存在
  onInitJobNameValue = (value) => {
    this.props.onLoadJobName(this.state.projectId, value, () => {}, () => {
      this.workbenchJobForm.setFields({
        jobName: {
          value: value,
          errors: [new Error(operateLanguageNameExist())]
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
    const { flowMode, streamMode, jobMode, formStep, isWormhole } = this.state
    const { flowFormTranTableSource, jobFormTranTableSource } = this.state
    const { streams, projectNamespaces, streamSubmitLoading } = this.props

    const languagetext = localStorage.getItem('preferredLanguage')
    const sidebarPrefixes = {
      add: languagetext === 'zh' ? '新增' : 'Create',
      edit: languagetext === 'zh' ? '修改' : 'Modify',
      copy: languagetext === 'zh' ? '复制' : 'Copy'
    }

    const stepButtons = this.generateStepButtons()

    const paneHeight = document.documentElement.clientHeight - 64 - 50 - 48

    return (
      <div className="workbench-main-body">
        <Helmet title="Workbench" />
        <Tabs
          defaultActiveKey="flow"
          className="ri-tabs"
          animated={false}
          onChange={this.changeTag}
        >
          {/* Flow Panel */}
          <TabPane tab="Flow" key="flow" style={{height: `${paneHeight}px`}}>
            <div className="ri-workbench" style={{height: `${paneHeight}px`}}>
              <Flow
                className={flowMode ? 'op-mode' : ''}
                onShowAddFlow={this.showAddFlowWorkbench}
                onShowEditFlow={this.showEditFlowWorkbench}
                onShowCopyFlow={this.showCopyFlowWorkbench}
                projectIdGeted={this.state.projectId}
                flowClassHide={this.state.flowClassHide}
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
                    flowMode={this.state.flowMode}
                    projectIdGeted={this.state.projectId}

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
                    onInitSourceTypeNamespace={this.onInitSourceTypeNamespace}
                    onInitHdfslogNamespace={this.onInitHdfslogNamespace}
                    onInitRoutingNamespace={this.onInitRoutingNamespace}
                    onInitSinkTypeNamespace={this.onInitSinkTypeNamespace}
                    sourceTypeNamespaceData={this.state.sourceTypeNamespaceData}
                    hdfslogNsData={this.state.hdfslogNsData}
                    routingNsData={this.state.routingNsData}
                    sinkTypeNamespaceData={this.state.sinkTypeNamespaceData}
                    routingSinkTypeNsData={this.state.routingSinkTypeNsData}

                    resultFieldsValue={this.state.resultFieldsValue}
                    dataframeShowNumValue={this.state.dataframeShowNumValue}
                    etpStrategyConfirmValue={this.state.etpStrategyConfirmValue}
                    transConfigConfirmValue={this.state.transConfigConfirmValue}
                    transformTableConfirmValue={this.state.transformTableConfirmValue}

                    transformTableRequestValue={this.state.transformTableRequestValue}
                    streamDiffType={this.state.streamDiffType}
                    hdfslogSinkDataSysValue={this.state.hdfslogSinkDataSysValue}
                    hdfslogSinkNsValue={this.state.hdfslogSinkNsValue}
                    routingSourceNsValue={this.state.routingSourceNsValue}
                    routingSinkNsValue={this.state.routingSinkNsValue}
                    initialHdfslogCascader={this.initialHdfslogCascader}
                    initialRoutingCascader={this.initialRoutingCascader}
                    initialRoutingSinkCascader={this.initialRoutingSinkCascader}

                    flowKafkaInstanceValue={this.state.flowKafkaInstanceValue}
                    flowKafkaTopicValue={this.state.flowKafkaTopicValue}
                    sinkConfigCopy={this.state.sinkConfigCopy}

                    ref={(f) => { this.workbenchFlowForm = f }}
                  />
                  {/* Flow Transform Modal */}
                  <Modal
                    title="Transformation"
                    okText="保存"
                    wrapClassName="transform-form-style"
                    visible={this.state.transformModalVisible}
                    onOk={this.onTransformModalOk}
                    onCancel={this.hideTransformModal}>
                    <FlowTransformForm
                      ref={(f) => { this.flowTransformForm = f }}
                      projectIdGeted={this.state.projectId}
                      tabPanelKey={this.state.tabPanelKey}
                      flowTransNsData={this.state.flowTransNsData}
                      sinkNamespaces={projectNamespaces || []}
                      onInitTransformValue={this.onInitTransformValue}
                      transformValue={this.state.transformValue}
                      step2SinkNamespace={this.state.step2SinkNamespace}
                      step2SourceNamespace={this.state.step2SourceNamespace}
                      onInitTransformSinkTypeNamespace={this.onInitTransformSinkTypeNamespace}
                      transformSinkTypeNamespaceData={this.state.transformSinkTypeNamespaceData}
                    />
                  </Modal>
                  {/* Flow Sink Config Modal */}
                  <Modal
                    title="Sink Config"
                    okText="保存"
                    wrapClassName="ant-modal-large"
                    visible={this.state.sinkConfigModalVisible}
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
                    visible={this.state.flowSpecialConfigModalVisible}
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
                    visible={this.state.etpStrategyModalVisible}
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
                projectIdGeted={this.state.projectId}
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
                    onInitStreamNameValue={this.onInitStreamNameValue}
                    kafkaValues={this.state.kafkaValues}

                    onShowConfigModal={this.onShowConfigModal}
                    streamConfigCheck={this.state.streamConfigCheck}
                    topicEditValues={this.state.topicEditValues}

                    ref={(f) => { this.workbenchStreamForm = f }}
                  />
                  {/* Config Modal */}
                  <Modal
                    title="Configs"
                    okText="保存"
                    wrapClassName="ant-modal-large"
                    visible={this.state.streamConfigModalVisible}
                    onOk={this.onConfigModalOk}
                    onCancel={this.hideConfigModal}>
                    <StreamConfigForm
                      tabPanelKey={this.state.tabPanelKey}
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
                projectIdGeted={this.state.projectId}
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
                    projectIdGeted={this.state.projectId}
                    jobMode={this.state.jobMode}
                    sparkConfigCheck={this.state.sparkConfigCheck}
                    onShowSparkConfigModal={this.onShowSparkConfigModal}
                    fieldSelected={this.state.fieldSelected}
                    initResultFieldClass={this.initResultFieldClass}
                    onShowJobSinkConfigModal={this.onShowJobSinkConfigModal}
                    onInitJobNameValue={this.onInitJobNameValue}
                    onInitJobSourceNs={this.onInitJobSourceNs}
                    onInitJobSinkNs={this.onInitJobSinkNs}
                    sourceTypeNamespaceData={this.state.jobSourceNsData}
                    sinkTypeNamespaceData={this.state.jobSinkNsData}
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

                    ref={(f) => { this.workbenchJobForm = f }}
                  />
                  <Modal
                    title="Configs"
                    okText="保存"
                    wrapClassName="ant-modal-large"
                    visible={this.state.sparkConfigModalVisible}
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
                    visible={this.state.jobSinkConfigModalVisible}
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
                    visible={this.state.jobTransModalVisible}
                    onOk={this.onJobTransModalOk}
                    onCancel={this.hideJobTransModal}>
                    <JobTransformForm
                      ref={(f) => { this.jobTransformForm = f }}
                      projectIdGeted={this.state.projectId}
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
                    visible={this.state.jobSpecialConfigModalVisible}
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
                projectIdGeted={this.state.projectId}
                namespaceClassHide={this.state.namespaceClassHide}
              />
            </div>
          </TabPane>
          {/* User Panel */}
          <TabPane tab="User" key="user" style={{height: `${paneHeight}px`}}>
            <div className="ri-workbench" style={{height: `${paneHeight}px`}}>
              <User
                projectIdGeted={this.state.projectId}
                userClassHide={this.state.userClassHide}
              />
            </div>
          </TabPane>
          {/* Udf Panel */}
          <TabPane tab="UDF" key="udf" style={{height: `${paneHeight}px`}}>
            <div className="ri-workbench" style={{height: `${paneHeight}px`}}>
              <Udf
                projectIdGeted={this.state.projectId}
                udfClassHide={this.state.udfClassHide}
              />
            </div>
          </TabPane>
          {/* Resource Panel */}
          <TabPane tab="Resource" key="resource" style={{height: `${paneHeight}px`}}>
            <div className="ri-workbench" style={{height: `${paneHeight}px`}}>
              <Resource
                projectIdGeted={this.state.projectId}
              />
            </div>
          </TabPane>
        </Tabs>
      </div>
    )
  }
}

Workbench.propTypes = {
  streams: React.PropTypes.oneOfType([
    React.PropTypes.array,
    React.PropTypes.bool
  ]),
  streamSubmitLoading: React.PropTypes.bool,
  streamNameExited: React.PropTypes.bool,
  flowSubmitLoading: React.PropTypes.bool,
  sourceToSinkExited: React.PropTypes.bool,
  projectNamespaces: React.PropTypes.oneOfType([
    React.PropTypes.array,
    React.PropTypes.bool
  ]),
  router: React.PropTypes.any,
  onAddStream: React.PropTypes.func,
  onEditStream: React.PropTypes.func,
  onAddFlow: React.PropTypes.func,
  onEditFlow: React.PropTypes.func,
  onQueryFlow: React.PropTypes.func,
  onLoadkafka: React.PropTypes.func,
  onLoadStreamConfigJvm: React.PropTypes.func,
  onLoadStreamNameValue: React.PropTypes.func,
  onLoadStreamDetail: React.PropTypes.func,
  onLoadSelectStreamKafkaTopic: React.PropTypes.func,
  onLoadSourceSinkTypeNamespace: React.PropTypes.func,
  onLoadSinkTypeNamespace: React.PropTypes.func,
  onLoadTranSinkTypeNamespace: React.PropTypes.func,
  onLoadSourceToSinkExist: React.PropTypes.func,
  onLoadJobSourceToSinkExist: React.PropTypes.func,

  onLoadAdminSingleFlow: React.PropTypes.func,
  onLoadUserAllFlows: React.PropTypes.func,
  onLoadAdminSingleStream: React.PropTypes.func,
  onLoadUserStreams: React.PropTypes.func,
  onLoadUserNamespaces: React.PropTypes.func,
  onLoadSelectNamespaces: React.PropTypes.func,
  onLoadUserUsers: React.PropTypes.func,
  onLoadSelectUsers: React.PropTypes.func,
  onLoadResources: React.PropTypes.func,
  onLoadSingleUdf: React.PropTypes.func,
  onAddJob: React.PropTypes.func,
  onQueryJob: React.PropTypes.func,
  onEditJob: React.PropTypes.func,

  onLoadJobName: React.PropTypes.func,
  onLoadJobSourceNs: React.PropTypes.func,
  onLoadJobSinkNs: React.PropTypes.func,
  onChangeLanguage: React.PropTypes.func,
  onLoadLookupSql: React.PropTypes.func,
  jobNameExited: React.PropTypes.bool,
  jobSubmitLoading: React.PropTypes.bool
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
    onLoadStreamNameValue: (projectId, value, resolve, reject) => dispatch(loadStreamNameValue(projectId, value, resolve, reject)),
    onLoadStreamDetail: (projectId, streamId, roleType, resolve) => dispatch(loadStreamDetail(projectId, streamId, roleType, resolve)),
    onLoadSelectStreamKafkaTopic: (projectId, value, resolve) => dispatch(loadSelectStreamKafkaTopic(projectId, value, resolve)),

    onLoadSourceSinkTypeNamespace: (projectId, streamId, value, type, resolve) => dispatch(loadSourceSinkTypeNamespace(projectId, streamId, value, type, resolve)),
    onLoadSinkTypeNamespace: (projectId, streamId, value, type, resolve) => dispatch(loadSinkTypeNamespace(projectId, streamId, value, type, resolve)),
    onLoadTranSinkTypeNamespace: (projectId, streamId, value, type, resolve) => dispatch(loadTranSinkTypeNamespace(projectId, streamId, value, type, resolve)),
    onLoadSourceToSinkExist: (projectId, sourceNs, sinkNs, resolve, reject) => dispatch(loadSourceToSinkExist(projectId, sourceNs, sinkNs, resolve, reject)),
    onLoadJobSourceToSinkExist: (projectId, sourceNs, sinkNs, resolve, reject) => dispatch(loadJobSourceToSinkExist(projectId, sourceNs, sinkNs, resolve, reject)),
    onLoadJobName: (projectId, value, resolve, reject) => dispatch(loadJobName(projectId, value, resolve, reject)),
    onLoadJobSourceNs: (projectId, value, type, resolve, reject) => dispatch(loadJobSourceNs(projectId, value, type, resolve, reject)),
    onLoadJobSinkNs: (projectId, value, type, resolve, reject) => dispatch(loadJobSinkNs(projectId, value, type, resolve, reject)),
    onAddJob: (values, resolve, final) => dispatch(addJob(values, resolve, final)),
    onQueryJob: (values, resolve, final) => dispatch(queryJob(values, resolve, final)),
    onEditJob: (values, resolve, final) => dispatch(editJob(values, resolve, final)),
    onChangeLanguage: (type) => dispatch(changeLocale(type)),
    onLoadLookupSql: (values, resolve, reject) => dispatch(loadLookupSql(values, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  streams: selectStreams(),
  streamSubmitLoading: selectStreamSubmitLoading(),
  streamNameExited: selectStreamNameExited(),
  flows: selectFlows(),
  flowSubmitLoading: selectFlowSubmitLoading(),
  sourceToSinkExited: selectSourceToSinkExited(),
  namespaces: selectNamespaces(),
  users: selectUsers(),
  resources: selectResources(),
  projectNamespaces: selectProjectNamespaces(),
  jobNameExited: selectJobNameExited(),
  jobSourceToSinkExited: selectJobSourceToSinkExited()
})

export default connect(mapStateToProps, mapDispatchToProps)(Workbench)
