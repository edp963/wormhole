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
import CodeMirror from 'codemirror'
require('../../../node_modules/codemirror/addon/display/placeholder')
require('../../../node_modules/codemirror/mode/javascript/javascript')

import Flow from '../Flow'
import Manager from '../Manager'
import Namespace from '../Namespace'
import User from '../User'
import Udf from '../Udf'
import Resource from '../Resource'

import WorkbenchFlowForm from './WorkbenchFlowForm'
import WorkbenchStreamForm from './WorkbenchStreamForm'
import FlowEtpStrategyForm from './FlowEtpStrategyForm'
import FlowTransformForm from './FlowTransformForm'
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

import {loadUserAllFlows, loadAdminSingleFlow, loadSelectStreamKafkaTopic, loadSourceSinkTypeNamespace, loadSinkTypeNamespace, loadTranSinkTypeNamespace, loadSourceToSinkExist, addFlow, editFlow, queryFlow} from '../Flow/action'
import {loadUserStreams, loadAdminSingleStream, loadStreamNameValue, loadKafka, loadStreamConfigJvm, addStream, loadStreamDetail, editStream} from '../Manager/action'
import {loadSelectNamespaces, loadUserNamespaces} from '../Namespace/action'
import {loadUserUsers, loadSelectUsers} from '../User/action'
import {loadResources} from '../Resource/action'
import {loadSingleUdf} from '../Udf/action'

import { selectFlows, selectFlowSubmitLoading, selectSourceToSinkExited } from '../Flow/selectors'
import { selectStreams, selectStreamSubmitLoading, selectStreamNameExited } from '../Manager/selectors'
import { selectProjectNamespaces, selectNamespaces } from '../Namespace/selectors'
import { selectUsers } from '../User/selectors'
import { selectResources } from '../Resource/selectors'

export class Workbench extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      projectId: '',
      flowMode: '',
      streamMode: '',
      transformMode: '',
      formStep: 0,

      // all and parts of flow/stream/namespace/user
      userClassHide: 'hide',
      namespaceClassHide: 'hide',
      flowClassHide: 'hide',
      streamClassHide: 'hide',
      udfClassHide: 'hide',

      // Stream Form
      isWormhole: true,

      // Flow Modal
      transformModalVisible: false,
      etpStrategyModalVisible: false,
      sinkConfigModalVisible: false,

      // Flow Modal Transform
      flowFormTransformTableSource: [],
      transformTagClassName: '',
      transformTableClassName: 'hide',
      transformValue: '',
      transConnectClass: 'hide',

      step2SinkNamespace: '',
      step2SourceNamespace: '',

      // Flow Modal ETP Strategy
      etpStrategyCheck: false,

      // Stream Modal
      streamConfigModalVisible: false,

      // Stream Config
      streamConfigCheck: false,

      kafkaValues: [],
      kafkaInstanceId: 0,
      topicsValues: [],

      streamConfigValues: {
        sparkConfig: '',
        startConfig: '{"driverCores":1,"driverMemory":2,"executorNums":6,"perExecutorMemory":2,"perExecutorCores":1}',
        launchConfig: '{"durations": 10, "partitions": 6, "maxRecords": 600}'
      },
      streamQueryValues: {},

      // streamDagModalShow: 'hide',
      // flowDagModalShow: 'hide',

      selectStreamKafkaTopicValue: [],
      sourceTypeNamespaceData: [],
      hdfslogNsData: [],
      sinkTypeNamespaceData: [],
      transformSinkTypeNamespaceData: [],

      sourceNamespaceArray: [],
      hdfslogNsArray: [],
      sinkNamespaceArray: [],
      transformSinkNamespaceArray: [],

      // request data
      resultFiledsOutput: '',
      dataframeShowOrNot: '',
      etpStrategyRequestValue: '',
      transformTableRequestValue: '',
      pushdownConnectRequestValue: '',

      // add flow confirmation data
      resultFieldsValue: 'all',
      dataframeShowNumValue: 'false',
      etpStrategyConfirmValue: '',
      transformTableConfirmValue: '',

      // flow response data
      etpStrategyResponseValue: '',

      topicEditValues: [],

      responseTopicInfo: [],
      fieldSelected: 'hide',
      dataframeShowSelected: 'hide',

      singleFlowResult: {},
      streamDiffType: 'default',
      pipelineStreamId: 0,
      hdfslogSinkDataSysValue: '',
      hdfslogSinkNsValue: '',

      flowKafkaInstanceValue: '',
      flowKafkaTopicValue: '',

      sinkConfigMsg: ''
    }
  }

  componentWillMount () {
    const projectId = this.props.router.params.projectId
    this.loadData(projectId)
  }

  componentWillReceiveProps (props) {
    const projectId = props.router.params.projectId
    if (projectId !== this.state.projectId) {
      this.loadData(projectId)
    }
  }

  loadData (projectId) {
    this.setState({
      projectId: projectId
    })
  }

  changeTag = (key) => {
    const { projectId } = this.state
    const { onLoadAdminSingleFlow, onLoadUserAllFlows, onLoadAdminSingleStream, onLoadUserStreams } = this.props
    const { onLoadSelectNamespaces, onLoadUserNamespaces, onLoadSelectUsers, onLoadUserUsers, onLoadResources, onLoadSingleUdf } = this.props
    let roleTypeTemp = localStorage.getItem('loginRoleType')

    if (key === 'flow') {
      if (roleTypeTemp === 'admin') {
        onLoadAdminSingleFlow(projectId, () => {})
      } else if (roleTypeTemp === 'user') {
        onLoadUserAllFlows(projectId, () => {})
      }
    } else if (key === 'stream') {
      if (roleTypeTemp === 'admin') {
        onLoadAdminSingleStream(projectId, () => {})
      } else if (roleTypeTemp === 'user') {
        onLoadUserStreams(projectId, () => {})
      }
    } else if (key === 'namespace') {
      if (roleTypeTemp === 'admin') {
        onLoadSelectNamespaces(projectId, () => {})
      } else if (roleTypeTemp === 'user') {
        onLoadUserNamespaces(projectId, () => {})
      }
    } else if (key === 'user') {
      if (roleTypeTemp === 'admin') {
        onLoadSelectUsers(projectId, () => {})
      } else if (roleTypeTemp === 'user') {
        onLoadUserUsers(projectId, () => {})
      }
    } else if (key === 'resource') {
      if (roleTypeTemp === 'admin') {
        onLoadResources(projectId, 'admin')
      } else if (roleTypeTemp === 'user') {
        onLoadResources(projectId, 'user')
      }
    } else if (key === 'udf') {
      if (roleTypeTemp === 'admin') {
        onLoadSingleUdf(projectId, 'admin', () => {})
      } else if (roleTypeTemp === 'user') {
        onLoadSingleUdf(projectId, 'user', () => {})
      }
    }
  }

  /***
   * 新增Stream时，通过验证 stream name 是否存在
   * */
  onInitStreamNameValue = (value) => {
    this.props.onLoadStreamNameValue(this.state.projectId, value, () => {}, () => {
      this.workbenchStreamForm.setFields({
        streamName: {
          value: value,
          errors: [new Error('该 Name 已存在')]
        }
      })
    })
  }

  /**
   * hdfslog namespace
   * */
  initialHdfslogCascader = (value) => this.setState({ hdfslogSinkNsValue: value.join('.') })
  /**
   * 新增Flow时，获取 default type source namespace 下拉框
   * */
  onInitSourceTypeNamespace = (projectId, value, type) => {
    const { flowMode, pipelineStreamId } = this.state

    this.setState({ sourceTypeNamespaceData: [] })

    if (pipelineStreamId !== 0) {
      this.props.onLoadSourceSinkTypeNamespace(projectId, pipelineStreamId, value, type, (result) => {
        this.setState({
          sourceNamespaceArray: result,
          sourceTypeNamespaceData: this.generateSourceSinkNamespaceHierarchy(value, result)
        })
        // default source ns 和 sink ns 同时调同一个接口获得，保证两处的 placeholder 和单条数据回显都能正常
        if (flowMode === 'add' || flowMode === 'copy') {
          this.workbenchFlowForm.setFieldsValue({
            sourceNamespace: undefined
          })
        }
      })
    }
  }

  /**
   * 新增Flow时，获取 hdfslog type source namespace 下拉框
   * */
  onInitHdfslogNamespace = (projectId, value, type) => {
    const { pipelineStreamId } = this.state

    this.setState({
      hdfslogSinkNsValue: '',
      hdfslogNsData: []
    })
    if (pipelineStreamId !== 0) {
      this.props.onLoadSourceSinkTypeNamespace(projectId, pipelineStreamId, value, type, (result) => {
        this.setState({
          hdfslogNsArray: result,
          hdfslogNsData: this.generateHdfslogNamespaceHierarchy(value, result),
          hdfslogSinkDataSysValue: value
        })
      })
    }
  }

  /**
   * 新增Flow时，获取 default type sink namespace 下拉框
   * */
  onInitSinkTypeNamespace = (projectId, value, type) => {
    const { flowMode, pipelineStreamId } = this.state

    let sinkConfigMsgTemp = ''
    if (value === 'hbase') {
      sinkConfigMsgTemp = 'For example: {"sink_specific_config":{"hbase.columnFamily":"cf","hbase.saveAsString": true, "hbase.rowKey":[{"name":"id","pattern":"mod_64_2"}, {"name":"name","pattern":"value"}, {"name":"address","pattern":"hash"}, {"name": "name", "pattern": "reverse"}]}}'
    } else if (value === 'mysql' || value === 'oracle' || value === 'postgresql') {
      sinkConfigMsgTemp = 'For example: {"sink_specific_config":{"db.mutation_type":"iud","db.sql_batch_size": 100}}'
    } else if (value === 'es') {
      sinkConfigMsgTemp = 'For example: {"sink_specific_config":{"es.mutation_type":"iud"}}'
    } else if (value === 'phoenix') {
      sinkConfigMsgTemp = 'For example: {"sink_specific_config":{"db.sql_batch_size": 100}}'
    } else {
      sinkConfigMsgTemp = ''
    }

    this.setState({
      sinkTypeNamespaceData: [],
      sinkConfigMsg: sinkConfigMsgTemp
    })
    if (pipelineStreamId !== 0) {
      this.props.onLoadSinkTypeNamespace(projectId, pipelineStreamId, value, type, (result) => {
        this.setState({
          sinkNamespaceArray: result,
          sinkTypeNamespaceData: this.generateSourceSinkNamespaceHierarchy(value, result)
        })
        if (flowMode === 'add' || flowMode === 'copy') {
          this.workbenchFlowForm.setFieldsValue({
            sinkNamespace: undefined
          })
        }
      })
    }
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
          transformSinkTypeNamespaceData: this.generateTransformSinkNamespaceHierarchy(value, result)
        })
      })
    }
  }

  /**
   * 生成 step1 的 Source/Sink Namespace Cascader 所需数据源
   */
  generateSourceSinkNamespaceHierarchy = (system, result) => {
    const snsHierarchy = []
    result.forEach(item => {
      if (item.nsSys === system) {
        let instance = snsHierarchy.find(i => i.value === item.nsInstance)
        if (!instance) {
          const newInstance = {
            value: item.nsInstance,
            label: item.nsInstance,
            children: []
          }
          snsHierarchy.push(newInstance)
          instance = newInstance
        }

        let database = instance.children.find(i => i.value === item.nsDatabase)
        if (!database) {
          const newDatabase = {
            value: item.nsDatabase,
            label: item.nsDatabase,
            children: []
          }
          instance.children.push(newDatabase)
          database = newDatabase
        }

        let table = database.children.find(i => i.value === item.nsTable)
        if (!table) {
          const newTable = {
            value: item.nsTable,
            label: item.nsTable
          }
          database.children.push(newTable)
        }
      }
    })
    return snsHierarchy
  }

  /**
   * 生成 step1 的 Hdfslog Source/Sink Namespace Cascader 所需数据源
   */
  generateHdfslogNamespaceHierarchy = (system, result) => {
    const snsHierarchy = result.length === 0
      ? []
      : [{
        value: '*',
        label: '*',
        children: [{
          value: '*',
          label: '*',
          children: [{
            value: '*',
            label: '*'
          }]
        }]
      }]

    result.forEach(item => {
      if (item.nsSys === system) {
        let instance = snsHierarchy.find(i => i.value === item.nsInstance)
        if (!instance) {
          const newInstance = {
            value: item.nsInstance,
            label: item.nsInstance,
            children: [{
              value: '*',
              label: '*',
              children: [{
                value: '*',
                label: '*'
              }]
            }]
          }
          snsHierarchy.push(newInstance)
          instance = newInstance
        }

        let database = instance.children.find(i => i.value === item.nsDatabase)
        if (!database) {
          const newDatabase = {
            value: item.nsDatabase,
            label: item.nsDatabase,
            children: [{
              value: '*',
              label: '*'
            }]
          }
          instance.children.push(newDatabase)
          database = newDatabase
        }

        let table = database.children.find(i => i.value === item.nsTable)
        if (!table) {
          const newTable = {
            value: item.nsTable,
            label: item.nsTable
          }
          database.children.push(newTable)
        }
      }
    })
    return snsHierarchy
  }

  /**
   * 生成 transformation 中 的 Sink Namespace Cascader 所需数据源
   */
  generateTransformSinkNamespaceHierarchy = (system, result) => {
    const snsHierarchy = []
    result.forEach(item => {
      if (item.nsSys === system) {
        let instance = snsHierarchy.find(i => i.value === item.nsInstance)
        if (!instance) {
          const newInstance = {
            value: item.nsInstance,
            label: item.nsInstance,
            children: []
          }
          snsHierarchy.push(newInstance)
          instance = newInstance
        }

        let database = instance.children.find(i => i.value === item.nsDatabase)
        if (!database) {
          const newDatabase = {
            value: item.nsDatabase,
            label: item.nsDatabase
            // children: []
          }
          instance.children.push(newDatabase)
          // database = newDatabase
        }

        // let permission = database.children.find(i => i.value === item.permission)
        // if (!permission) {
        //   const newPermission = {
        //     value: item.permission,
        //     label: item.permission
        //   }
        //   database.children.push(newPermission)
        // }
      }
    })
    return snsHierarchy
  }

  // 控制 result field show／hide
  initResultFieldClass = (e) => {
    if (e.target.value === 'selected') {
      this.setState({ fieldSelected: '' })
    } else if (e.target.value === 'all') {
      this.setState({ fieldSelected: 'hide' })
    }
  }

  // 控制 data frame number show／hide
  initDataShowClass = (e) => {
    if (e.target.value === 'true') {
      this.setState({ dataframeShowSelected: '' })
    } else {
      this.setState({ dataframeShowSelected: 'hide' })
    }
  }

  showAddFlowWorkbench = () => {
    this.workbenchFlowForm.resetFields()
    this.setState({
      flowMode: 'add',
      formStep: 0,
      flowFormTransformTableSource: [],
      transformTagClassName: '',
      transformTableClassName: 'hide',
      transConnectClass: 'hide',
      fieldSelected: 'hide',
      etpStrategyCheck: false,
      dataframeShowSelected: 'hide',
      resultFieldsValue: 'all',
      etpStrategyConfirmValue: '',
      etpStrategyRequestValue: ''
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
    if (val === 'default') {
      this.setState({ streamDiffType: 'default' })
    } else if (val === 'hdfslog') {
      this.setState({ streamDiffType: 'hdfslog' })
    }

    // 显示 Stream 信息
    this.props.onLoadSelectStreamKafkaTopic(this.state.projectId, val, (result) => {
      const resultFinal = result.map(s => {
        const responseResult = Object.assign({}, s.stream, {
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
        message.warning('请先新建相应类型的 Stream！', 3)
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

  onInitStreamNameSelect = (valId) => {
    const { streamDiffType, selectStreamKafkaTopicValue } = this.state

    const selName = selectStreamKafkaTopicValue.find(s => s.id === Number(valId))
    const topicTemp = selName.topicInfo
    this.setState({
      pipelineStreamId: Number(valId),
      flowKafkaInstanceValue: selName.instance,
      flowKafkaTopicValue: topicTemp.map(j => j.name).join(',')
    })

    if (streamDiffType === 'default') {
      this.workbenchFlowForm.setFieldsValue({
        flowStreamId: Number(valId),
        sourceDataSystem: '',
        sinkDataSystem: '',
        sourceNamespace: undefined,
        sinkNamespace: undefined
      })
    } else if (streamDiffType === 'hdfslog') {
      this.setState({
        hdfslogSinkDataSysValue: '',
        hdfslogSinkNsValue: ''
      })
      this.workbenchFlowForm.setFieldsValue({
        sourceDataSystem: '',
        hdfslogNamespace: undefined
      })
    }
  }

  showCopyFlowWorkbench = (flow) => {
    this.setState({ flowMode: 'copy' })
    this.workbenchFlowForm.resetFields()
    this.queryFlowInfo(flow)
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

  /**
   *  Flow 调单条查询的接口，回显数据
   * */
  queryFlowInfo = (flow) => {
    this.setState({
      streamDiffType: flow.streamType
    }, () => {
      if (flow.streamType === 'default') {
        this.queryFlowDefault(flow)
      } else if (flow.streamType === 'hdfslog') {
        this.queryFlowHdfslog(flow)
      }
    })
  }

  queryFlowDefault (flow) {
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
            updateBy: result.updateBy
          }
        })
      })
    })
      .then((result) => {
        const sourceNsArr = result.sourceNs.split('.')
        const sinkNsArr = result.sinkNs.split('.')

        let resultFieldsVal = ''
        let dataframeShowVal = ''

        if (result.tranConfig !== '') {
          const tranConfigVal = JSON.parse(JSON.parse(JSON.stringify(result.tranConfig)))

          if (result.tranConfig.indexOf('output') < 0) {
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
                resultFieldsSelected: tranConfigVal.output,
                resultFields: 'selected'
              })
            })
          }

          if (result.tranConfig.indexOf('action') > 0) {
            let validityTemp = tranConfigVal.validity

            if (result.tranConfig.indexOf('validity') > 0) {
              this.setState({
                etpStrategyCheck: true,
                etpStrategyResponseValue: validityTemp,
                etpStrategyRequestValue: `"validity":{"check_columns":"${validityTemp.check_columns}","check_rule":"${validityTemp.check_rule}","rule_mode":"${validityTemp.rule_mode}","rule_params":"${validityTemp.rule_params}","against_action":"${validityTemp.against_action}"}`,
                etpStrategyConfirmValue: `"check_columns":"${validityTemp.check_columns}","check_rule":"${validityTemp.check_rule}","rule_mode":"${validityTemp.rule_mode}","rule_params":"${validityTemp.rule_params}","against_action":"${validityTemp.against_action}"`
              })
            } else {
              this.setState({
                etpStrategyCheck: false,
                etpStrategyResponseValue: ''
              })
            }

            if (result.tranConfig.indexOf('dataframe_show_num') > 0) {
              dataframeShowVal = 'true'
              this.setState({
                dataframeShowSelected: ''
              }, () => {
                this.workbenchFlowForm.setFieldsValue({
                  dataframeShowNum: tranConfigVal.dataframe_show_num
                })
              })
            } else {
              dataframeShowVal = 'false'
              this.setState({
                dataframeShowSelected: 'hide'
              })
              this.workbenchFlowForm.setFieldsValue({
                dataframeShow: 'false',
                dataframeShowNum: 10
              })
            }

            const tranActionArr = tranConfigVal.action.split(';')
            tranActionArr.splice(tranActionArr.length - 1, 1)

            this.state.flowFormTransformTableSource = tranActionArr.map((i, index) => {
              const tranTableSourceTemp = {}
              let tranConfigInfoTemp = ''
              let tranTypeTepm = ''
              let pushdownConTepm = ''

              if (i.indexOf('pushdown_sql') > 0 || i.indexOf('pushdown_sql') === 0) {
                if (i.indexOf('left join') > 0) {
                  i = i.replace('left join', 'leftJoin')
                }
                if (i.indexOf('inner join') > 0) {
                  i = i.replace('inner join', 'innerJoin')
                }
                const lookupBeforePart = i.substring(0, i.indexOf('=') - 1)
                const lookupAfterPart = i.substring(i.indexOf('=') + 1)
                const lookupBeforePartTemp = (lookupBeforePart.replace(/(^\s*)|(\s*$)/g, '')).split(' ')
                const lookupAfterPartTepm = lookupAfterPart.replace(/(^\s*)|(\s*$)/g, '') // 去字符串前后的空白；sql语句回显

                tranConfigInfoTemp = [lookupBeforePartTemp[1], lookupBeforePartTemp[3], lookupAfterPartTepm].join('.')
                tranTypeTepm = 'lookupSql'

                const pushdownConTepmObj = tranConfigVal.pushdown_connection.find(g => g.name_space === lookupBeforePartTemp[3])
                pushdownConTepm = `{"name_space":"${pushdownConTepmObj.name_space}","jdbc_url":"${pushdownConTepmObj.jdbc_url}","username":"${pushdownConTepmObj.username}","password":"${pushdownConTepmObj.password}"}`
              }

              if (i.indexOf('parquet_sql') > 0 || i.indexOf('parquet_sql') === 0) {
                if (i.indexOf('left join') > 0) {
                  i = i.replace('left join', 'leftJoin')
                }
                if (i.indexOf('right join') > 0) {
                  i = i.replace('right join', 'rightJoin')
                }
                if (i.indexOf('inner join') > 0) {
                  i = i.replace('inner join', 'innerJoin')
                }

                const streamJoinBeforePart = i.substring(0, i.indexOf('=') - 1)
                const streamJoinAfterPart = i.substring(i.indexOf('=') + 1)
                const streamJoinBeforePartTemp = streamJoinBeforePart.replace(/(^\s*)|(\s*$)/g, '').split(' ')
                const streamJoinAfterPartTepm = streamJoinAfterPart.replace(/(^\s*)|(\s*$)/g, '')

                const iTemp3Temp = streamJoinBeforePartTemp[3].substring(streamJoinBeforePartTemp[3].indexOf('(') + 1)
                const iTemp3Val = iTemp3Temp.substring(0, iTemp3Temp.indexOf(')'))
                tranConfigInfoTemp = [streamJoinBeforePartTemp[1], iTemp3Val, streamJoinAfterPartTepm].join('.')
                tranTypeTepm = 'streamJoinSql'
                pushdownConTepm = ''
              }

              if (i.indexOf('spark_sql') > 0 || i.indexOf('spark_sql') === 0) {
                const sparkAfterPart = i.substring(i.indexOf('=') + 1)
                const sparkAfterPartTepm = sparkAfterPart.replace(/(^\s*)|(\s*$)/g, '')

                tranConfigInfoTemp = sparkAfterPartTepm
                tranTypeTepm = 'sparkSql'
                pushdownConTepm = ''
              }

              if (i.indexOf('custom_class') > 0 || i.indexOf('custom_class') === 0) {
                const sparkAfterPart = i.substring(i.indexOf('=') + 1)
                const sparkAfterPartTepm = sparkAfterPart.replace(/(^\s*)|(\s*$)/g, '')

                tranConfigInfoTemp = sparkAfterPartTepm
                tranTypeTepm = 'transformClassName'
                pushdownConTepm = ''
              }

              tranTableSourceTemp.order = index + 1
              tranTableSourceTemp.transformConfigInfo = `${tranConfigInfoTemp};`
              tranTableSourceTemp.transformConfigInfoRequest = `${i};`
              tranTableSourceTemp.transformType = tranTypeTepm
              tranTableSourceTemp.pushdownConnection = pushdownConTepm
              // tranTableSourceTemp.key = index
              // tranTableSourceTemp.visible = false
              return tranTableSourceTemp
            })

            this.setState({
              transformTagClassName: 'hide',
              transformTableClassName: '',
              transConnectClass: ''
            })
          } else {
            this.setState({
              transformTagClassName: '',
              transformTableClassName: 'hide',
              transConnectClass: 'hide',
              etpStrategyCheck: false,
              dataframeShowSelected: 'hide'
            })
          }
        } else {
          this.setState({
            fieldSelected: 'hide',
            transformTagClassName: '',
            transformTableClassName: 'hide',
            transConnectClass: 'hide',
            etpStrategyCheck: false,
            dataframeShowSelected: 'hide'
          })
        }

        this.workbenchFlowForm.setFieldsValue({
          sourceDataSystem: sourceNsArr[0],
          sourceNamespace: [
            sourceNsArr[1],
            sourceNsArr[2],
            sourceNsArr[3]
          ],
          sinkDataSystem: sinkNsArr[0],
          sinkNamespace: [
            sinkNsArr[1],
            sinkNsArr[2],
            sinkNsArr[3]
          ],

          sinkConfig: result.sinkConfig,
          resultFields: resultFieldsVal,
          dataframeShow: dataframeShowVal
        })
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

        this.setState({
          formStep: 0,
          pipelineStreamId: result.streamId,
          hdfslogSinkNsValue: this.state.flowMode === 'copy' ? '' : result.sinkNs,
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
            updateBy: result.updateBy
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

  showAddStreamWorkbench = () => {
    this.workbenchStreamForm.resetFields()
    // 显示 jvm 数据，从而获得初始的 sparkConfig
    this.setState({
      streamMode: 'add',
      streamConfigCheck: false
    })

    this.props.onLoadStreamConfigJvm((result) => {
      const othersInit = 'spark.locality.wait=10ms,spark.shuffle.spill.compress=false,spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec,spark.streaming.stopGracefullyOnShutdown=true,spark.scheduler.listenerbus.eventqueue.size=1000000,spark.sql.ui.retainedExecutions=3'
      this.setState({
        streamConfigValues: {
          sparkConfig: `${result},${othersInit}`,
          startConfig: '{"driverCores":1,"driverMemory":2,"executorNums":6,"perExecutorMemory":2,"perExecutorCores":1}',
          launchConfig: '{"durations": 10, "partitions": 6, "maxRecords": 600}'
        }
      })
    })

    // 显示 Kafka
    this.props.onLoadkafka(this.state.projectId, 'kafka', (result) => {
      this.setState({ kafkaValues: result })
    })
  }

  showEditStreamWorkbench = (stream) => () => {
    this.setState({
      streamMode: 'edit',
      streamConfigCheck: true
    })
    this.workbenchStreamForm.resetFields()

    this.props.onLoadStreamDetail(this.state.projectId, stream.id, 'user', (result) => {
      const resultVal = Object.assign({}, result.stream, {
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

  hideStreamWorkbench = () => this.setState({ streamMode: '' })

  /**
   * Stream Config Modal
   * */
  onShowConfigModal = () => {
    const { streamConfigValues } = this.state
    this.setState({
      streamConfigModalVisible: true
    }, () => {
      // 点击 config 按钮时，回显 stream config 数据

      // let sparkConArr = []
      // if (streamConfigValues.sparkConfig.indexOf(',') < 0) {
      //   // 只有一条 jvm 且没有 others
      //   sparkConArr = [streamConfigValues.sparkConfig]
      // } else {
      //   sparkConArr = streamConfigValues.sparkConfig.split(',')
      // }

      // 有且只有2条 jvm 配置
      const sparkConArr = streamConfigValues.sparkConfig.split(',')

      let tempJvmArr = []
      let tempOthersArr = []
      for (let i = 0; i < sparkConArr.length; i++) {
        if (sparkConArr[i].indexOf('extraJavaOptions') > 0 || sparkConArr[i].indexOf('extraJavaOptions') === 0) {
          tempJvmArr.push(sparkConArr[i])   // 是 jvm
        } else {
          tempOthersArr.push(sparkConArr[i]) // 非 jvm
        }
      }

      const jvmTempValue = tempJvmArr.join('\n')
      const personalConfTempValue = tempOthersArr.join('\n')

      // JSON字符串转换成JSON对象
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

  hideConfigModal = () => this.setState({ streamConfigModalVisible: false })

  onConfigModalOk = () => {
    this.streamConfigForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        values.personalConf = values.personalConf.trim()
        values.jvm = values.jvm.trim()

        const nJvm = (values.jvm.split('extraJavaOptions')).length - 1
        let jvmValTemp = ''
        if (nJvm === 2) {
          jvmValTemp = values.jvm.replace(/\n/g, ',')

          let sparkConfigValue = ''
          if (values.personalConf === undefined || values.personalConf === '') {
            sparkConfigValue = jvmValTemp
          } else {
            const nOthers = (values.jvm.split('=')).length - 1

            let personalConfTemp = ''
            if (nOthers === 1) {
              personalConfTemp = values.personalConf
            } else {
              personalConfTemp = values.personalConf.replace(/\n/g, ',')
            }
            sparkConfigValue = `${jvmValTemp},${personalConfTemp}`
          }

          this.setState({
            streamConfigCheck: true,
            streamConfigValues: {
              sparkConfig: sparkConfigValue,
              startConfig: `{"driverCores":${values.driverCores},"driverMemory":${values.driverMemory},"executorNums":${values.executorNums},"perExecutorMemory":${values.perExecutorMemory},"perExecutorCores":${values.perExecutorCores}}`,
              launchConfig: `{"durations": ${values.durations}, "partitions": ${values.partitions}, "maxRecords": ${values.maxRecords}}`
            }
          })
          this.hideConfigModal()
        } else {
          message.warning('请正确配置 JVM！', 3)
        }
      }
    })
  }

  hideFlowWorkbench = () => this.setState({ flowMode: '' })

  /**
   *  JSON 格式校验
   *  如果JSON.parse能转换成功；并且字符串中包含 { 时，那么该字符串就是JSON格式的字符串。
   *  另：sink config 可为空
   */
  isJSON (str) {
    if (typeof str === 'string') {
      if (str === '') {
        return true
      } else {
        try {
          JSON.parse(str)
          if (str.indexOf('{') > -1) {
            return true
          } else {
            return false
          }
        } catch (e) {
          return false
        }
      }
    }
    return false
  }

  forwardStep = () => {
    if (this.state.streamDiffType === 'default') {
      this.handleForwardDefault()
    } else if (this.state.streamDiffType === 'hdfslog') {
      this.handleForwardHdfslog()
    }
  }

  // 验证 source to sink 存在性
  loadSTSExit (values) {
    const { flowMode } = this.state

    if (flowMode === 'add' || flowMode === 'copy') {
      // 新增flow时验证source to sink 是否存在
      const sourceInfo = [values.sourceDataSystem, values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2], '*', '*', '*'].join('.')
      const sinkInfo = [values.sinkDataSystem, values.sinkNamespace[0], values.sinkNamespace[1], values.sinkNamespace[2], '*', '*', '*'].join('.')

      this.props.onLoadSourceToSinkExist(this.state.projectId, sourceInfo, sinkInfo, () => {
        this.setState({
          formStep: this.state.formStep + 1,
          step2SourceNamespace: [values.sourceDataSystem, values.sourceNamespace.join('.')].join('.'),
          step2SinkNamespace: [values.sinkDataSystem, values.sinkNamespace.join('.')].join('.')
        })
      }, () => {
        message.error('Source to Sink 已存在！', 3)
      })
    } else if (flowMode === 'edit') {
      this.setState({
        formStep: this.state.formStep + 1,
        step2SourceNamespace: [values.sourceDataSystem, values.sourceNamespace.join('.')].join('.'),
        step2SinkNamespace: [values.sinkDataSystem, values.sinkNamespace.join('.')].join('.')
      })
    }
  }

  handleForwardDefault () {
    const { flowFormTransformTableSource, streamDiffType } = this.state

    let transformRequestTempArr = []
    flowFormTransformTableSource.map(i => transformRequestTempArr.push(i.transformConfigInfoRequest))
    const transformRequestTempString = transformRequestTempArr.join('')
    this.setState({
      transformTableRequestValue: transformRequestTempString === '' ? '' : `"action": "${transformRequestTempString}"`,
      transformTableConfirmValue: transformRequestTempString === '' ? '' : `"${transformRequestTempString}"`
    })

    // 只有 lookup sql 才有 pushdownConnection
    let tempSource = flowFormTransformTableSource.filter(s => s.pushdownConnection !== '')

    let pushdownConnectionTempString = []
    for (let i = 0; i < tempSource.length; i++) {
      pushdownConnectionTempString.push(tempSource[i].pushdownConnection)
    }

    this.setState({
      pushdownConnectRequestValue: pushdownConnectionTempString === '' ? '' : `"pushdown_connection":[${pushdownConnectionTempString}],`
    })

    this.workbenchFlowForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        switch (this.state.formStep) {
          case 0:
            const values = this.workbenchFlowForm.getFieldsValue()
            if (values.sinkConfig === undefined || values.sinkConfig === '') {
              // 是否是 hbase/mysql/oracle.postgresql
              if (values.sinkDataSystem === 'hbase' || values.sinkDataSystem === 'mysql' || values.sinkDataSystem === 'oracle' || values.sinkDataSystem === 'postgresql') {
                message.error(`Data System 为 ${values.sinkDataSystem} 时，Sink Config 不能为空！`, 3)
              } else {
                this.loadSTSExit(values)
              }
            } else {
              // json 校验
              if (this.isJSON(values.sinkConfig) === false) {
                message.error('Sink Config 应为 JSON格式！', 3)
                return
              } else {
                this.loadSTSExit(values)
              }
            }
            break
          case 1:
            if (streamDiffType === 'default') {
              const rfSelect = this.workbenchFlowForm.getFieldValue('resultFields')
              if (rfSelect === 'all') {
                this.setState({
                  resultFiledsOutput: '',
                  resultFieldsValue: 'all'
                })
              } else if (rfSelect === 'selected') {
                const rfSelectSelected = this.workbenchFlowForm.getFieldValue('resultFieldsSelected')
                this.setState({
                  resultFiledsOutput: `"output":"${rfSelectSelected}"`,
                  resultFieldsValue: rfSelectSelected
                })
              }

              const dataframeShowSelect = this.workbenchFlowForm.getFieldValue('dataframeShow')
              if (dataframeShowSelect === 'true') {
                const dataframeShowNum = this.workbenchFlowForm.getFieldValue('dataframeShowNum')
                this.setState({
                  dataframeShowOrNot: `"dataframe_show":"true","dataframe_show_num":"${dataframeShowNum}","swifts_specific_config":""`,
                  dataframeShowNumValue: `true; Number is ${dataframeShowNum}`
                })
              } else {
                this.setState({
                  dataframeShowOrNot: `"dataframe_show":"false","swifts_specific_config":""`,
                  dataframeShowNumValue: 'false'
                })
              }
              this.setState({
                formStep: this.state.formStep + 1
              })
            } else if (streamDiffType === 'hdfslog') {
              this.setState({
                formStep: this.state.formStep + 2
              })
            }
            break
        }
      }
    })
  }

  handleForwardHdfslog () {
    const { flowMode, projectId } = this.state

    this.workbenchFlowForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (flowMode === 'add' || flowMode === 'copy') {
          // 新增flow时验证source to sink 是否存在
          const sourceInfo = [values.sourceDataSystem, values.hdfslogNamespace[0], values.hdfslogNamespace[1], values.hdfslogNamespace[2], '*', '*', '*'].join('.')
          const sinkInfo = sourceInfo
          this.props.onLoadSourceToSinkExist(projectId, sourceInfo, sinkInfo, () => {
            this.setState({ formStep: this.state.formStep + 2 })
          }, () => {
            message.error('Source to Sink 已存在！', 3)
          })
        } else if (flowMode === 'edit') {
          this.setState({ formStep: this.state.formStep + 2 })
        }
      }
    })
  }

  backwardStep = () => {
    const { streamDiffType, formStep } = this.state
    if (streamDiffType === 'default') {
      this.setState({ formStep: formStep - 1 })
    } else if (streamDiffType === 'hdfslog') {
      this.setState({ formStep: formStep - 2 })
    }
  }

  generateStepButtons = () => {
    switch (this.state.formStep) {
      case 0:
        return (
          <div className="ri-workbench-step-button-area">
            <Button type="primary" className="next" onClick={this.forwardStep}>下一步</Button>
          </div>
        )
      case 1:
        return (
          <div className="ri-workbench-step-button-area">
            <Button type="ghost" onClick={this.backwardStep}>上一步</Button>
            <Button type="primary" className="next" onClick={this.forwardStep}>下一步</Button>
          </div>
        )
      case 2:
        return (
          <div className="ri-workbench-step-button-area">
            <Button type="ghost" onClick={this.backwardStep}>上一步</Button>
            <Button
              type="primary"
              className="next"
              loading={this.props.flowSubmitLoading}
              onClick={this.submitFlowForm}>提交</Button>
          </div>
        )
      default:
        return ''
    }
  }

  submitFlowForm = () => {
    if (this.state.streamDiffType === 'default') {
      this.handleSubmitFlowDefault()
    } else if (this.state.streamDiffType === 'hdfslog') {
      this.handleSubmitFlowHdfslog()
    }
  }

  handleSubmitFlowDefault () {
    const values = this.workbenchFlowForm.getFieldsValue()
    const { projectId, flowMode, singleFlowResult } = this.state
    const { sourceToSinkExited } = this.props
    const { resultFiledsOutput, dataframeShowOrNot, etpStrategyRequestValue, transformTableRequestValue, pushdownConnectRequestValue } = this.state

    const sinkConfigValue = this.workbenchFlowForm.getFieldValue('sinkConfig')
    const sinkConfigRequest = sinkConfigValue === undefined ? '' : sinkConfigValue

    let etpStrategyRequestValFinal = ''
    if (etpStrategyRequestValue === '') {
      etpStrategyRequestValFinal = etpStrategyRequestValue
    } else {
      etpStrategyRequestValFinal = `${etpStrategyRequestValue},`
    }

    let resultFiledsOutputFinal = ''
    if (resultFiledsOutput === '') {
      resultFiledsOutputFinal = resultFiledsOutput
    } else {
      resultFiledsOutputFinal = `${resultFiledsOutput},`
    }

    const tranConfigRequest = transformTableRequestValue === ''
      ? `{${resultFiledsOutput}}`
      : `{${etpStrategyRequestValFinal}${resultFiledsOutputFinal}${transformTableRequestValue},${pushdownConnectRequestValue}${dataframeShowOrNot}}`

    if (flowMode === 'add' || flowMode === 'copy') {
      const sourceDataInfo = [values.sourceDataSystem, values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2], '*', '*', '*'].join('.')
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

      if (sourceToSinkExited === true) {
        message.error('Source to Sink 已存在！', 3)
      } else {
        new Promise((resolve) => {
          this.props.onAddFlow(submitFlowData, () => {
            resolve()
            if (flowMode === 'add') {
              message.success('Flow 添加成功！', 3)
            } else if (flowMode === 'copy') {
              message.success('Flow 复制成功！', 3)
            }
          }, () => {
            this.hideFlowSubmit()
            this.setState({
              transformTagClassName: '',
              transformTableClassName: 'hide',
              transConnectClass: 'hide',
              fieldSelected: 'hide',
              etpStrategyCheck: false,
              dataframeShowSelected: 'hide',
              flowFormTransformTableSource: []
            })
          })
        })
          .then(() => {
            // onchange 事件影响，Promise 解决
            this.workbenchFlowForm.resetFields()
            this.setState({
              flowKafkaInstanceValue: '',
              flowKafkaTopicValue: ''
            })
          })
      }
    } else if (flowMode === 'edit') {
      const editData = {
        sinkConfig: `${sinkConfigRequest}`,
        tranConfig: tranConfigRequest,
        consumedProtocol: values.protocol
      }

      new Promise((resolve) => {
        this.props.onEditFlow(Object.assign({}, editData, singleFlowResult), () => {
          resolve()
          message.success('Flow 修改成功！', 3)
        }, () => {
          this.hideFlowSubmit()
          this.setState({
            transformTagClassName: '',
            transformTableClassName: 'hide',
            transConnectClass: 'hide',
            fieldSelected: 'hide',
            etpStrategyCheck: false,
            dataframeShowSelected: 'hide',
            flowFormTransformTableSource: []
          })
        })
      })
        .then(() => {
          this.workbenchFlowForm.resetFields()
          this.setState({
            flowKafkaInstanceValue: '',
            flowKafkaTopicValue: ''
          })
        })
    }
  }

  handleSubmitFlowHdfslog () {
    const { flowMode, projectId, singleFlowResult } = this.state
    const { sourceToSinkExited } = this.props

    const values = this.workbenchFlowForm.getFieldsValue()
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

      if (sourceToSinkExited === true) {
        message.error('Source to Sink 已存在！', 3)
      } else {
        new Promise((resolve) => {
          this.props.onAddFlow(submitFlowData, (result) => {
            resolve(result)
            if (result.length === 0) {
              message.success('该 Flow 已被创建！', 3)
            } else if (flowMode === 'add') {
              message.success('Flow 添加成功！', 3)
            } else if (flowMode === 'copy') {
              message.success('Flow 复制成功！', 3)
            }
          }, () => {
            this.hideFlowSubmit()
          })
        })
          .then(() => {
            this.workbenchFlowForm.resetFields()
          })
      }
    } else if (flowMode === 'edit') {
      const editData = {
        sinkConfig: '',
        tranConfig: '',
        consumedProtocol: 'all'
      }

      new Promise((resolve) => {
        this.props.onEditFlow(Object.assign({}, editData, singleFlowResult), () => {
          resolve()
          message.success('Flow 修改成功！', 3)
        }, () => {
          this.hideFlowSubmit()
        })
      })
        .then(() => {
          this.workbenchFlowForm.resetFields()
        })
    }
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

          if (streamNameExited === true) {
            this.workbenchStreamForm.setFields({
              streamName: {
                value: values.streamName,
                errors: [new Error('该 Name 已存在')]
              }
            })
            this.hideStreamSubmit()
          } else {
            this.props.onAddStream(projectId, Object.assign({}, requestValues, streamConfigValues), () => {
              message.success('Stream 添加成功！', 3)
              this.setState({
                streamMode: ''
              })
              this.workbenchStreamForm.resetFields()
              if (streamConfigCheck === true) {
                this.streamConfigForm.resetFields()
              }
              this.hideStreamSubmit()
            })
          }
        } else if (streamMode === 'edit') {
          const editValues = {
            desc: values.desc
          }
          const requestEditValues = Object.assign({}, editValues, streamQueryValues, streamConfigValues)

          this.props.onEditStream(requestEditValues, () => {
            message.success('Stream 修改成功！', 3)
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

  onChange = (pagination, filter, sorter) => {
    console.log('params', pagination, filter, sorter)
  }

  /**
   * Flow Transformation Modal
   * */
  onShowTransformModal = () => this.setState({ transformModalVisible: true })

  // 将 transformValue 放在上一级，来控制 transformatio type 显示不同的内容
  onInitTransformValue = (value) => this.setState({ transformValue: value })

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

      if (record.transformType === 'lookupSql') {
        // 以"." 为分界线(注：sql语句中可能会出现 ".")
        const tranLookupVal1 = record.transformConfigInfo.substring(record.transformConfigInfo.indexOf('.') + 1) // 去除第一项后的字符串
        const tranLookupVal2 = tranLookupVal1.substring(tranLookupVal1.indexOf('.') + 1)  // 去除第二项后的字符串
        const tranLookupVal3 = tranLookupVal2.substring(tranLookupVal2.indexOf('.') + 1)  // 去除第三项后的字符串
        const tranLookupVal4 = tranLookupVal3.substring(tranLookupVal3.indexOf('.') + 1)  // 去除第四项后的字符串

        this.flowTransformForm.setFieldsValue({
          lookupSqlType: record.transformConfigInfo.substring(0, record.transformConfigInfo.indexOf('.')),
          transformSinkDataSystem: tranLookupVal1.substring(0, tranLookupVal1.indexOf('.')),
          lookupSql: tranLookupVal4
        })
        setTimeout(() => {
          this.flowTransformForm.setFieldsValue({
            transformSinkNamespace: [
              tranLookupVal2.substring(0, tranLookupVal2.indexOf('.')),
              tranLookupVal3.substring(0, tranLookupVal3.indexOf('.'))
            ]
          })
        }, 50)
      } else if (record.transformType === 'sparkSql') {
        this.flowTransformForm.setFieldsValue({
          sparkSql: record.transformConfigInfo
        })
      } else if (record.transformType === 'streamJoinSql') {
        // 以"."为分界线
        const tranStreamJoinVal1 = record.transformConfigInfo.substring(record.transformConfigInfo.indexOf('.') + 1) // 去除第一项后的字符串
        const tranStreamJoinVal2 = tranStreamJoinVal1.substring(tranStreamJoinVal1.indexOf('.') + 1)  // 去除第二项后的字符串

        this.flowTransformForm.setFieldsValue({
          streamJoinSqlType: record.transformConfigInfo.substring(0, record.transformConfigInfo.indexOf('.')),
          timeout: tranStreamJoinVal1.substring(0, tranStreamJoinVal1.indexOf('.')),
          streamJoinSql: tranStreamJoinVal2
        })
      } else if (record.transformType === 'transformClassName') {
        this.flowTransformForm.setFieldsValue({
          transformClassName: record.transformConfigInfo
        })
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
      this.flowTransformForm.setFieldsValue({
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
  }

  onTransformModalOk = () => {
    const { transformMode, transformSinkNamespaceArray, step2SourceNamespace } = this.state
    this.flowTransformForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        let transformConfigInfoString = ''
        let transformConfigInfoRequestString = ''
        let pushdownConnectionString = ''

        let num = 0
        let valLength = 0
        let finalVal = ''

        if (values.transformation === 'lookupSql') {
          // values.transformSinkNamespace 为 []
          // 去掉字符串前后的空格
          const lookupSqlVal = values.lookupSql.replace(/(^\s*)|(\s*$)/g, '')

          let lookupSqlTypeOrigin = ''
          if (values.lookupSqlType === 'leftJoin') {
            lookupSqlTypeOrigin = 'left join'
          } else if (values.lookupSqlType === 'innerJoin') {
            lookupSqlTypeOrigin = 'inner join'
          } else if (values.lookupSqlType === 'union') {
            lookupSqlTypeOrigin = 'union'
          }

          const systemInstanceDatabase = [values.transformSinkDataSystem, values.transformSinkNamespace[0], values.transformSinkNamespace[1]].join('.')
          transformConfigInfoString = `${values.lookupSqlType}.${values.transformSinkDataSystem}.${values.transformSinkNamespace.join('.')}.${lookupSqlVal}`
          transformConfigInfoRequestString = `pushdown_sql ${lookupSqlTypeOrigin} with ${systemInstanceDatabase} = ${lookupSqlVal}`
          const pushdownConnectArrTemp = transformSinkNamespaceArray.find(i => [i.nsSys, i.nsInstance, i.nsDatabase].join('.') === systemInstanceDatabase)
          const pushdownConnectArr = `{"name_space":"${pushdownConnectArrTemp.nsSys}.${pushdownConnectArrTemp.nsInstance}.${pushdownConnectArrTemp.nsDatabase}", "jdbc_url": "${pushdownConnectArrTemp.conn_url}", "username": "${pushdownConnectArrTemp.user}", "password": "${pushdownConnectArrTemp.pwd}"}`
          pushdownConnectionString = pushdownConnectArr

          num = (lookupSqlVal.split(';')).length - 1
          valLength = lookupSqlVal.length
          finalVal = lookupSqlVal.substring(lookupSqlVal.length - 1)
        } else if (values.transformation === 'sparkSql') {
          const sparkSqlVal = values.sparkSql.replace(/(^\s*)|(\s*$)/g, '')

          transformConfigInfoString = sparkSqlVal
          transformConfigInfoRequestString = `spark_sql = ${sparkSqlVal}`
          pushdownConnectionString = ''

          num = (sparkSqlVal.split(';')).length - 1
          valLength = sparkSqlVal.length
          finalVal = sparkSqlVal.substring(sparkSqlVal.length - 1)
        } else if (values.transformation === 'streamJoinSql') {
          const streamJoinSqlVal = values.streamJoinSql.replace(/(^\s*)|(\s*$)/g, '')

          let streamJoinSqlTypeOrigin = ''
          if (values.streamJoinSqlType === 'leftJoin') {
            streamJoinSqlTypeOrigin = 'left join'
          } else if (values.streamJoinSqlType === 'innerJoin') {
            streamJoinSqlTypeOrigin = 'inner join'
          } else if (values.streamJoinSqlType === 'rightJoin') {
            streamJoinSqlTypeOrigin = 'right join'
          }
          // transformConfigInfoString = `${values.streamJoinSqlType}.${values.streamJoinSqlConfig}.${values.timeout}.${streamJoinSqlVal}`
          transformConfigInfoString = `${values.streamJoinSqlType}.${values.timeout}.${streamJoinSqlVal}`
          transformConfigInfoRequestString = `parquet_sql ${streamJoinSqlTypeOrigin} with ${step2SourceNamespace}.*.*.*(${values.timeout}) = ${streamJoinSqlVal}`
          pushdownConnectionString = ''

          num = (streamJoinSqlVal.split(';')).length - 1
          valLength = streamJoinSqlVal.length
          finalVal = streamJoinSqlVal.substring(streamJoinSqlVal.length - 1)
        } else if (values.transformation === 'transformClassName') {
          const transformClassNameVal = values.transformClassName.replace(/(^\s*)|(\s*$)/g, '')

          transformConfigInfoString = transformClassNameVal
          transformConfigInfoRequestString = `custom_class = ${transformClassNameVal}`
          pushdownConnectionString = ''

          num = (transformClassNameVal.split(';')).length - 1
          valLength = transformClassNameVal.length
          finalVal = transformClassNameVal.substring(transformClassNameVal.length - 1)
        }

        if (num === 0) {
          message.warning('SQL语句应以一个分号结束！', 3)
        } else if (num > 1) {
          message.warning('SQL语句应只有一个分号！', 3)
        } else if (num === 1 && finalVal !== ';') {
          message.warning('SQL语句应以一个分号结束！', 3)
        } else if (num === 1 && finalVal === ';') {
          if (valLength === 1) {
            message.warning('请填写 SQL语句内容！', 3)
          } else {
            // 加隐藏字段 transformType, 获得每次选中的transformation type
            if (transformMode === '') {
              // 第一次添加数据时
              this.state.flowFormTransformTableSource.push({
                transformType: values.transformation,
                order: 1,
                transformConfigInfo: transformConfigInfoString,
                transformConfigInfoRequest: transformConfigInfoRequestString,
                pushdownConnection: pushdownConnectionString
              })

              this.setState({
                dataframeShowSelected: 'hide'
              }, () => {
                this.workbenchFlowForm.setFieldsValue({
                  dataframeShow: 'false',
                  dataframeShowNum: 10
                })
              })
            } else if (transformMode === 'edit') {
              this.state.flowFormTransformTableSource[values.editTransformId - 1] = {
                transformType: values.transformation,
                order: values.editTransformId,
                transformConfigInfo: transformConfigInfoString,
                transformConfigInfoRequest: transformConfigInfoRequestString,
                pushdownConnection: pushdownConnectionString
              }
            } else if (transformMode === 'add') {
              const tableSourceArr = this.state.flowFormTransformTableSource
              // 当前插入的数据
              tableSourceArr.splice(values.editTransformId, 0, {
                transformType: values.transformation,
                order: values.editTransformId + 1,
                transformConfigInfo: transformConfigInfoString,
                transformConfigInfoRequest: transformConfigInfoRequestString,
                pushdownConnection: pushdownConnectionString
              })
              // 当前数据的下一条开始，order+1
              for (let i = values.editTransformId + 1; i < tableSourceArr.length; i++) {
                tableSourceArr[i].order = tableSourceArr[i].order + 1
              }
              // 重新setState数组
              this.setState({ flowFormTransformTableSource: tableSourceArr })
            }
            this.tranModalOkSuccess()
          }
        }
      }
    })
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
    const tableSourceArr = this.state.flowFormTransformTableSource
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

    // 删除当条数据
    tableSourceArr.splice(record.order - 1, 1)

    // 当条下的数据 order-1
    for (let i = record.order - 1; i < tableSourceArr.length; i++) {
      tableSourceArr[i].order = tableSourceArr[i].order - 1
    }
    this.setState({ flowFormTransformTableSource: tableSourceArr })
  }

  onUpTransform = (record) => (e) => {
    const tableSourceArr = this.state.flowFormTransformTableSource

    // 当前数据
    let currentInfo = [{
      transformType: record.transformType,
      order: record.order,
      transformConfigInfo: record.transformConfigInfo,
      transformConfigInfoRequest: record.transformConfigInfoRequest,
      pushdownConnection: record.pushdownConnection
    }]

    // 上一条数据
    let beforeArr = tableSourceArr.slice(record.order - 2, record.order - 1)

    currentInfo[0] = {
      transformType: beforeArr[0].transformType,
      order: record.order,
      transformConfigInfo: beforeArr[0].transformConfigInfo,
      transformConfigInfoRequest: beforeArr[0].transformConfigInfoRequest,
      pushdownConnection: beforeArr[0].pushdownConnection
    }

    beforeArr[0] = {
      transformType: record.transformType,
      order: record.order - 1,
      transformConfigInfo: record.transformConfigInfo,
      transformConfigInfoRequest: record.transformConfigInfoRequest,
      pushdownConnection: record.pushdownConnection
    }

    tableSourceArr.splice(record.order - 2, 2, beforeArr[0], currentInfo[0])

    this.setState({ flowFormTransformTableSource: tableSourceArr })
  }

  onDownTransform = (record) => (e) => {
    const tableSourceArr = this.state.flowFormTransformTableSource

    // 当前数据
    let currentInfo = [{
      transformType: record.transformType,
      order: record.order,
      transformConfigInfo: record.transformConfigInfo,
      transformConfigInfoRequest: record.transformConfigInfoRequest,
      pushdownConnection: record.pushdownConnection
    }]

    // 下一条数据
    let afterArr = tableSourceArr.slice(record.order, record.order + 1)

    currentInfo[0] = {
      transformType: afterArr[0].transformType,
      order: record.order,
      transformConfigInfo: afterArr[0].transformConfigInfo,
      transformConfigInfoRequest: afterArr[0].transformConfigInfoRequest,
      pushdownConnection: afterArr[0].pushdownConnection
    }

    afterArr[0] = {
      transformType: record.transformType,
      order: record.order + 1,
      transformConfigInfo: record.transformConfigInfo,
      transformConfigInfoRequest: record.transformConfigInfoRequest,
      pushdownConnection: record.pushdownConnection
    }

    tableSourceArr.splice(record.order - 1, 2, currentInfo[0], afterArr[0])

    this.setState({ flowFormTransformTableSource: tableSourceArr })
  }

  /**
   * Flow ETP Strategy Modal
   * */
  onShowEtpStrategyModal = () => {
    const { etpStrategyCheck, etpStrategyResponseValue } = this.state

    this.setState({
      etpStrategyModalVisible: true
    }, () => {
      if (etpStrategyCheck === true) {
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
        this.setState({
          etpStrategyCheck: true,
          etpStrategyRequestValue: `"validity":{"check_columns":"${values.checkColumns}","check_rule":"${values.checkRule}","rule_mode":"${values.ruleMode}","rule_params":"${values.ruleParams}","against_action":"${values.againstAction}"}`,
          etpStrategyConfirmValue: `"check_columns":"${values.checkColumns}","check_rule":"${values.checkRule}","rule_mode":"${values.ruleMode}","rule_params":"${values.ruleParams}","against_action":"${values.againstAction}"`
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
      this.cm.doc.setValue(this.workbenchFlowForm.getFieldValue('sinkConfig') || '')
    })
  }

  hideSinkConfigModal = () => this.setState({ sinkConfigModalVisible: false })

  onSinkConfigModalOk = () => {
    this.workbenchFlowForm.setFieldsValue({
      sinkConfig: this.cm.doc.getValue()
    })
    this.hideSinkConfigModal()
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
    const {flowMode, streamMode, formStep, isWormhole, flowFormTransformTableSource} = this.state
    const {streams, projectNamespaces, streamSubmitLoading} = this.props

    const sidebarPrefixes = {
      add: '新增',
      edit: '修改',
      copy: '复制'
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

                    transformTableSource={flowFormTransformTableSource}
                    // onStreamJoinSqlConfigTypeSelect={this.onStreamJoinSqlConfigTypeSelect}

                    onShowTransformModal={this.onShowTransformModal}
                    onShowEtpStrategyModal={this.onShowEtpStrategyModal}
                    onShowSinkConfigModal={this.onShowSinkConfigModal}

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
                    onInitSinkTypeNamespace={this.onInitSinkTypeNamespace}
                    sourceTypeNamespaceData={this.state.sourceTypeNamespaceData}
                    hdfslogNsData={this.state.hdfslogNsData}
                    sinkTypeNamespaceData={this.state.sinkTypeNamespaceData}

                    resultFieldsValue={this.state.resultFieldsValue}
                    dataframeShowNumValue={this.state.dataframeShowNumValue}
                    etpStrategyConfirmValue={this.state.etpStrategyConfirmValue}
                    transformTableConfirmValue={this.state.transformTableConfirmValue}

                    transformTableRequestValue={this.state.transformTableRequestValue}
                    streamDiffType={this.state.streamDiffType}
                    hdfslogSinkDataSysValue={this.state.hdfslogSinkDataSysValue}
                    hdfslogSinkNsValue={this.state.hdfslogSinkNsValue}
                    initialHdfslogCascader={this.initialHdfslogCascader}

                    flowKafkaInstanceValue={this.state.flowKafkaInstanceValue}
                    flowKafkaTopicValue={this.state.flowKafkaTopicValue}

                    ref={(f) => { this.workbenchFlowForm = f }}
                  />
                  {/* Transform Modal */}
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
                      sinkNamespaces={projectNamespaces || []}
                      onInitTransformValue={this.onInitTransformValue}
                      transformValue={this.state.transformValue}
                      step2SinkNamespace={this.state.step2SinkNamespace}
                      step2SourceNamespace={this.state.step2SourceNamespace}
                      onInitTransformSinkTypeNamespace={this.onInitTransformSinkTypeNamespace}
                      transformSinkTypeNamespaceData={this.state.transformSinkTypeNamespaceData}
                    />
                  </Modal>
                  {/* Sink Config Modal */}
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
                        placeholder="Paste your Sink Config JSON here"
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
              <div className={`ri-workbench-graph ri-common-block ${flowMode ? 'op-mode' : ''}`}>
                <h3 className="ri-common-block-title">Flow DAG</h3>
                <div className="ri-common-block-tools">
                  {/* <Button icon="arrows-alt" type="ghost" onClick={this.showFlowDagModal}></Button> */}
                </div>
              </div>
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
                    topicsValues={this.state.topicsValues}

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
                      ref={(f) => {
                        this.streamConfigForm = f
                      }}
                    />
                  </Modal>
                  <div className="ri-workbench-step-button-area">
                    <Button
                      type="primary"
                      className="next"
                      onClick={this.submitStreamForm}
                      loading={streamSubmitLoading}
                    >
                      保存
                    </Button>
                  </div>
                </div>
              </div>
              <div className={`ri-workbench-graph ri-common-block ${streamMode ? 'op-mode' : ''}`}>
                <h3 className="ri-common-block-title">Stream DAG</h3>
                <div className="ri-common-block-tools">
                  {/* <Button icon="arrows-alt" type="ghost" onClick={this.showStreamDagModal}></Button> */}
                </div>
              </div>
              {/* <div className={this.state.streamDagModalShow}>
                <div className="dag-madal-mask"></div>
                <div className="dag-modal">
                  <Button icon="shrink" type="ghost" className="hide-dag-modal" onClick={this.hideStreamDagModal}></Button>
                  <StreamDagModal />
                </div>
              </div> */}
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

  onLoadAdminSingleFlow: React.PropTypes.func,
  onLoadUserAllFlows: React.PropTypes.func,
  onLoadAdminSingleStream: React.PropTypes.func,
  onLoadUserStreams: React.PropTypes.func,
  onLoadUserNamespaces: React.PropTypes.func,
  onLoadSelectNamespaces: React.PropTypes.func,
  onLoadUserUsers: React.PropTypes.func,
  onLoadSelectUsers: React.PropTypes.func,
  onLoadResources: React.PropTypes.func,
  onLoadSingleUdf: React.PropTypes.func
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
    onLoadSourceToSinkExist: (projectId, sourceNs, sinkNs, resolve, reject) => dispatch(loadSourceToSinkExist(projectId, sourceNs, sinkNs, resolve, reject))
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
  projectNamespaces: selectProjectNamespaces()
})

export default connect(mapStateToProps, mapDispatchToProps)(Workbench)
