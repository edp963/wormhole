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
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import Form from 'antd/lib/form'
const FormItem = Form.Item
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Input from 'antd/lib/input'
import InputNumber from 'antd/lib/input-number'
import Select from 'antd/lib/select'
const Option = Select.Option
import Cascader from 'antd/lib/cascader'
import Popconfirm from 'antd/lib/popconfirm'
import Tooltip from 'antd/lib/tooltip'
import Button from 'antd/lib/button'
import Tag from 'antd/lib/tag'
import Icon from 'antd/lib/icon'
import Table from 'antd/lib/table'
import Card from 'antd/lib/card'
import Radio from 'antd/lib/radio'
import { Checkbox, Switch } from 'antd'
const CheckboxGroup = Checkbox.Group
const RadioGroup = Radio.Group
const RadioButton = Radio.Button

import { loadSourceSinkTypeNamespace, loadSinkTypeNamespace } from '../Flow/action'

import DataSystemSelector from '../../components/DataSystemSelector'

import {
  prettyShownText, uuid, forceCheckNum, operateLanguageSelect, operateLanguageFillIn, transformStringWithDot
} from '../../utils/util'
import { sourceDataSystemData, sinkDataSystemData } from '../../components/DataSystemSelector/dataSystemFunction'
import { generateSourceSinkNamespaceHierarchy, generateHdfslogNamespaceHierarchy } from './workbenchFunction'
import Modal from 'antd/lib/modal'
import WorkbenchDebugForm from './WorkbenchDebugForm'
import message from 'antd/lib/message'
import { loadUdfs, loadSingleUdf, loadLastestOffset, startDebug, stopDebug } from './action'
import { createStructuredSelector } from 'reselect'
import { selectDebugModalLoading } from './selectors'

export class WorkbenchFlowForm extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      flowMode: '',
      sinkConfigClass: '',
      defaultSourceNsData: [],
      defaultSinkNsData: [],
      hdfslogSourceNsData: [],
      hdfslogSinkDSValue: '',
      routingNsData: [],
      sinkNamespaceResult: [],
      debugModalVisible: false,
      debugFormData: [],
      autoRegisteredTopics: [],
      userDefinedTopics: [],
      startUdfVals: [],
      currentUdfVal: [],
      unValidate: false,
      tempUserTopics: [],
      debugRunning: false
    }
  }

  componentWillReceiveProps (props) {
    this.setState({
      flowMode: props.flowMode
    })
    if (props.transformTableSource) {
      props.transformTableSource.map(s => {
        s.key = uuid()
        return s
      })
    }
    if (!props.flowMode && this.state.debugRunning) {
      this.handleStopDebug()
    }
  }

  // 通过不同的 Source Data System 显示不同的 Source Namespace 内容
  onSourceDataSystemItemSelect = (val) => {
    const { streamDiffType, flowMode, projectIdGeted, streamId } = this.props
    this.props.emitDataSystem(val)
    if (val) {
      switch (streamDiffType) {
        case 'default':
          if (streamId !== 0) {
            this.props.onLoadSourceSinkTypeNamespace(projectIdGeted, streamId, val, 'sourceType', (result) => {
              this.setState({
                defaultSourceNsData: generateSourceSinkNamespaceHierarchy(val, result)
              })
              // default source ns 和 sink ns 同时调同一个接口获得，保证两处的 placeholder 和单条数据回显都能正常
              if (flowMode === 'add' || flowMode === 'copy') {
                this.props.form.setFieldsValue({ sourceNamespace: undefined })
              }
            })
          }
          break
        case 'hdfslog':
        case 'hdfscsv':
          if (streamId !== 0) {
            this.props.onLoadSourceSinkTypeNamespace(projectIdGeted, streamId, val, 'sourceType', (result) => {
              this.setState({
                hdfslogSourceNsData: generateHdfslogNamespaceHierarchy(val, result),
                hdfslogSinkDSValue: val
              })
              // placeholder 和单条数据回显
              if (flowMode === 'add' || flowMode === 'copy') {
                this.props.form.setFieldsValue({ hdfsNamespace: undefined })
              }
            })
          }
          break
        case 'routing':
          if (streamId !== 0) {
            this.props.onLoadSourceSinkTypeNamespace(projectIdGeted, streamId, val, 'sourceType', (result) => {
              this.setState({
                routingNsData: generateSourceSinkNamespaceHierarchy(val, result)
              })
              if (flowMode === 'add' || flowMode === 'copy') {
                this.props.form.setFieldsValue({
                  routingNamespace: undefined,
                  routingSinkNs: undefined
                })
              }
            })
          }
          break
      }
    }
  }
  namespaceChange = (value, selectedOptions) => {
    let id = selectedOptions[2].id
    let sinkNamespaceResult = this.state.sinkNamespaceResult
    sinkNamespaceResult.forEach(v => {
      if (v.id === id) {
        this.props.form.setFieldsValue({tableKeys: v.keys})
      }
    })
  }
  // 通过不同的 Sink Data System 显示不同的 Sink Namespace 的内容
  onSinkDataSystemItemSelect = (val) => {
    if (val) {
      const { flowMode, projectIdGeted, streamId } = this.props
      this.props.onInitSinkTypeNamespace(val)

      if (streamId !== 0) {
        this.props.onLoadSinkTypeNamespace(projectIdGeted, streamId, val, 'sinkType', (result) => {
          this.setState({
            sinkNamespaceResult: result,
            defaultSinkNsData: generateSourceSinkNamespaceHierarchy(val, result)
          })
          if (flowMode === 'add' || flowMode === 'copy') {
            this.props.form.setFieldsValue({ sinkNamespace: undefined })
          }
        })
      }
    }
    this.setState({
      sinkConfigClass: val === 'hbase' ? 'sink-config-class' : ''
    })
    if (this.state.flowMode !== 'edit') {
      this.props.form.setFieldsValue({ sinkConfig: '' })
    }
  }

  changeStreamType = (e) => {
    this.props.emitFlowFunctionType(e.target.value)
    this.props.onInitStreamTypeSelect(e.target.value)
    this.setState({
      hdfslogSinkDSValue: ''
    })
  }

  handleStartDebug = (e) => {
    const { form, projectIdGeted, locale, flowMode, flowSourceNsSys } = this.props
    const values = form.getFieldsValue()
    this.setState({
      debugModalVisible: true
    })

    // 单条查询接口获得回显的topic Info，回显选中的UDFs
    if (values.streamType === 'spark') {
      this.props.onLoadUdfs(projectIdGeted, 'streams', values.flowStreamId, 'user', (result) => {
        // 回显选中的 topic，必须有 id
        const currentUdfTemp = result
        let topicsSelectValue = []
        for (let i = 0; i < currentUdfTemp.length; i++) {
          topicsSelectValue.push(`${currentUdfTemp[i].id}`)
        }
        this.workbenchDebugForm.setFieldsValue({ udfs: topicsSelectValue })
      })
    } else {
      if (flowMode === 'edit') {
        this.props.onLoadUdfs(projectIdGeted, 'flows', values.flowId, 'user', (result) => {
          // 回显选中的 topic，必须有 id
          const currentUdfTemp = result
          let topicsSelectValue = []
          for (let i = 0; i < currentUdfTemp.length; i++) {
            topicsSelectValue.push(`${currentUdfTemp[i].id}`)
          }
          this.workbenchDebugForm.setFieldsValue({ udfs: topicsSelectValue })
        })
      }
    }

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
    }, values.streamType)

    // 显示 Latest offset
    if (values.streamType === 'spark') {
      this.props.onLoadLastestOffset(projectIdGeted, 'streams', values.flowStreamId, null, (result) => {
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
            debugFormData: autoRegisteredTopics
          })
        } else {
          this.setState({
            debugFormData: []
          })
        }
      })
    } else {
      if (flowMode === 'edit') {
        this.props.onLoadLastestOffset(projectIdGeted, 'flows', values.flowId, null, (result) => {
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
              debugFormData: autoRegisteredTopics
            })
          } else {
            this.setState({
              debugFormData: []
            })
          }
        })
      } else {
        const sourceDataInfo = [flowSourceNsSys, values.sourceNamespace[0], values.sourceNamespace[1], values.sourceNamespace[2], '*', '*', '*'].join('.')
        this.props.onLoadLastestOffset(projectIdGeted, null, null, sourceDataInfo, (result) => {
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
              debugFormData: autoRegisteredTopics
            })
          } else {
            this.setState({
              debugFormData: []
            })
          }
        })
      }
    }
  }

  handleEditDebugOk = (e) => {
    const { projectIdGeted, locale, form } = this.props
    const { debugFormData, userDefinedTopics, startUdfVals } = this.state
    const streamIdGeted = form.getFieldValue('flowStreamId')

    const offsetText = locale === 'en' ? 'Offset cannot be empty' : 'Offset 不能为空！'
    this.setState(
      {unValidate: true},
      () => {
        this.workbenchDebugForm.validateFieldsAndScroll((err, values) => {
          if (!err || err.newTopicName) {
            let requestVal = {}
            if (!debugFormData) {
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
              const autoRegisteredData = this.formatTopicInfo(debugFormData, 'auto', values, offsetText)
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

            let actionTypeRequest = 'debug'
            let actionTypeMsg = locale === 'en' ? 'Debug Successfully!' : '启动成功！'

            if (form.getFieldValue('streamType') === 'spark') {
              this.props.onStartDebug(projectIdGeted, streamIdGeted, requestVal, actionTypeRequest, (result) => {
                this.setState({
                  debugModalVisible: false,
                  debugFormData: [],
                  userDefinedData: [],
                  tempUserTopics: [],
                  unValidate: false,
                  debugRunning: true
                })
                message.success(actionTypeMsg, 3)
                form.setFieldsValue({debugLogPath: result['logPath']})
              }, (result) => {
                const failText = locale === 'en' ? 'Operation failed:' : '操作失败：'
                message.error(`${failText} ${result}`, 3)
                this.setState({unValidate: false})
              })
            } else {
              form.setFieldsValue({flinkFlowDirective: requestVal})
              this.setState({
                debugModalVisible: false,
                debugFormData: [],
                userDefinedData: [],
                tempUserTopics: [],
                unValidate: false,
                debugRunning: true
              })
              message.success(actionTypeMsg, 3)
            }
          }
        })
      }
    )
  }

  handleDebugCancel = (e) => {
    this.setState({
      debugModalVisible: false
    }, () => {
      this.setState({
        debugFormData: []
      })
    })
    this.workbenchDebugForm.resetFields()
  }

  handleStopDebug = (e) => {
    const { projectIdGeted, form, locale } = this.props
    const streamIdGeted = form.getFieldValue('flowStreamId')
    const debugLogPath = form.getFieldValue('debugLogPath')
    const actionTypeMsg = locale === 'en' ? 'Stop Debug Successfully!' : '停止成功！'
    if (debugLogPath) {
      this.props.onStopDebug(projectIdGeted, streamIdGeted, debugLogPath, () => {
        this.setState({
          debugRunning: false
        })
        form.setFieldsValue({debugLogPath: ''})
        message.success(actionTypeMsg, 3)
      })
    } else {
      this.setState({
        debugRunning: false
      })
      form.setFieldsValue({flinkStreamDirective: ''})
      message.success(actionTypeMsg, 3)
    }
  }

  queryLastestoffset = (e) => {
    const { projectIdGeted, form, flowMode } = this.props
    const { userDefinedTopics, autoRegisteredTopics } = this.state
    let topics = {}
    topics.userDefinedTopics = userDefinedTopics.map((v, i) => v.name)
    topics.autoRegisteredTopics = autoRegisteredTopics.map((v, i) => v.name)
    if (form.getFieldValue('streamType') === 'spark') {
      this.loadLastestOffsetFunc(projectIdGeted, 'streams', form.getFieldValue('flowStreamId'), 'post', topics)
    } else {
      if (flowMode === 'edit') {
        this.loadLastestOffsetFunc(projectIdGeted, 'flows', form.getFieldValue('flowId'), 'post', topics)
      }
    }
  }

  // Load Latest Offset
  loadLastestOffsetFunc (projectId, type, id, method, topics) {
    this.props.onLoadLastestOffset(projectId, type, id, null, (result) => {
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
        debugFormData: autoRegisteredTopics
      })
    }, method, topics)
  }

  onChangeEditSelect = () => {
    const { debugFormData, userDefinedTopics } = this.state

    for (let i = 0; i < debugFormData.length; i++) {
      const partitionAndOffset = debugFormData[i].consumedLatestOffset.split(',')

      for (let j = 0; j < partitionAndOffset.length; j++) {
        this.workbenchDebugForm.setFieldsValue({
          [`${debugFormData[i].name}_${j}_auto`]: partitionAndOffset[j].substring(partitionAndOffset[j].indexOf(':') + 1),
          [`${debugFormData[i].name}_${debugFormData[i].rate}_rate`]: debugFormData[i].rate
        })
      }
    }
    for (let i = 0; i < userDefinedTopics.length; i++) {
      const partitionAndOffset = userDefinedTopics[i].consumedLatestOffset.split(',')
      for (let j = 0; j < partitionAndOffset.length; j++) {
        this.workbenchDebugForm.setFieldsValue({
          [`${userDefinedTopics[i].name}_${j}_user`]: partitionAndOffset[j].substring(partitionAndOffset[j].indexOf(':') + 1),
          [`${userDefinedTopics[i].name}_${userDefinedTopics[i].rate}_rate`]: userDefinedTopics[i].rate
        })
      }
    }
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

  getStartFormDataFromSub = (userDefinedTopics) => {
    this.setState({ userDefinedTopics })
  }

  render () {
    const {
      step, form, fieldSelected, dataframeShowSelected, streamDiffType, hdfsSinkNsValue, routingSourceNsValue,
      routingSinkNsValue, transformTableConfirmValue, flowKafkaTopicValue,
      onShowTransformModal, onShowEtpStrategyModal, onShowSinkConfigModal, onShowSpecialConfigModal,
      transformTableSource, onDeleteSingleTransform, onAddTransform, onEditTransform, onUpTransform, onDownTransform,
      step2SourceNamespace, step2SinkNamespace, etpStrategyCheck, transformTagClassName, transformTableClassName, transConnectClass,
      selectStreamKafkaTopicValue, routingSinkTypeNsData, sinkConfigCopy,
      initResultFieldClass, initDataShowClass, onInitStreamNameSelect, initialDefaultCascader,
      initialHdfslogCascader, initialRoutingCascader, initialRoutingSinkCascader, flowSourceNsSys, onDebugFlow, projectIdGeted
    } = this.props

    const { getFieldDecorator } = form

    const { flowMode, sinkConfigClass, defaultSourceNsData, defaultSinkNsData, hdfslogSourceNsData,
      hdfslogSinkDSValue, routingNsData, debugModalVisible, debugFormData, autoRegisteredTopics, userDefinedTopics, startUdfVals, currentUdfVal, debugRunning } = this.state

    // edit 时，不能修改部分元素
    const flowDisabledOrNot = flowMode === 'edit'

    const stepClassNames = [
      step === 0 ? '' : 'hide',
      step === 1 ? '' : 'hide',
      step === 2 ? '' : 'hide'
    ]

    const stepHiddens = [
      false,
      step <= 0,
      step <= 1
    ]

    const streamTypeClass = [
      streamDiffType === 'default' ? '' : 'hide',
      streamDiffType === 'hdfslog' ? '' : 'hide',
      streamDiffType === 'routing' ? '' : 'hide',
      streamDiffType === 'hdfscsv' ? '' : 'hide',
      streamDiffType === 'hdfslog' || streamDiffType === 'hdfscsv' ? '' : 'hide'
    ]

    const streamTypeHiddens = [
      streamDiffType !== 'default',
      streamDiffType !== 'hdfslog',
      streamDiffType !== 'routing',
      streamDiffType !== 'hdfscsv',
      streamDiffType !== 'hdfslog' && streamDiffType !== 'hdfscsv'
    ]

    const itemStyle = {
      labelCol: { span: 6 },
      wrapperCol: { span: 17 }
    }

    const itemStyleDFS = {
      labelCol: { span: 9 },
      wrapperCol: { span: 9 }
    }

    const itemStyleDFSN = {
      labelCol: { span: 14 },
      wrapperCol: { span: 10 }
    }

    let formValues = this.props.form.getFieldsValue([
      'streamName',
      'streamType'
    ])

    const step3ConfirmContent = Object.keys(formValues).map(key => (
      <Col span={24} key={key}>
        <div className="ant-row ant-form-item">
          <Row>
            <Col span={8} className="ant-form-item-label">
              <label htmlFor="#">{prettyShownText(key)}</label>
            </Col>
            <Col span={15} className="value-font-style">
              <div className="ant-form-item-control">
                <strong>
                  {Object.prototype.toString.call(formValues[key]) === '[object Array]'
                    ? formValues[key].join('.')
                    : formValues[key]}
                </strong>
              </div>
            </Col>
          </Row>
        </div>
      </Col>
    ))

    let formDSNSValues = ''
    if (streamDiffType === 'default') {
      formDSNSValues = this.props.form.getFieldsValue([
        'sourceDataSystem',
        'sourceNamespace',
        'sinkDataSystem',
        'sinkNamespace',
        'sinkConfig'
      ])
      // formDSNSValues.sourceDataSystem = flowSourceNsSys
      // formDSNSValues.sinkDataSystem = flowSourceNsSys
    } else if (streamDiffType === 'routing') {
      formDSNSValues = this.props.form.getFieldsValue([
        'sourceDataSystem'
      ])
      formDSNSValues.sourceDataSystem = flowSourceNsSys
    }

    const step3ConfirmDSNS = Object.keys(formDSNSValues).map(key => (
      <Col span={24} key={key}>
        <div className="ant-row ant-form-item">
          <Row>
            <Col span={8} className="ant-form-item-label">
              <label htmlFor="#">{prettyShownText(key)}</label>
            </Col>
            <Col span={15} className="value-font-style">
              <div className="ant-form-item-control" style={{font: 'bolder'}}>
                <strong>
                  {Object.prototype.toString.call(formDSNSValues[key]) === '[object Array]'
                    ? formDSNSValues[key].join('.')
                    : formDSNSValues[key]}
                </strong>
              </div>
            </Col>
          </Row>
        </div>
      </Col>
    ))

    const sinkConfigColor = (
      <Tag color="#7CB342" onClick={onShowSinkConfigModal}>
        <Icon type="check-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
      </Tag>
    )
    const sinkConfigNoColor = (
      <Tag onClick={onShowSinkConfigModal}>
        <Icon type="minus-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
      </Tag>
    )

    const sinkConfigTag = flowMode === 'copy'
      ? sinkConfigCopy ? sinkConfigColor : sinkConfigNoColor
      : form.getFieldValue('sinkConfig') ? sinkConfigColor : sinkConfigNoColor

    const flowSpecialConfigTag = form.getFieldValue('flowSpecialConfig')
      ? (
        <Tag color="#7CB342" onClick={onShowSpecialConfigModal}>
          <Icon type="check-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
        </Tag>
      )
      : (
        <Tag onClick={onShowSpecialConfigModal}>
          <Icon type="minus-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
        </Tag>
      )

    const etpStrategyTag = etpStrategyCheck
      ? (
        <Tag color="#7CB342" onClick={onShowEtpStrategyModal}>
          <Icon type="check-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
        </Tag>
      )
      : (
        <Tag onClick={onShowEtpStrategyModal}>
          <Icon type="minus-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
        </Tag>
      )

    const columns = [{
      title: 'Num',
      dataIndex: 'order',
      key: 'order',
      width: '12%'
    }, {
      title: 'Config Info',
      dataIndex: 'tranConfigInfoSql',
      key: 'tranConfigInfoSql',
      width: '65%'
    }, {
      title: 'Transform Config Info Request',
      dataIndex: 'transformConfigInfoRequest',
      key: 'transformConfigInfoRequest',
      className: 'hide'
    }, {
      title: 'Pushdown Connection',
      dataIndex: 'pushdownConnection',
      key: 'pushdownConnection',
      className: 'hide'
    }, {
      title: 'Action',
      key: 'action',
      render: (text, record) => {
        const transformUpHide = record.order === 1 ? 'hide' : ''
        const transformDownHide = record.order === transformTableSource.length ? 'hide' : ''
        const flowDebugHide = debugRunning ? '' : 'hide'
        const addFormat = <FormattedMessage {...messages.workbenchTransAdd} />
        const modifyFormat = <FormattedMessage {...messages.workbenchTransModify} />
        const deleteFormat = <FormattedMessage {...messages.workbenchTransDelete} />
        const sureDeleteFormat = <FormattedMessage {...messages.workbenchTransSureDelete} />
        const upFormat = <FormattedMessage {...messages.workbenchTransUp} />
        const downFormat = <FormattedMessage {...messages.workbenchTransDown} />
        const viewFormat = <FormattedMessage {...messages.workbenchFlowView} />

        return (
          <span className="ant-table-action-column">
            <Tooltip title={modifyFormat}>
              <Button icon="edit" shape="circle" type="ghost" onClick={onEditTransform(record)}></Button>
            </Tooltip>

            <Tooltip title={addFormat}>
              <Button shape="circle" type="ghost" onClick={onAddTransform(record)}>
                <i className="iconfont icon-jia"></i>
              </Button>
            </Tooltip>

            <Popconfirm placement="bottom" title={sureDeleteFormat} okText="Yes" cancelText="No" onConfirm={onDeleteSingleTransform(record)}>
              <Tooltip title={deleteFormat}>
                <Button shape="circle" type="ghost">
                  <i className="iconfont icon-jian"></i>
                </Button>
              </Tooltip>
            </Popconfirm>

            <Tooltip title={upFormat}>
              <Button shape="circle" type="ghost" onClick={onUpTransform(record)} className={transformUpHide}>
                <i className="iconfont icon-up"></i>
              </Button>
            </Tooltip>

            <Tooltip title={downFormat}>
              <Button shape="circle" type="ghost" onClick={onDownTransform(record)} className={transformDownHide}>
                <i className="iconfont icon-down"></i>
              </Button>
            </Tooltip>

            <Tooltip title={viewFormat}>
              {/* <Button icon="eye" shape="circle" type="ghost" onClick={onDebugFlow(record)} ></Button> */}
              <Button icon="eye" shape="circle" type="ghost" onClick={onDebugFlow(record)} className={flowDebugHide}></Button>
            </Tooltip>
          </span>
        )
      }
    }]

    const pagination = {
      defaultPageSize: 5,
      pageSizeOptions: ['5', '10', '15'],
      showSizeChanger: true,
      onShowSizeChange: (current, pageSize) => {
        this.setState({
          pageIndex: current,
          pageSize: pageSize
        })
      },
      onChange: (current) => {
        this.setState({ pageIndex: current })
      }
    }

    const flowProtocolCheckboxList = [
      { label: 'Increment', value: 'increment' },
      { label: 'Initial', value: 'initial' },
      { label: 'Backfill', value: 'backfill' }
    ]
    const streamNameOptions = selectStreamKafkaTopicValue.length === 0
      ? undefined
      : selectStreamKafkaTopicValue.map(s => (<Option key={s.id} value={`${s.name}`}>{s.name}</Option>))

    const { etpStrategyConfirmValue, transConfigConfirmValue, resultFieldsValue, flowKafkaInstanceValue, flowSubPanelKey, timeCharacteristic, debugModalLoading } = this.props

    // let maxParallelism = 0
    // for (let v of selectStreamKafkaTopicValue) {
    //   if (v.id === streamId) maxParallelism = v.maxParallelism
    // }

    const workbenchDebugForm = debugModalVisible
      ? (
        <WorkbenchDebugForm
          data={debugFormData}
          autoRegisteredTopics={autoRegisteredTopics}
          userDefinedTopics={userDefinedTopics}
          emitStartFormDataFromSub={this.getStartFormDataFromSub}
          startUdfValsOption={startUdfVals}
          currentUdfVal={currentUdfVal}
          projectIdGeted={projectIdGeted}
          streamIdGeted={form.getFieldValue('flowStreamId')}
          unValidate={this.state.unValidate}
          ref={(f) => { this.workbenchDebugForm = f }}
        />
      )
      : ''

    return (
      <Form className="ri-workbench-form workbench-flow-form">
        {/* Step 1 */}
        <Row gutter={8} className={stepClassNames[0]}>
          <Card title="Stream" className="ri-workbench-form-card-style stream-card">
            <Col span={24}>
              <FormItem label="Flow name" {...itemStyle}>
                {getFieldDecorator('flowName', {
                  rules: [{
                    required: true,
                    message: operateLanguageFillIn('flow name', 'Flow name')
                  }]
                })(
                  <Input />
                )}
              </FormItem>
            </Col>
            <Col span={24}>
              <FormItem label="Stream type" {...itemStyle}>
                {getFieldDecorator('streamType', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('type', 'Type')
                  }],
                  initialValue: flowSubPanelKey
                })(
                  <RadioGroup className="radio-group-style" disabled={flowDisabledOrNot} size="default" onChange={this.props.changeStreamType('flow')}>
                    <RadioButton value="spark" className="radio-btn-style radio-btn-extra">Spark</RadioButton>
                    <RadioButton value="flink" className="radio-btn-style radio-btn-extra">Flink</RadioButton>
                  </RadioGroup>
                )}
              </FormItem>
            </Col>
            <Col span={24}>
              <FormItem label="Function Type" {...itemStyle}>
                {getFieldDecorator('functionType', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('function type', 'Function Type')
                  }],
                  initialValue: streamDiffType || 'default'
                })(
                  <RadioGroup className="radio-group-style" onChange={this.changeStreamType} size="default">
                    <RadioButton value="default" className="radio-btn-style radio-btn-extra" disabled={flowDisabledOrNot}>Default</RadioButton>
                    {flowSubPanelKey === 'flink' ? '' : (
                      <RadioButton value="hdfslog" className={`radio-btn-style radio-btn-extra`} disabled={flowDisabledOrNot}>Hdfslog</RadioButton>
                    )}
                    {flowSubPanelKey === 'flink' ? '' : (
                      <RadioButton value="routing" className={`radio-btn-style radio-btn-extra`} disabled={flowDisabledOrNot}>Routing</RadioButton>
                    )}
                    {flowSubPanelKey === 'flink' ? '' : (
                      <RadioButton value="hdfscsv" className={`radio-btn-style radio-btn-extra`} disabled={flowDisabledOrNot}>Hdfscsv</RadioButton>
                    )}
                  </RadioGroup>
                )}
              </FormItem>
            </Col>
            <Col span={24} className="hide">
              <FormItem label="">
                {getFieldDecorator('flowId', {})(
                  <Input />
                )}
              </FormItem>
            </Col>
            <Col span={24} className="hide">
              <FormItem label="">
                {getFieldDecorator('flowStreamId', {})(
                  <Input />
                )}
              </FormItem>
            </Col>
            <Col span={24}>
              <FormItem label="Stream Name" {...itemStyle}>
                {getFieldDecorator('streamName', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('stream name', 'Stream Name')
                  }]
                })(
                  <Select
                    dropdownClassName="ri-workbench-select-dropdown"
                    onChange={(e) => onInitStreamNameSelect(e)}
                    placeholder="Select a Stream Name"
                    disabled={flowDisabledOrNot}
                  >
                    {streamNameOptions}
                  </Select>
                )}
              </FormItem>
            </Col>
            <Col span={24} className="ri-input-text">
              <FormItem label="Kafka Instance" {...itemStyle} >
                {getFieldDecorator('kafkaInstance', {})(
                  <strong className="value-font-style">{flowKafkaInstanceValue}</strong>
                )}
              </FormItem>
            </Col>
            <Col span={24} className="ri-input-text">
              <FormItem label="Exist Kafka Topics" {...itemStyle}>
                {getFieldDecorator('kafkaTopic', {})(
                  <strong className="value-font-style">{flowKafkaTopicValue}</strong>
                )}
              </FormItem>
            </Col>
            {flowSubPanelKey === 'flink' ? (
              <Col span={24}>
                <FormItem label="Parallelism" {...itemStyle}>
                  {getFieldDecorator('parallelism', {
                    rules: [{
                      required: true,
                      message: operateLanguageFillIn('parallelism', 'Parallelism')
                    }],
                    initialValue: 6
                  })(
                    <InputNumber min={1} />
                  )}
                </FormItem>
              </Col>
            ) : ''}
            {flowSubPanelKey === 'flink' ? (
              <Col span={24}>
                <FormItem label="Checkpoint" {...itemStyle}>
                  {getFieldDecorator('checkpoint', {
                    valuePropName: 'checked',
                    initialValue: false
                  })(
                    <Switch />
                  )}
                </FormItem>
              </Col>
            ) : ''}
          </Card>
          <Card title="Source" className="ri-workbench-form-card-style source-card">
            <Col span={24}>
              <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
                {getFieldDecorator('sourceDataSystem', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('data system', 'Data System')
                  }]
                })(
                  <DataSystemSelector
                    flowMode={flowMode}
                    data={sourceDataSystemData()}
                    onItemSelect={this.onSourceDataSystemItemSelect}
                    dataSystemDisabled={flowDisabledOrNot}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={24} className={streamTypeClass[0]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('sourceNamespace', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('namespace', 'Namespace')
                  }],
                  hidden: streamTypeHiddens[0]
                })(
                  <Cascader
                    disabled={flowDisabledOrNot}
                    placeholder="Select a Source Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={defaultSourceNsData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                    onChange={(value, selectedOptions) => initialDefaultCascader(value, selectedOptions)}
                  />
                )}
              </FormItem>
            </Col>

            <Col span={24} className={streamTypeClass[4]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('hdfsNamespace', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('namespace', 'Namespace')
                  }],
                  hidden: streamTypeHiddens[4]
                })(
                  <Cascader
                    disabled={flowDisabledOrNot}
                    placeholder="Select a Source Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={hdfslogSourceNsData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                    onChange={(value, selectedOptions) => initialHdfslogCascader(value, selectedOptions)}
                  />
                )}
              </FormItem>
            </Col>

            <Col span={24} className={streamTypeClass[2]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('routingNamespace', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('namespace', 'Namespace')
                  }],
                  hidden: streamTypeHiddens[2]
                })(
                  <Cascader
                    disabled={flowDisabledOrNot}
                    placeholder="Select a Source Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={routingNsData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                    onChange={(value, selectedOptions) => initialRoutingCascader(value, selectedOptions)}
                  />
                )}
              </FormItem>
            </Col>

            <Col span={24} className={streamTypeClass[0]}>
              <FormItem label="Protocol" {...itemStyle}>
                {getFieldDecorator('protocol', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('protocol', 'Protocol')
                  }],
                  hidden: streamTypeHiddens[0],
                  initialValue: ['increment', 'initial']
                })(
                  <CheckboxGroup options={flowProtocolCheckboxList} />
                  // <RadioGroup className="radio-group-style" size="default">
                  //   {/* <RadioButton value="all" className="radio-btn-style radio-btn-extra">All</RadioButton> */}
                  //   <RadioButton value="increment" className="radio-btn-style radio-btn-extra">Increment</RadioButton>
                  //   <RadioButton value="initial" className="radio-btn-style radio-btn-extra">Initial</RadioButton>
                  //   <RadioButton value="backfill" className="radio-btn-style radio-btn-extra">Backfill</RadioButton>
                  // </RadioGroup>
                )}
              </FormItem>
            </Col>
          </Card>

          <Card title="Sink" className="ri-workbench-form-card-style sink-card">
            <Col span={24} className={streamTypeClass[0]}>
              <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
                {getFieldDecorator('sinkDataSystem', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('data system', 'Data System')
                  }],
                  hidden: streamTypeHiddens[0]
                })(
                  <DataSystemSelector
                    flowMode={flowMode}
                    data={sinkDataSystemData()}
                    onItemSelect={this.onSinkDataSystemItemSelect}
                    dataSystemDisabled={flowDisabledOrNot}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={24} className={streamTypeClass[2]}>
              <FormItem label="Data System" {...itemStyle} style={{lineHeight: '36px'}}>
                {getFieldDecorator('routingSinkDataSystem', {
                  hidden: streamTypeHiddens[2]
                })(
                  <p>kafka</p>
                )}
              </FormItem>
            </Col>
            <Col span={24} className={streamTypeClass[0]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('sinkNamespace', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('namespace', 'Namespace')
                  }],
                  hidden: streamTypeHiddens[0]
                })(
                  <Cascader
                    disabled={flowDisabledOrNot}
                    placeholder="Select a Sink Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={defaultSinkNsData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                    onChange={this.namespaceChange}
                  />
                )}
              </FormItem>
            </Col>
            <Col span={24} className={streamTypeClass[2]}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('routingSinkNs', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('namespace', 'Namespace')
                  }],
                  hidden: streamTypeHiddens[2]
                })(
                  <Cascader
                    disabled={flowDisabledOrNot}
                    placeholder="Select a Sink Namespace"
                    popupClassName="ri-workbench-select-dropdown"
                    options={routingSinkTypeNsData}
                    expandTrigger="hover"
                    displayRender={(labels) => labels.join('.')}
                    onChange={(e) => initialRoutingSinkCascader(e)}
                  />
                )}
              </FormItem>
            </Col>
            {
              streamDiffType === 'default' ? (<Col span={24}>
                <FormItem label="Table keys" {...itemStyle}>
                  {getFieldDecorator('tableKeys')(
                    <Input />
                  )}
                </FormItem>
              </Col>) : ''
            }
            <Col span={24} className={`result-field-class ${streamDiffType === 'default' ? '' : 'hide'}`}>
              <FormItem label="Result Fields" {...itemStyle}>
                {getFieldDecorator('resultFields', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('result fields', 'Result Fields')
                  }],
                  hidden: stepHiddens[1] || streamTypeHiddens[0]
                })(
                  <RadioGroup className="radio-group-style" onChange={(e) => initResultFieldClass(e.target.value)} size="default">
                    <RadioButton value="all" className="radio-btn-style fradio-btn-extra">All</RadioButton>
                    <RadioButton value="selected" className="radio-btn-style radio-btn-extra">Selected</RadioButton>
                  </RadioGroup>
                )}
              </FormItem>
            </Col>
            <Col span={6}></Col>
            <Col span={17} className={`${fieldSelected}`}>
              <FormItem>
                {getFieldDecorator('resultFieldsSelected', {
                  hidden: stepHiddens[1] || streamTypeHiddens[0]
                })(
                  <Input type="textarea" placeholder="Result Fields 多条时以英文逗号分隔" autosize={{ minRows: 2, maxRows: 6 }} />
                )}
              </FormItem>
            </Col>

            <Col span={24} className={streamTypeClass[0]} style={{marginBottom: '8px'}}>
              <div className="ant-col-6 ant-form-item-label">
                <label htmlFor="#" className={sinkConfigClass}>Sink Config</label>
              </div>
              <div className="ant-col-17">
                <div className="ant-form-item-control">
                  {sinkConfigTag}
                </div>
              </div>
            </Col>
            <Col span={24} className="hide">
              <FormItem>
                {getFieldDecorator('sinkConfig', {
                  hidden: streamTypeHiddens[0]
                })(<Input />)}
              </FormItem>
            </Col>
            <Col span={24} className={`ri-input-text ${streamTypeClass[4]}`}>
              <FormItem label="Data System" {...itemStyle}>
                {getFieldDecorator('hdfsDataSys', {
                  hidden: streamTypeHiddens[4]
                })(
                  <strong className="value-font-style">{hdfslogSinkDSValue}</strong>
                )}
              </FormItem>
            </Col>
            <Col span={24} className={`ri-input-text ${streamTypeClass[4]}`}>
              <FormItem label="Namespace" {...itemStyle}>
                {getFieldDecorator('hdfsSinkNs', {
                  hidden: streamTypeHiddens[4]
                })(
                  <strong className="value-font-style">{hdfsSinkNsValue}</strong>
                )}
              </FormItem>
            </Col>
          </Card>
        </Row>
        {/* Step 2 */}
        <Row gutter={8} className={`${stepClassNames[1]} ${streamTypeClass[0]}`}>
          <Col span={24}>
            <FormItem label="Source Namespace" {...itemStyle}>
              {getFieldDecorator('step2SourceNamespace', {
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <strong className="value-font-style">{step2SourceNamespace}</strong>
              )}
            </FormItem>
          </Col>
          <Col span={24}>
            <FormItem label="Sink Namespace" {...itemStyle}>
              {getFieldDecorator('step2SinkNamespace', {
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <strong className="value-font-style">{step2SinkNamespace}</strong>
              )}
            </FormItem>
          </Col>

          <Col span={24}>
            <FormItem label="Transformation" {...itemStyle}>
              {getFieldDecorator('transformation', {
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <Tag className={transformTagClassName} onClick={onShowTransformModal}>
                  <Icon type="minus-circle-o" /> <FormattedMessage {...messages.workbenchConfigBtn} />
                </Tag>
              )}
            </FormItem>
          </Col>

          <Col span={6}></Col>
          <Col span={18} className={transformTableClassName}>
            <Table
              dataSource={transformTableSource}
              columns={columns}
              pagination={pagination}
              bordered
              className="tran-table-style"
            />
          </Col>

          <Col span={24} className={transConnectClass} style={{marginBottom: '8px'}}>
            <div className="ant-col-6 ant-form-item-label">
              <label htmlFor="#">Transformation Config</label>
            </div>
            <div className="ant-col-17">
              <div className="ant-form-item-control">
                {flowSpecialConfigTag}
              </div>
            </div>
          </Col>
          <Col span={24} className="hide">
            <FormItem>
              {getFieldDecorator('flowSpecialConfig', {
                hidden: streamTypeHiddens[0]
              })(<Input />)}
            </FormItem>
          </Col>
          {flowSubPanelKey === 'flink' ? '' : (
            <Col span={24} className={transConnectClass}>
              <div className="ant-col-6 ant-form-item-label">
                <label htmlFor="#">Event Time Processing</label>
              </div>
              <div className="ant-col-17">
                <div className="ant-form-item-control">
                  {etpStrategyTag}
                </div>
              </div>
            </Col>
          )}
          <Col span={24} className="hide">
            <FormItem>
              {getFieldDecorator('etpStrategy', {
                hidden: flowSubPanelKey === 'flink'
              })(<Input />)}
            </FormItem>
          </Col>
          {flowSubPanelKey === 'flink' ? '' : (
            <Col span={16} className={`ds-class ${transConnectClass}`}>
              <FormItem label="Sample Show" {...itemStyleDFS}>
                {getFieldDecorator('dataframeShow', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('sample show', 'Sample Show')
                  }],
                  hidden: stepHiddens[1] || transformTableClassName || streamTypeHiddens[0]
                })(
                  <RadioGroup className="radio-group-style" onChange={(e) => initDataShowClass(e.target.value)} size="default">
                    <RadioButton value="false" className="radio-btn-style radio-btn-extra">False</RadioButton>
                    <RadioButton value="true" className="radio-btn-style">True</RadioButton>
                  </RadioGroup>
                )}
              </FormItem>
            </Col>
          )}
          {flowSubPanelKey === 'flink' ? '' : (
            <Col span={7} className={`ds-class ${dataframeShowSelected}`}>
              <FormItem label="Number" {...itemStyleDFSN}>
                {getFieldDecorator('dataframeShowNum', {
                  rules: [{
                    required: true,
                    message: operateLanguageFillIn('number', 'Number')
                  }, {
                    validator: forceCheckNum
                  }],
                  initialValue: 10,
                  hidden: stepHiddens[1] || streamTypeHiddens[0]
                })(
                  <InputNumber min={10} step={10} />
                )}
              </FormItem>
            </Col>
          )}
          {flowSubPanelKey === 'flink' ? '' : (
            <Col span={24} className="hide">
              <FormItem label="Swifts Specific Config" {...itemStyle}>
                {getFieldDecorator('swiftsSpecificConfig', {
                  hidden: stepHiddens[1] || streamTypeHiddens[0]
                })(
                  <Input placeholder="Swifts Specific Config" />
                )}
              </FormItem>
            </Col>
          )}
          {flowSubPanelKey === 'flink' ? (
            <Col span={16} className={`ds-class ${transConnectClass}`}>
              <FormItem label="Time Characteristic" labelCol={{span: 9}} wrapperCol={{span: 14}}>
                {getFieldDecorator('time_characteristic', {
                  rules: [{
                    required: true,
                    message: operateLanguageSelect('Time Characteristic', 'Time Characteristic')
                  }],
                  hidden: stepHiddens[1] || transConnectClass === 'hide',
                  initialValue: 'processing_time'
                })(
                  <RadioGroup className="radio-group-style" size="default">
                    <RadioButton value="processing_time">Processing Time</RadioButton>
                    <RadioButton value="event_time">Event Time</RadioButton>
                  </RadioGroup>
                )}
              </FormItem>
            </Col>
          ) : ''}
          {flowSubPanelKey === 'flink' ? (
            <Col span={24} className="hide">
              <FormItem>
                {getFieldDecorator('flinkFlowDirective', {
                  hidden: flowSubPanelKey === 'spark'
                })(<Input />)}
              </FormItem>
            </Col>
          ) : ''}
          <Col span={24}>
            <FormItem label="Debug" {...itemStyle}>
              {getFieldDecorator('debugLogPath', {
                hidden: stepHiddens[1] || streamTypeHiddens[0]
              })(
                <div className="debug">
                  <Button type="ghost" className="next" onClick={this.handleStartDebug} loading={debugRunning} size="default">
                    {debugRunning ? 'Running' : 'Start'}
                  </Button>
                  &nbsp;
                  {debugRunning ? (
                    <Button type="danger" className="next" onClick={this.handleStopDebug} size="default">
                      {'Stop'}
                    </Button>) : (
                    ''
                  )}
                </div>
              )}
            </FormItem>
          </Col>
          <Modal
            title={<FormattedMessage {...messages.workbenchSureDebug} />}
            visible={debugModalVisible}
            wrapClassName="ant-modal-large stream-start-renew-modal"
            onCancel={this.handleDebugCancel}
            footer={[
              <Button
                className={`query-offset-btn`}
                key="query"
                size="large"
                onClick={this.queryLastestoffset}
              >
                <FormattedMessage {...messages.workbenchModalView} /> Latest Offset
              </Button>,
              <Button
                className={`edit-topic-btn`}
                type="default"
                onClick={this.onChangeEditSelect}
                key="renewEdit"
                size="large">
                <FormattedMessage {...messages.workbenchModalReset} />
              </Button>,
              <Button
                key="cancel"
                size="large"
                onClick={this.handleDebugCancel}
              >
                <FormattedMessage {...messages.workbenchModalCancel} />
              </Button>,
              <Button
                key="submit"
                size="large"
                type="primary"
                loading={debugModalLoading}
                onClick={this.handleEditDebugOk}
              >
                <FormattedMessage {...messages.workbenchTableStart} />
              </Button>
            ]}
          >
            {workbenchDebugForm}
          </Modal>
        </Row>
        {/* Step 3 */}
        <Row gutter={8} className={`ri-workbench-confirm-step ${stepClassNames[2]}`}>
          {step3ConfirmContent}
          <Col span={24}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Kafka Instance</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{flowKafkaInstanceValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Exist Kafka Topics</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{flowKafkaTopicValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          {step3ConfirmDSNS}
          <Col span={24} className={streamTypeClass[0]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Result Fields</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{resultFieldsValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[0]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Transformation</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{transformTableConfirmValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[0]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Transformation Config</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{transConfigConfirmValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          {flowSubPanelKey === 'flink' ? '' : (
            <Col span={24} className={`${transConnectClass} ${streamTypeClass[0]}`}>
              <div className="ant-row ant-form-item">
                <Row>
                  <Col span={8} className="ant-form-item-label">
                    <label htmlFor="#">Event Time Processing</label>
                  </Col>
                  <Col span={15}>
                    <div className="ant-form-item-control">
                      <strong className="value-font-style">{etpStrategyConfirmValue}</strong>
                    </div>
                  </Col>
                </Row>
              </div>
            </Col>
          )}
          {flowSubPanelKey === 'flink' ? '' : (
            <Col span={24} className={`${transConnectClass} ${streamTypeClass[0]}`}>
              <div className="ant-row ant-form-item">
                <Row>
                  <Col span={8} className="ant-form-item-label">
                    <label htmlFor="#">Dataframe Show</label>
                  </Col>
                  <Col span={15}>
                    <div className="ant-form-item-control">
                      <strong className="value-font-style">{this.props.dataframeShowNumValue}</strong>
                    </div>
                  </Col>
                </Row>
              </div>
            </Col>
          )}
          {flowSubPanelKey === 'flink' ? (
            <Col span={24} className={`${transConnectClass} ${streamTypeClass[0]}`}>
              <div className="ant-row ant-form-item">
                <Row>
                  <Col span={8} className="ant-form-item-label">
                    <label htmlFor="#">Time Characteristic</label>
                  </Col>
                  <Col span={15}>
                    <div className="ant-form-item-control">
                      <strong className="value-font-style">{timeCharacteristic}</strong>
                    </div>
                  </Col>
                </Row>
              </div>
            </Col>
          ) : ''}
          <Col span={24} className={streamTypeClass[4]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Source Data System</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{flowSourceNsSys}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[4]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Source Namespace</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{hdfsSinkNsValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[4]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Sink Data System</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{flowSourceNsSys}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[4]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Sink Namespace</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{hdfsSinkNsValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[2]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Source Namespace</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{routingSourceNsValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[2]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Sink Data System</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">kafka</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
          <Col span={24} className={streamTypeClass[2]}>
            <div className="ant-row ant-form-item">
              <Row>
                <Col span={8} className="ant-form-item-label">
                  <label htmlFor="#">Sink Namespace</label>
                </Col>
                <Col span={15}>
                  <div className="ant-form-item-control">
                    <strong className="value-font-style">{routingSinkNsValue}</strong>
                  </div>
                </Col>
              </Row>
            </div>
          </Col>
        </Row>
      </Form>
    )
  }
}

WorkbenchFlowForm.propTypes = {
  step: PropTypes.number,
  transformTableSource: PropTypes.array,
  form: PropTypes.any,
  projectIdGeted: PropTypes.string,
  flowMode: PropTypes.string,
  streamId: PropTypes.number,
  onShowTransformModal: PropTypes.func,
  onShowEtpStrategyModal: PropTypes.func,
  onShowSinkConfigModal: PropTypes.func,
  onShowSpecialConfigModal: PropTypes.func,
  onDeleteSingleTransform: PropTypes.func,
  onAddTransform: PropTypes.func,
  onEditTransform: PropTypes.func,
  onUpTransform: PropTypes.func,
  onDownTransform: PropTypes.func,
  step2SourceNamespace: PropTypes.string,
  step2SinkNamespace: PropTypes.string,
  etpStrategyCheck: PropTypes.bool,
  transformTagClassName: PropTypes.string,
  transformTableClassName: PropTypes.string,
  transConnectClass: PropTypes.string,
  selectStreamKafkaTopicValue: PropTypes.array,
  routingSinkTypeNsData: PropTypes.array,
  onInitSinkTypeNamespace: PropTypes.func,
  onInitStreamNameSelect: PropTypes.func,
  resultFieldsValue: PropTypes.string,
  dataframeShowNumValue: PropTypes.string,
  etpStrategyConfirmValue: PropTypes.string,
  transConfigConfirmValue: PropTypes.string,
  transformTableConfirmValue: PropTypes.string,
  fieldSelected: PropTypes.string,
  dataframeShowSelected: PropTypes.string,
  streamDiffType: PropTypes.string,
  hdfsSinkNsValue: PropTypes.string,
  routingSourceNsValue: PropTypes.string,
  routingSinkNsValue: PropTypes.string,
  initResultFieldClass: PropTypes.func,
  initDataShowClass: PropTypes.func,
  onInitStreamTypeSelect: PropTypes.func,
  initialHdfslogCascader: PropTypes.func,
  initialDefaultCascader: PropTypes.func,
  initialRoutingSinkCascader: PropTypes.func,
  initialRoutingCascader: PropTypes.func,
  flowKafkaTopicValue: PropTypes.string,
  flowKafkaInstanceValue: PropTypes.string,
  onLoadSourceSinkTypeNamespace: PropTypes.func,
  onLoadSinkTypeNamespace: PropTypes.func,
  sinkConfigCopy: PropTypes.string,
  flowSourceNsSys: PropTypes.string,
  emitDataSystem: PropTypes.func,
  changeStreamType: PropTypes.func,
  flowSubPanelKey: PropTypes.string,
  emitFlowFunctionType: PropTypes.func,
  timeCharacteristic: PropTypes.string,
  locale: PropTypes.string,
  onDebugFlow: PropTypes.func,
  onLoadUdfs: PropTypes.func,
  onLoadSingleUdf: PropTypes.func,
  onLoadLastestOffset: PropTypes.func,
  onStartDebug: PropTypes.func,
  onStopDebug: PropTypes.func,
  debugModalLoading: PropTypes.bool
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadSourceSinkTypeNamespace: (projectId, streamId, value, type, resolve) => dispatch(loadSourceSinkTypeNamespace(projectId, streamId, value, type, resolve)),
    onLoadSinkTypeNamespace: (projectId, streamId, value, type, resolve) => dispatch(loadSinkTypeNamespace(projectId, streamId, value, type, resolve)),
    onLoadUdfs: (projectId, type, id, roleType, resolve) => dispatch(loadUdfs(projectId, type, id, roleType, resolve)),
    onLoadSingleUdf: (projectId, roleType, resolve, type) => dispatch(loadSingleUdf(projectId, roleType, resolve, type)),
    onLoadLastestOffset: (projectId, type, id, sourceNs, resolve, method, topics) => dispatch(loadLastestOffset(projectId, type, id, sourceNs, resolve, method, topics)),
    onStartDebug: (projectId, id, topicResult, action, resolve, reject) => dispatch(startDebug(projectId, id, topicResult, action, resolve, reject)),
    onStopDebug: (projectId, id, logPath, resolve) => dispatch(stopDebug(projectId, id, logPath, resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  debugModalLoading: selectDebugModalLoading()
})

export default Form.create({wrappedComponentRef: true})(connect(mapStateToProps, mapDispatchToProps)(WorkbenchFlowForm))
