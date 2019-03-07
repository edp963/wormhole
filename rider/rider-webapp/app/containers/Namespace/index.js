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
import CodeMirror from 'codemirror'
require('../../../node_modules/codemirror/addon/display/placeholder')
require('../../../node_modules/codemirror/mode/javascript/javascript')
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import Table from 'antd/lib/table'
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
import Button from 'antd/lib/button'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
import Popconfirm from 'antd/lib/popconfirm'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import {
  jsonParse,
  fieldTypeAlter,
  renameAlter,
  genDefaultSchemaTable,
  umsSysFieldSelected,
  umsSysFieldCanceled,
  getRepeatFieldIndex,
  genSchema
} from './umsFunction'
import { isJSONNotEmpty, operateLanguageText } from '../../utils/util'
import { filterDataSystemData } from '../../components/DataSystemSelector/dataSystemFunction'

import NamespaceForm from './NamespaceForm'
import SchemaTypeConfig from './SchemaTypeConfig'
import SinkSchemaTypeConfig from './SinkSchemaTypeConfig'

import { loadDatabasesInstance } from '../DataBase/action'
import { selectDbUrlValue } from '../DataBase/selectors'
import { selectRoleType } from '../App/selectors'
import { loadSingleInstance } from '../Instance/action'
import {
  loadAdminAllNamespaces,
  loadUserNamespaces,
  loadSelectNamespaces,
  loadNamespaceDatabase,
  addNamespace,
  editNamespace,
  loadTableNameExist,
  loadSingleNamespace,
  setSchema,
  querySchemaConfig,
  deleteNs
} from './action'
import {
  selectNamespaces,
  selectError,
  selectModalLoading,
  selectTableNameExited
} from './selectors'
import { selectLocale } from '../LanguageProvider/selectors'

export class Namespace extends React.PureComponent {
  constructor (props) {
    super(props)
    this.state = {
      formVisible: false,
      namespaceFormType: 'add',
      refreshNsLoading: false,
      refreshNsText: 'Refresh',
      showNsDetail: {},

      currentNamespaces: [],
      originNamespaces: [],

      filteredInfo: null,
      sortedInfo: null,

      searchNsInstance: '',
      filterDropdownVisibleNsInstance: false,
      searchNsDatabase: '',
      filterDropdownVisibleNsDatabase: false,
      searchNsTable: '',
      filterDropdownVisibleNsTable: false,
      searchNsKey: '',
      filterDropdownVisibleNsKey: false,
      searchNstopic: '',
      filterDropdownVisibleNsTopic: false,
      filterDatepickerShown: false,
      startTimeText: '',
      endTimeText: '',
      createStartTimeText: '',
      createEndTimeText: '',
      filterDropdownVisibleCreateTime: false,
      updateStartTimeText: '',
      updateEndTimeText: '',
      filterDropdownVisibleUpdateTime: false,

      columnNameText: '',
      valueText: '',
      visibleBool: false,
      startTimeTextState: '',
      endTimeTextState: '',
      paginationInfo: null,

      databaseSelectValue: [],
      deleteTableClass: 'hide',
      addTableClass: '',
      addTableClassTable: 'hide',
      addBtnDisabled: false,
      count: 0,
      namespaceTableSource: [],

      editNamespaceData: {},
      exitedNsTableValue: '',
      nsDsVal: '',
      nsInstanceVal: '',
      queryConnUrl: '',

      schemaModalVisible: false,
      jsonSampleValue: [],
      umsTableDataSource: [],
      umsTypeSeleted: 'ums',
      nsIdValue: 0,
      repeatRenameArr: [],
      repeatArrayArr: [],
      selectAllState: 'all',
      beforesepratorValue: '',
      umsopRecordValue: -1,

      sinkSchemaModalVisible: false,
      sinkTableDataSource: [],
      sinkJsonSampleValue: [],
      sinkSelectAllState: 'all',

      umsopable: false,
      umsopKey: -1
    }
  }

  componentWillMount () {
    const { roleType } = this.props
    if (roleType === 'admin') {
      if (!this.props.namespaceClassHide) {
        this.props.onLoadAdminAllNamespaces(() => { this.nsRefreshState() })
      }
    }
  }

  componentWillReceiveProps (props) {
    if (props.namespaces) {
      const originNamespaces = props.namespaces.map(s => {
        s.key = s.id
        s.visible = false
        return s
      })
      this.setState({ originNamespaces: originNamespaces.slice() })
      this.state.columnNameText === ''
        ? this.setState({ currentNamespaces: originNamespaces.slice() })
        : this.searchOperater()
    }
  }

  searchOperater () {
    const { columnNameText, valueText, visibleBool, startTimeTextState, endTimeTextState } = this.state

    if (columnNameText !== '') {
      this.onSearch(columnNameText, valueText, visibleBool)()

      if (columnNameText === 'createTime' || columnNameText === 'updateTime') {
        this.onRangeTimeSearch(columnNameText, startTimeTextState, endTimeTextState, visibleBool)()
      }
    }
  }

  refreshNamespace = () => {
    const { projectIdGeted, namespaceClassHide, roleType } = this.props

    this.setState({
      refreshNsLoading: true,
      refreshNsText: 'Refreshing'
    })
    if (roleType === 'admin') {
      namespaceClassHide === 'hide'
        ? this.props.onLoadSelectNamespaces(projectIdGeted, () => { this.nsRefreshState() })
        : this.props.onLoadAdminAllNamespaces(() => { this.nsRefreshState() })
    } else if (roleType === 'user') {
      this.props.onLoadUserNamespaces(projectIdGeted, () => { this.nsRefreshState() })
    }
  }

  nsRefreshState () {
    this.setState({
      refreshNsLoading: false,
      refreshNsText: 'Refresh'
    })

    const { paginationInfo, filteredInfo, sortedInfo } = this.state
    this.handleNamespaceChange(paginationInfo, filteredInfo, sortedInfo)
    this.searchOperater()
  }

  handleNamespaceChange = (pagination, filters, sorter) => {
    if (filters) {
      if (filters.nsSys) {
        if (filters.nsSys.length !== 0) {
          this.onSearch('', '', false)()
        }
      }
    }
    this.setState({
      filteredInfo: filters,
      sortedInfo: sorter,
      paginationInfo: pagination
    })
  }

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      filteredInfo: {nsSys: []}
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        valueText: value,
        visibleBool: visible,
        currentNamespaces: this.state.originNamespaces.map((record) => {
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

  onInputChange = (value) => (e) => this.setState({ [value]: e.target.value })
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
      filteredInfo: {nsSys: []}
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        startTimeTextState: startTimeText,
        endTimeTextState: endTimeText,
        visibleBool: visible,
        currentNamespaces: this.state.originNamespaces.map((record) => {
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

  showAddNamespace = () => {
    this.setState({
      formVisible: true,
      namespaceFormType: 'add',
      addTableClass: '',
      addTableClassTable: 'hide',
      addBtnDisabled: false
    })
  }

  showEditNamespace = (record) => (e) => {
    this.setState({
      formVisible: true,
      namespaceFormType: 'edit',
      deleteTableClass: 'hide',
      addTableClass: 'hide',
      addTableClassTable: 'hide',
      namespaceTableSource: []
    }, () => {
      new Promise((resolve) => {
        this.props.onLoadSingleNamespace(record.id, (result) => {
          resolve(result)
          this.namespaceForm.setFieldsValue({
            dataBaseDataSystem: result.nsSys,
            nsDatabase: [result.nsDatabase],
            instance: result.nsInstance,
            nsSingleTableName: result.nsTable,
            nsSingleKeyValue: result.keys
          })

          this.setState({
            editNamespaceData: {
              id: result.id,
              nsSys: result.nsSys,
              nsInstance: result.nsInstance,
              nsDatabase: result.nsDatabase,
              nsTable: result.nsTable,
              nsVersion: result.nsVersion,
              nsDbpar: result.nsDbpar,
              nsTablepar: result.nsTablepar,
              nsDatabaseId: result.nsDatabaseId,
              nsInstanceId: result.nsInstanceId,
              active: result.active,
              createTime: result.createTime,
              createBy: result.createBy,
              updateTime: result.updateTime,
              updateBy: result.updateBy,
              topic: result.topic
            },
            addTableClass: '',
            addBtnDisabled: true
          })
        })
      })
        .then((result) => {
          this.props.onLoadSingleInstance(result.nsInstanceId, (result) => {
            this.setState({ queryConnUrl: result.connUrl })
          })
        })
    })
  }

  // 点击遮罩层或右上角叉或取消按钮的回调
  hideForm = () => {
    this.setState({
      formVisible: false,
      queryConnUrl: ''
    })
  }

  // Modal 完全关闭后的回调
  resetModal = () => {
    this.namespaceForm.resetFields()
    this.cleanNsTableData()
  }

  // 清table data
  cleanNsTableData = () => {
    this.namespaceForm.setFields({
      nsSingleTableName: '',
      nsSingleKeyValue: '',
      nsDatabase: undefined,
      nsTables: {
        errors: []
      }
    })
    this.setState({ namespaceTableSource: [] })
  }

  nsErrorMsg = (msg) => {
    this.namespaceForm.setFields({
      nsTables: {
        errors: [new Error(msg)]
      }
    })
  }

  nsAdd = (value) => {
    this.props.onAddNamespace(value, () => {
      this.hideForm()
      message.success('Namespace 添加成功！', 3)
    })
  }

  nsKeyAdd (addTableValue, addKeyValue, requestOthers) {
    const requestNsTables = [{
      table: addTableValue,
      key: addKeyValue
    }]
    const addValues = Object.assign(requestOthers, { nsTables: requestNsTables })
    this.nsAdd(addValues)
  }

  nsTableInputAdd (requestNsTables, requestOthers) {
    const { namespaceTableSource } = this.state

    namespaceTableSource.map(i => {
      requestNsTables.push({
        table: i.nsModalTable,
        key: i.nsModalKey
      })
      return i
    })

    const addValues = Object.assign(requestOthers, { nsTables: requestNsTables })
    this.nsAdd(addValues)
  }

  nsTableAdd (requestNsTables, addTableValue, addKeyValue, requestOthers) {
    const { namespaceTableSource } = this.state
    const { locale } = this.props
    const tableText = locale === 'en' ? 'Duplication of Table name' : 'Table 重名'

    requestNsTables.push({
      table: addTableValue,
      key: addKeyValue
    })

    if (namespaceTableSource.find(i => i.nsModalTable === addTableValue)) {
      this.nsErrorMsg(tableText)
    } else {
      const addValues = Object.assign(requestOthers, { nsTables: requestNsTables })
      this.nsAdd(addValues)
    }
  }

  onModalOk = () => {
    const {
      namespaceTableSource, databaseSelectValue, namespaceFormType, exitedNsTableValue,
      editNamespaceData, queryConnUrl, nsInstanceVal
    } = this.state
    const { tableNameExited, locale } = this.props
    const existText = locale === 'en' ? 'already exists' : '已存在'
    const typeText = locale === 'en' ? 'Please fill in type' : '请填写 Type'
    const tableText = locale === 'en' ? 'Please fill in the Table' : '请填写 Table'
    const keyText = locale === 'en' ? 'Please fill in the Key' : '请填写 Key'
    const typeKeyText = locale === 'en' ? 'Both Type and Key should be filled in' : 'Type & Key 填写同步'
    const tableKeyText = locale === 'en' ? 'Both Table and Key should be filled in' : 'Table & Key 填写同步'
    const successText = locale === 'en' ? 'Namespace is modified successfully!' : 'Namespace 修改成功！'

    this.namespaceForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (tableNameExited) {
          this.nsErrorMsg(`${exitedNsTableValue} ${existText}`)
        } else {
          const selDatabase = databaseSelectValue.find(s => s.id === Number(values.nsDatabase))
          const addTableValue = values.nsSingleTableName
          const addKeyValue = values.nsSingleKeyValue

          switch (namespaceFormType) {
            case 'add':
              const instanceTemp = nsInstanceVal.filter(i => i.id === Number(values.instance))

              let requestNsTables = []
              let requestOthers = {
                nsDatabase: selDatabase.nsDatabase,
                nsDatabaseId: Number(values.nsDatabase),
                nsInstance: instanceTemp[0].nsInstance,
                nsInstanceId: Number(values.instance),
                nsSys: values.dataBaseDataSystem
              }

              if (namespaceTableSource.length === 0) {
                if (!addTableValue) {
                  this.nsErrorMsg(values.dataBaseDataSystem === 'es' ? typeText : tableText)
                } else {
                  if (values.dataBaseDataSystem === 'redis') {
                    this.namespaceForm.setFields({
                      nsTables: {
                        errors: []
                      }
                    })
                    this.nsKeyAdd(addTableValue, addKeyValue, requestOthers)
                  } else {
                    !addKeyValue
                      ? this.nsErrorMsg(keyText)
                      : this.nsKeyAdd(addTableValue, addKeyValue, requestOthers)
                  }
                }
              } else {
                if (values.dataBaseDataSystem === 'redis') {
                  if (addTableValue === '' && addKeyValue === '') { // 当tables表格有数据时，table input 和 key input 可以为空
                    this.nsTableInputAdd(requestNsTables, requestOthers)
                  } else {
                    namespaceTableSource.map(i => {
                      requestNsTables.push({
                        table: i.nsModalTable,
                        key: ''
                      })
                      return i
                    })
                    this.nsTableAdd(requestNsTables, addTableValue, addKeyValue, requestOthers)
                  }
                } else {
                  if ((addTableValue === '' && addKeyValue !== '') || (addTableValue !== '' && addKeyValue === '')) {
                    this.nsErrorMsg(values.dataBaseDataSystem === 'es' ? typeKeyText : tableKeyText)
                  } else if (addTableValue === '' && addKeyValue === '') {
                    this.nsTableInputAdd(requestNsTables, requestOthers)
                  } else if (addTableValue !== '' && addKeyValue !== '') {
                    namespaceTableSource.map(i => {
                      requestNsTables.push({
                        table: i.nsModalTable,
                        key: i.nsModalKey
                      })
                      return i
                    })
                    this.nsTableAdd(requestNsTables, addTableValue, addKeyValue, requestOthers)
                  }
                }
              }
              break
            case 'edit':
              const editKeysValue = values.nsSingleKeyValue

              if (values.dataBaseDataSystem === 'redis') {
                this.props.onEditNamespace(Object.assign(editNamespaceData, queryConnUrl, { keys: '' }), () => {
                  this.hideForm()
                  message.success(successText, 3)
                })
              } else {
                if (editKeysValue === '') {
                  this.nsErrorMsg(keyText)
                } else {
                  this.props.onEditNamespace(Object.assign(editNamespaceData, queryConnUrl, { keys: editKeysValue }), () => {
                    this.hideForm()
                    message.success(successText, 3)
                  })
                }
              }
              break
          }
        }
      }
    })
  }

  // 新增时，通过选择不同的 data system 显示不同的 Instance 内容
  onInitNamespaceUrlValue = (value) => {
    this.setState({ addTableClassTable: 'hide' })

    this.props.onLoadDatabasesInstance(value, (result) => {
      this.setState({
        databaseSelectValue: [],
        nsDsVal: value,
        nsInstanceVal: result
      })
      // namespaceForm 的 placeholder
      this.namespaceForm.setFieldsValue({
        connectionUrl: '',
        instance: undefined,
        nsDatabase: undefined
      })
      this.cleanNsTableData()
    })
  }

  onInitDatabaseSelectValue = (value, url) => {
    this.props.onLoadNamespaceDatabase(value, (result) => {
      this.setState({
        databaseSelectValue: result,
        queryConnUrl: url
      })
      this.cleanNsTableData()
    })
  }

  onDeleteTable = (index) => (e) => {
    const { namespaceTableSource } = this.state
    this.namespaceForm.setFields({
      nsTables: { errors: [] }
    })
    namespaceTableSource.splice(index, 1)
    this.setState({
      namespaceTableSource: [...namespaceTableSource]
    }, () => {
      if (namespaceTableSource.length === 0) {
        this.setState({ addTableClassTable: 'hide' })
      }
    })
  }

  onAddTable = () => {
    const { namespaceTableSource, exitedNsTableValue, nsDsVal } = this.state
    const { tableNameExited, locale } = this.props
    const existText = locale === 'en' ? 'already exists' : '已存在'
    const typeText = locale === 'en' ? 'Please fill in the Type' : '请填写 Type'
    const tableText = locale === 'en' ? 'Please fill in the Table' : '请填写 Table'
    const keyText = locale === 'en' ? 'Please fill in the Key' : '请填写 Key'
    const tableDupText = locale === 'en' ? 'Duplication of Table name' : 'Table 重名'
    const otherText = locale === 'en' ? 'Please select other items first' : '请先选择其他项'

    const moadlTempVal = this.namespaceForm.getFieldsValue()
    const { dataBaseDataSystem, connectionUrl, instance, nsDatabase, nsSingleTableName, nsSingleKeyValue } = moadlTempVal

    if (!dataBaseDataSystem || !connectionUrl || !instance || !nsDatabase) {
      this.nsErrorMsg(otherText)
    } else if (!nsSingleTableName) {
      this.nsErrorMsg(nsDsVal === 'es' ? typeText : tableText)
    } else if (tableNameExited) {
      this.nsErrorMsg(`${exitedNsTableValue} ${existText}`)
    } else if (namespaceTableSource.find(i => i.nsModalTable === nsSingleTableName)) {
      this.nsErrorMsg(tableDupText)
    } else {
      if (nsDsVal === 'redis') {
        this.addTableTemp('')
      } else {
        nsSingleKeyValue
          ? this.addTableTemp(nsSingleKeyValue)
          : this.nsErrorMsg(keyText)
      }
    }
  }

  addTableTemp (val) {
    const { count, namespaceTableSource } = this.state
    const moadlTempVal = this.namespaceForm.getFieldsValue()

    const nsTableSourceTemp = {
      key: count,
      nsModalTable: moadlTempVal.nsSingleTableName,
      nsModalKey: val
    }

    this.setState({
      namespaceTableSource: [...namespaceTableSource, nsTableSourceTemp],
      count: count + 1,
      deleteTableClass: '',
      addTableClassTable: ''
    }, () => {
      this.namespaceForm.setFieldsValue({
        nsSingleTableName: '',
        nsSingleKeyValue: '',
        nsTables: { errors: [] }
      })
    })
  }

  // 新增时，验证 table name 是否存在，不存在时，才能新增
  onInitNsNameInputValue = (val) => {
    const formValues = this.namespaceForm.getFieldsValue()

    const requestValues = {
      instanceId: Number(formValues.instance),
      databaseId: Number(formValues.nsDatabase),
      tableNames: val
    }

    this.props.onLoadTableNameExist(requestValues, () => {
      this.namespaceForm.setFields({
        nsTables: { errors: [] }
      })
    }, (err) => {
      this.nsErrorMsg(err)
      this.setState({ exitedNsTableValue: val })
    })
  }

  // table key 不为空时
  onInitNsKeyInputValue = (val) => {
    if (val !== '') {
      this.namespaceForm.setFields({
        nsTables: { errors: [] }
      })
    }
  }

  showEditUms = (record) => (e) => {
    this.setState({
      schemaModalVisible: true,
      nsIdValue: record.id
    }, () => {
      if (this.cmSample) {
        this.cmSample.doc.setValue(this.cmSample.doc.getValue() || '')
      }
      this.makeCodeMirrorInstance()

      this.props.onQuerySchemaConfig(this.sourceSinkRequestParam(record), 'source', (result) => {
        if (!result) {
          this.schemaTypeConfig.setFieldsValue({ umsType: 'ums' })
          this.setState({ umsTypeSeleted: 'ums' })
        } else {
          this.schemaTypeConfig.setFieldsValue({ umsType: result.umsType })
          this.setState({
            umsTypeSeleted: result.umsType
          }, () => {
            if (this.state.umsTypeSeleted === 'ums_extension') {
              this.cmSample.doc.setValue(result.jsonSample)

              setTimeout(() => this.onJsonFormat(), 205)

              const tableData = result.umsSchemaTable.map((s, index) => {
                s.key = index
                return s
              })
              this.setState({
                umsTableDataSource: tableData,
                jsonSampleValue: result.jsonSample
              }, () => {
                const tempArr = this.state.umsTableDataSource.filter(s => !s.forbidden)
                const selectedArr = tempArr.filter(s => s.selected)

                let tempState = ''
                if (selectedArr.length !== 0) {
                  tempState = selectedArr.length === tempArr.length ? 'all' : 'part'
                } else {
                  tempState = 'not'
                }
                this.setState({ selectAllState: tempState })
              })
            } else {
              this.makeCodeMirrorInstance()
              this.cmSample.doc.setValue('')
            }
          })
        }
      })
    })
  }

  sourceSinkRequestParam (record) {
    const { projectIdGeted, namespaceClassHide, roleType } = this.props

    let requestParam = {}
    if (roleType === 'admin') {
      requestParam = namespaceClassHide === 'hide'
        ? {
          projectId: projectIdGeted,
          namespaceId: record.id
        }
        : { namespaceId: record.id }
    } else if (roleType === 'user') {
      requestParam = {
        projectId: projectIdGeted,
        namespaceId: record.id
      }
    }
    return requestParam
  }

  showEditSink = (record) => (e) => {
    this.setState({
      sinkSchemaModalVisible: true,
      nsIdValue: record.id
    }, () => {
      if (this.cmSinkSample) {
        this.cmSinkSample.doc.setValue(this.cmSinkSample.doc.getValue() || '')
      }
      this.makeSinkCodeMirrorInstance()

      this.props.onQuerySchemaConfig(this.sourceSinkRequestParam(record), 'sink', (result) => {
        const { jsonSample, schemaTable, jsonParseArray, schema } = result
        if (result) {
          if (jsonSample === null && jsonParseArray === null &&
            schema === null && schemaTable === null) {
            this.makeSinkCodeMirrorInstance()
            this.cmSinkSample.doc.setValue('')
          } else {
            this.cmSinkSample.doc.setValue(jsonSample)

            setTimeout(() => this.onSinkJsonFormat(), 205)

            const tableData = schemaTable.map((s, index) => {
              s.key = index
              return s
            })
            this.setState({
              sinkTableDataSource: tableData,
              sinkJsonSampleValue: jsonSample
            }, () => {
              const tempArr = this.state.sinkTableDataSource.filter(s => !s.forbidden)
              const selectedArr = tempArr.filter(s => s.selected)

              let tempState = ''
              if (selectedArr.length !== 0) {
                tempState = selectedArr.length === tempArr.length ? 'all' : 'part'
              } else {
                tempState = 'not'
              }
              this.setState({ sinkSelectAllState: tempState })
            })
          }
        } else {
          this.makeSinkCodeMirrorInstance()
          this.cmSinkSample.doc.setValue('')
        }
      })
    })
  }

  initChangeUmsType = (value) => {
    this.setState({
      umsTypeSeleted: value
    }, () => {
      if (this.state.umsTypeSeleted === 'ums_extension') {
        this.makeCodeMirrorInstance()
        this.cmSample.doc.setValue(this.cmSample.doc.getValue() || '')
      }
    })
  }

  cmIsDisabled () {
    const { namespaceClassHide, roleType } = this.props
    let isDisabled = ''
    if (roleType === 'admin') {
      isDisabled = namespaceClassHide === 'hide'
    } else if (roleType === 'user') {
      isDisabled = true
    }
    return isDisabled
  }

  makeCodeMirrorInstance = () => {
    if (!this.cmSample) {
      const temp = document.getElementById('jsonSampleTextarea')

      this.cmSample = CodeMirror.fromTextArea(temp, {
        lineNumbers: true,
        matchBrackets: true,
        autoCloseBrackets: true,
        mode: 'application/ld+json',
        lineWrapping: true,
        readOnly: this.cmIsDisabled(),
        cursorBlinkRate: this.cmIsDisabled() ? -1 : 0 // 光标去掉
      })
      this.cmSample.setSize('100%', '528.8px')
    }
  }

  makeSinkCodeMirrorInstance = () => {
    if (!this.cmSinkSample) {
      const temp = document.getElementById('sinkJsonSampleTextarea')

      this.cmSinkSample = CodeMirror.fromTextArea(temp, {
        lineNumbers: true,
        matchBrackets: true,
        autoCloseBrackets: true,
        mode: 'application/ld+json',
        lineWrapping: true,
        readOnly: this.cmIsDisabled(),
        cursorBlinkRate: this.cmIsDisabled() ? -1 : 0 // 光标去掉
      })
      this.cmSinkSample.setSize('100%', '528.8px')
    }
  }

  hideSchemaModal = () => {
    this.setState({
      schemaModalVisible: false
    }, () => {
      this.setState({
        umsTableDataSource: [],
        umsTypeSeleted: 'ums',
        umsopRecordValue: -1,
        repeatRenameArr: [],
        umsopable: false,
        umsopKey: -1
      })
      this.schemaTypeConfig.resetFields()
    })
  }

  hideSinkSchemaModal = () => {
    this.setState({
      sinkSchemaModalVisible: false
    }, () => {
      this.setState({ sinkTableDataSource: [] })
    })
  }

  initSelectUmsop = (key) => this.setState({ umsopRecordValue: key })

  onSchemaModalOk = () => {
    const { locale } = this.props
    const typeFailText = locale === 'en' ? 'Tuple type configuration has error!' : 'Tuple 类型配置失败！'
    const successText = locale === 'en' ? 'Source Schema is configured successfully!' : 'Source Schema 配置成功！'
    const jsonText = locale === 'en' ? 'JSON Sample is not null!' : 'JSON Sample 不为空！'
    const tableText = locale === 'en' ? 'Table cannot be empty!' : 'Table 不为空！'
    const renameText = locale === 'en' ? 'Rename cannot be empty!' : 'Rename 不为空！'
    const renameRepeatText = locale === 'en' ? 'Please modify duplicated items of Rename column!' : '请修改 Rename 重复项！'
    const arrayTypeText = locale === 'en' ? 'Only one array type contained!' : '只能包含一个array 类型！'
    const umsTsText = locale === 'en' ? 'Please select ums_ts!' : '请选择 ums_ts！'
    const umsOpText = locale === 'en' ? 'ums_op_ configuration has error!' : 'ums_op_ 配置错误！'
    if (document.getElementById('sep')) {
      message.error(typeFailText, 3)
      return
    }
    this.schemaTypeConfig.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const { nsIdValue } = this.state
        switch (values.umsType) {
          case 'ums':
            const requestValue = {umsType: 'ums'}
            this.props.onSetSchema(nsIdValue, requestValue, 'source', () => {
              message.success(successText, 3)
              this.hideSchemaModal()
            })
            break
          case 'ums_extension':
            const { jsonSampleValue, umsTableDataSource } = this.state

            const jsontemp = this.cmSample.doc.getValue()
            if (!jsontemp) {
              message.error(jsonText, 3)
            } else {
              if (umsTableDataSource.length === 0) {
                message.error(tableText, 3)
              } else {
                const spaceRename = umsTableDataSource.find(s => !s.rename)
                // 除去selected的项，检查rename字段是否有重复, 提示rename重复的位置，数组中的值为rename重复的index
                const repeatArr = getRepeatFieldIndex(umsTableDataSource)

                // fieldType只能包含一个array类型
                const repeatTypeArr = umsTableDataSource.filter(s => s.fieldType.includes('array'))
                const repeatArrayIndex = repeatTypeArr.map(s => s.key)

                if (spaceRename) {
                  message.error(renameText, 3)
                } else if (repeatArr.length !== 0) {
                  message.error(renameRepeatText, 3)
                  this.setState({ repeatRenameArr: repeatArr })
                } else if (repeatTypeArr.length > 1) {
                  message.error(arrayTypeText, 3)
                  this.setState({ repeatArrayArr: repeatArrayIndex })
                } else {
                  this.setState({
                    repeatRenameArr: [],
                    repeatArrayArr: []
                  })

                  // 检查ums_ts_，分别必须得有一个
                  const umsTsExit = umsTableDataSource.find(i => i.ums_ts_ === true)
                  if (!umsTsExit) {
                    message.error(umsTsText, 3)
                  } else {
                    if (document.getElementById('insert')) {
                      const opInsert = document.getElementById('insert').value
                      const opUpdate = document.getElementById('update').value
                      const opDelete = document.getElementById('delete').value

                      if (opInsert && opUpdate && opDelete) {
                        const { umsTableDataSource, umsopRecordValue } = this.state
                        const originUmsop = umsTableDataSource.find(s => s.ums_op_ !== '')

                        const umsopKeyTemp = umsopRecordValue === -1 ? originUmsop.key : umsopRecordValue

                        const textVal = `i:${opInsert},u:${opUpdate},d:${opDelete}`
                        const tempArr = umsSysFieldSelected(umsTableDataSource, umsopKeyTemp, 'ums_op_', textVal)
                        this.setState({
                          umsTableDataSource: tempArr
                        }, () => {
                          const { umsTableDataSource } = this.state
                          const tableDataString = JSON.stringify(umsTableDataSource, ['selected', 'fieldName', 'rename', 'fieldType', 'ums_id_', 'ums_ts_', 'ums_op_', 'forbidden'])
                          const tableDataStringTemp = JSON.stringify(umsTableDataSource, ['key', 'selected', 'fieldName', 'rename', 'fieldType', 'ums_id_', 'ums_ts_', 'ums_op_', 'forbidden'])

                          const requestValue = {
                            umsType: 'ums_extension',
                            jsonSample: this.cmSample.doc.getValue(),
                            jsonParseArray: jsonSampleValue,
                            umsSchemaTable: JSON.parse(tableDataString),
                            umsSchema: genSchema(JSON.parse(tableDataStringTemp), 'source') // 生成 umsSchema json
                          }

                          this.props.onSetSchema(nsIdValue, requestValue, 'source', () => {
                            message.success(successText, 3)
                            this.hideSchemaModal()
                          })
                        })
                      } else {
                        message.error(umsOpText, 3)
                      }
                    } else {
                      const umsArr = umsSysFieldCanceled(this.state.umsTableDataSource, 'ums_op_')
                      this.setState({
                        umsTableDataSource: umsArr
                      }, () => {
                        const { umsTableDataSource } = this.state
                        const tableDataString = JSON.stringify(umsTableDataSource, ['selected', 'fieldName', 'rename', 'fieldType', 'ums_id_', 'ums_ts_', 'ums_op_', 'forbidden'])
                        const tableDataStringTemp = JSON.stringify(umsTableDataSource, ['key', 'selected', 'fieldName', 'rename', 'fieldType', 'ums_id_', 'ums_ts_', 'ums_op_', 'forbidden'])

                        const requestValue = {
                          umsType: 'ums_extension',
                          jsonSample: this.cmSample.doc.getValue(),
                          jsonParseArray: jsonSampleValue,
                          umsSchemaTable: JSON.parse(tableDataString),
                          umsSchema: genSchema(JSON.parse(tableDataStringTemp), 'source') // 生成 umsSchema json
                        }

                        this.props.onSetSchema(nsIdValue, requestValue, 'source', () => {
                          message.success(successText, 3)
                          this.hideSchemaModal()
                        })
                      })
                    }
                  }
                }
              }
            }
            break
        }
      }
    })
  }

  onSinkSchemaModalOk = () => {
    const { sinkTableDataSource, nsIdValue, sinkJsonSampleValue } = this.state
    const { locale } = this.props
    const successText = locale === 'en' ? 'Sink Schema is configured successfully!' : 'Sink Schema 配置成功！'
    const tableText = locale === 'en' ? 'Table cannot be empty!' : 'Table 不为空！'

    if (!this.cmSinkSample.doc.getValue()) {
      this.props.onSetSchema(nsIdValue, {}, 'sink', () => {
        message.success(successText, 3)
        this.hideSinkSchemaModal()
      })
    } else {
      if (sinkTableDataSource.length === 0) {
        message.error(tableText, 3)
      } else {
        const tableDataString = JSON.stringify(sinkTableDataSource, ['selected', 'fieldName', 'fieldType', 'forbidden'])

        // 调用genSchema函数后，不改变原this.stata.sinkTableDataSource
        const tableDataStringTemp = JSON.stringify(sinkTableDataSource, ['selected', 'fieldName', 'fieldType', 'forbidden', 'key'])

        const requestValue = {
          jsonSample: this.cmSinkSample.doc.getValue(),
          jsonParseArray: sinkJsonSampleValue,
          schemaTable: JSON.parse(tableDataString),
          schema: genSchema(JSON.parse(tableDataStringTemp), 'sink') // 生成 Schema json
        }

        this.props.onSetSchema(nsIdValue, requestValue, 'sink', () => {
          message.success(successText, 3)
          this.hideSinkSchemaModal()
        })
      }
    }
  }

  onJsonFormat = () => {
    const cmJsonvalue = this.cmSample.doc.getValue()
    const { locale } = this.props
    const jsonText = locale === 'en' ? 'JSON Sample is not null!' : 'JSON Sample 不为空！'
    const noJsonText = locale === 'en' ? 'Not JSON format!' : '非 JSON格式！'

    if (cmJsonvalue === '') {
      message.error(jsonText, 3)
    } else if (!isJSONNotEmpty(cmJsonvalue)) {
      message.error(noJsonText, 3)
    } else {
      const cmJsonvalueFormat = JSON.stringify(JSON.parse(cmJsonvalue), null, 3)
      this.cmSample.doc.setValue(cmJsonvalueFormat || '')
    }
  }

  onSinkJsonFormat = () => {
    const cmJsonvalue = this.cmSinkSample.doc.getValue()
    const { locale } = this.props
    const jsonText = locale === 'en' ? 'JSON Sample is not null!' : 'JSON Sample 不为空！'
    const noJsonText = locale === 'en' ? 'Not JSON format!' : '非 JSON格式！'

    if (cmJsonvalue === '') {
      message.error(jsonText, 3)
    } else if (!isJSONNotEmpty(cmJsonvalue)) {
      message.error(noJsonText, 3)
    } else {
      const cmJsonvalueFormat = JSON.stringify(JSON.parse(cmJsonvalue), null, 3)
      this.cmSinkSample.doc.setValue(cmJsonvalueFormat || '')
    }
  }

  onSinkNoJson = () => {
    this.cmSinkSample.doc.setValue('')
    this.setState({ sinkTableDataSource: [] })
  }

  getUmSopable = (umsopable, umsopKey) => {
    this.setState({umsopable, umsopKey})
  }
  onChangeUmsJsonToTable = () => {
    const cmVal = this.cmSample.doc.getValue()
    const { locale } = this.props
    const jsonText = locale === 'en' ? 'Please fill in JSON Sample' : '请填写 JSON Sample'
    const noJsonText = locale === 'en' ? 'Not JSON format!' : '非 JSON格式！'

    if (cmVal === '') {
      message.error(jsonText, 3)
    } else if (!isJSONNotEmpty(cmVal)) {
      message.error(noJsonText, 3)
    } else {
      const cmJsonvalue = JSON.parse(this.cmSample.doc.getValue())
      const jsonSmaple = jsonParse(cmJsonvalue, '', [])

      const tableArray = genDefaultSchemaTable(jsonSmaple, 'source')

      this.setState({
        jsonSampleValue: jsonSmaple,
        umsTableDataSource: tableArray.map((s, index) => {
          s.key = index
          return s
        }),
        umsopable: false,
        umsopKey: -1
      })
    }
  }

  onChangeSinkJsonToTable = () => {
    const cmVal = this.cmSinkSample.doc.getValue()
    const { locale } = this.props
    const jsonText = locale === 'en' ? 'Please fill in the JSON Sample' : '请填写 JSON Sample'
    const noJsonText = locale === 'en' ? 'Not JSON format!' : '非 JSON格式！'

    if (cmVal === '') {
      message.error(jsonText, 3)
    } else if (!isJSONNotEmpty(cmVal)) {
      message.error(noJsonText, 3)
    } else {
      const cmJsonvalue = JSON.parse(this.cmSinkSample.doc.getValue())
      const jsonSmaple = jsonParse(cmJsonvalue, '', [])

      const tableArray = genDefaultSchemaTable(jsonSmaple, 'sink')

      this.setState({
        sinkJsonSampleValue: jsonSmaple,
        sinkTableDataSource: tableArray.map((s, index) => {
          s.key = index
          return s
        })
      })
    }
  }

  initChangeSelected = (record) => {
    const { umsTableDataSource } = this.state

    const tempData = umsTableDataSource.map(s => {
      const temp = s.key === record.key
        ? {
          fieldName: s.fieldName,
          fieldType: s.fieldType,
          forbidden: s.forbidden,
          key: s.key,
          rename: s.rename,
          selected: !s.selected,
          ums_id_: s.ums_id_,
          ums_op_: s.ums_op_,
          ums_ts_: s.ums_ts_
        }
        : s
      return temp
    })
    this.setState({
      umsTableDataSource: tempData
    }, () => {
      const exceptForbidden = this.state.umsTableDataSource.filter(s => !s.forbidden)
      const exceptSelect = exceptForbidden.filter(s => s.selected)
      if (exceptSelect) {
        this.setState({ selectAllState: exceptSelect.length === exceptForbidden.length ? 'all' : 'part' })
      } else {
        this.setState({ selectAllState: 'no' })
      }
    })
  }

  initSinkChangeSelected = (record) => {
    const { sinkTableDataSource } = this.state

    const tempData = sinkTableDataSource.map(s => {
      const temp = s.key === record.key
        ? {
          fieldName: s.fieldName,
          fieldType: s.fieldType,
          forbidden: s.forbidden,
          key: s.key,
          selected: !s.selected
        }
        : s
      return temp
    })
    this.setState({
      sinkTableDataSource: tempData
    }, () => {
      const exceptForbidden = sinkTableDataSource.filter(s => !s.forbidden)
      const exceptSelect = exceptForbidden.filter(s => s.selected)
      if (exceptSelect) {
        this.setState({ sinkSelectAllState: exceptSelect.length === exceptForbidden.length ? 'all' : 'part' })
      } else {
        this.setState({ sinkSelectAllState: 'no' })
      }
    })
  }

  initRowSelectedAll = () => {
    const { umsTableDataSource, selectAllState } = this.state

    let temp = ''
    if (selectAllState === 'all') {
      temp = 'not'
    } else if (selectAllState === 'not') {
      temp = 'all'
    } else if (selectAllState === 'part') {
      temp = 'all'
    }

    this.setState({
      selectAllState: temp
    }, () => {
      let tempArr = []
      switch (this.state.selectAllState) {
        case 'all':
          tempArr = umsTableDataSource.map(s => {
            let tempObj = {}
            if (!s.forbidden) {
              tempObj = !s.selected
                ? {
                  fieldName: s.fieldName,
                  fieldType: s.fieldType,
                  forbidden: s.forbidden,
                  key: s.key,
                  rename: s.rename,
                  selected: true,
                  ums_id_: s.ums_id_,
                  ums_op_: s.ums_op_,
                  ums_ts_: s.ums_ts_
                }
                : s
            } else {
              tempObj = s
            }
            return tempObj
          })
          break
        case 'not':
          tempArr = umsTableDataSource.map(s => {
            let tempObj = {}
            if (!s.forbidden) {
              tempObj = s.selected
                ? {
                  fieldName: s.fieldName,
                  fieldType: s.fieldType,
                  forbidden: s.forbidden,
                  key: s.key,
                  rename: s.rename,
                  selected: false,
                  ums_id_: s.ums_id_,
                  ums_op_: s.ums_op_,
                  ums_ts_: s.ums_ts_
                }
                : s
            } else {
              tempObj = s
            }
            return tempObj
          })
          break
        case 'part':
          tempArr = umsTableDataSource
          break
      }

      this.setState({ umsTableDataSource: tempArr })
    })
  }

  initSinkRowSelectedAll = () => {
    const { sinkTableDataSource, sinkSelectAllState } = this.state

    let temp = ''
    if (sinkSelectAllState === 'all') {
      temp = 'not'
    } else if (sinkSelectAllState === 'not') {
      temp = 'all'
    } else if (sinkSelectAllState === 'part') {
      temp = 'all'
    }

    this.setState({
      sinkSelectAllState: temp
    }, () => {
      let tempArr = []
      switch (this.state.sinkSelectAllState) {
        case 'all':
          tempArr = sinkTableDataSource.map(s => {
            let tempObj = {}
            if (!s.forbidden) {
              tempObj = !s.selected
                ? {
                  fieldName: s.fieldName,
                  fieldType: s.fieldType,
                  forbidden: s.forbidden,
                  key: s.key,
                  selected: true
                }
                : s
            } else {
              tempObj = s
            }
            return tempObj
          })
          break
        case 'not':
          tempArr = sinkTableDataSource.map(s => {
            let tempObj = {}
            if (!s.forbidden) {
              tempObj = s.selected
                ? {
                  fieldName: s.fieldName,
                  fieldType: s.fieldType,
                  forbidden: s.forbidden,
                  key: s.key,
                  selected: false
                }
                : s
            } else {
              tempObj = s
            }
            return tempObj
          })
          break
        case 'part':
          tempArr = sinkTableDataSource
          break
      }

      this.setState({
        sinkTableDataSource: tempArr
      })
    })
  }

  umsFieldTypeSelectOk = (recordKey, selectTypeVal) => {
    const { umsTableDataSource } = this.state

    const umsArr = fieldTypeAlter(umsTableDataSource, recordKey, selectTypeVal, 'source')
    this.setState({ umsTableDataSource: umsArr })
  }

  initUmsopOther2Tuple = (record, delimiterValue, sizeValue, tupleForm) => {
    const { umsTableDataSource } = this.state

    const textVal = `tuple##${delimiterValue}##${sizeValue}`
    const tempArr = fieldTypeAlter(umsTableDataSource, record.key, textVal, 'source')
    this.setState({ umsTableDataSource: tempArr })
  }

  initEditRename = (recordKey, value) => {
    const { umsTableDataSource } = this.state

    const umsArr = renameAlter(umsTableDataSource, recordKey, value)
    this.setState({ umsTableDataSource: umsArr })
  }

  initSelectUmsIdTs = (record, umsSysField) => {
    const { umsTableDataSource } = this.state

    const tempArr = (umsSysField === 'ums_id_' && record[umsSysField])
      ? umsSysFieldCanceled(umsTableDataSource, 'ums_id_')
      : umsSysFieldSelected(umsTableDataSource, record.key, umsSysField, true)

    this.setState({ umsTableDataSource: tempArr })
  }

  handleVisibleChangeNs = (record) => (visible) => {
    if (visible) {
      this.setState({
        visible
      }, () => {
        this.props.onLoadSingleNamespace(record.id, (result) => this.setState({ showNsDetail: result }))
      })
    }
  }

  initChangeSinkType = (index, afterType) => {
    const { sinkTableDataSource } = this.state

    const arr = fieldTypeAlter(sinkTableDataSource, index, afterType, 'sink')
    this.setState({ sinkTableDataSource: arr })
  }

  deleteNsBtn = (record) => (e) => {
    this.props.onDeleteNs(record.id, () => {
      message.success(operateLanguageText('success', 'delete'), 3)
    }, (result) => {
      message.error(`${operateLanguageText('fail', 'delete')} ${result}`, 5)
    })
  }

  render () {
    const {
      count, formVisible, schemaModalVisible, sinkSchemaModalVisible, currentNamespaces,
      refreshNsLoading, refreshNsText, showNsDetail, namespaceFormType, queryConnUrl,
      databaseSelectValue, namespaceTableSource, deleteTableClass, addTableClass, addTableClassTable, addBtnDisabled
    } = this.state
    const { namespaceClassHide, roleType } = this.props

    let { sortedInfo, filteredInfo } = this.state
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const columns = [
      {
        title: 'Data System',
        dataIndex: 'nsSys',
        key: 'nsSys',
        sorter: (a, b) => a.nsSys < b.nsSys ? -1 : 1,
        sortOrder: sortedInfo.columnKey === 'nsSys' && sortedInfo.order,
        filters: filterDataSystemData(),
        filteredValue: filteredInfo.nsSys,
        onFilter: (value, record) => record.nsSys.includes(value)
      }, {
        title: 'Instance',
        dataIndex: 'nsInstance',
        key: 'nsInstance',
        sorter: (a, b) => {
          if (typeof a.nsInstance === 'object') {
            return a.nsInstanceOrigin < b.nsInstanceOrigin ? -1 : 1
          } else {
            return a.nsInstance < b.nsInstance ? -1 : 1
          }
        },
        sortOrder: sortedInfo.columnKey === 'nsInstance' && sortedInfo.order,
        filterDropdown: (
          <div className="custom-filter-dropdown">
            <Input
              ref={ele => { this.searchInput = ele }}
              placeholder="Instance"
              value={this.state.searchNsInstance}
              onChange={this.onInputChange('searchNsInstance')}
              onPressEnter={this.onSearch('nsInstance', 'searchNsInstance', 'filterDropdownVisibleNsInstance')}
            />
            <Button type="primary" onClick={this.onSearch('nsInstance', 'searchNsInstance', 'filterDropdownVisibleNsInstance')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleNsInstance,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisibleNsInstance: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Database',
        dataIndex: 'nsDatabase',
        key: 'nsDatabase',
        sorter: (a, b) => {
          if (typeof a.nsDatabase === 'object') {
            return a.nsDatabaseOrigin < b.nsDatabaseOrigin ? -1 : 1
          } else {
            return a.nsDatabase < b.nsDatabase ? -1 : 1
          }
        },
        sortOrder: sortedInfo.columnKey === 'nsDatabase' && sortedInfo.order,
        filterDropdown: (
          <div className="custom-filter-dropdown">
            <Input
              ref={ele => { this.searchInput = ele }}
              placeholder="Database"
              value={this.state.searchNsDatabase}
              onChange={this.onInputChange('searchNsDatabase')}
              onPressEnter={this.onSearch('nsDatabase', 'searchNsDatabase', 'filterDropdownVisibleNsDatabase')}
            />
            <Button type="primary" onClick={this.onSearch('nsDatabase', 'searchNsDatabase', 'filterDropdownVisibleNsDatabase')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleNsDatabase,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisibleNsDatabase: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Table',
        dataIndex: 'nsTable',
        key: 'nsTable',
        sorter: (a, b) => {
          if (typeof a.nsTable === 'object') {
            return a.nsTableOrigin < b.nsTableOrigin ? -1 : 1
          } else {
            return a.nsTable < b.nsTable ? -1 : 1
          }
        },
        sortOrder: sortedInfo.columnKey === 'nsTable' && sortedInfo.order,
        filterDropdown: (
          <div className="custom-filter-dropdown">
            <Input
              ref={ele => { this.searchInput = ele }}
              placeholder="Table"
              value={this.state.searchNsTable}
              onChange={this.onInputChange('searchNsTable')}
              onPressEnter={this.onSearch('nsTable', 'searchNsTable', 'filterDropdownVisibleNsTable')}
            />
            <Button type="primary" onClick={this.onSearch('nsTable', 'searchNsTable', 'filterDropdownVisibleNsTable')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleNsTable,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisibleNsTable: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Key',
        dataIndex: 'keys',
        key: 'keys',
        sorter: (a, b) => {
          if (typeof a.keys === 'object') {
            return a.keysOrigin < b.keysOrigin ? -1 : 1
          } else {
            return a.keys < b.keys ? -1 : 1
          }
        },
        sortOrder: sortedInfo.columnKey === 'keys' && sortedInfo.order,
        filterDropdown: (
          <div className="custom-filter-dropdown">
            <Input
              ref={ele => { this.searchInput = ele }}
              placeholder="Key"
              value={this.state.searchNsKey}
              onChange={this.onInputChange('searchNsKey')}
              onPressEnter={this.onSearch('keys', 'searchNsKey', 'filterDropdownVisibleNsKey')}
            />
            <Button type="primary" onClick={this.onSearch('keys', 'searchNsKey', 'filterDropdownVisibleNsKey')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleNsKey,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisibleNsKey: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Topic',
        dataIndex: 'topic',
        key: 'topic',
        sorter: (a, b) => {
          if (typeof a.topic === 'object') {
            return a.topicOrigin < b.topicOrigin ? -1 : 1
          } else {
            return a.topic < b.topic ? -1 : 1
          }
        },
        sortOrder: sortedInfo.columnKey === 'topic' && sortedInfo.order,
        filterDropdown: (
          <div className="custom-filter-dropdown">
            <Input
              ref={ele => { this.searchInput = ele }}
              placeholder="Topic"
              value={this.state.searchNstopic}
              onChange={this.onInputChange('searchNstopic')}
              onPressEnter={this.onSearch('topic', 'searchNstopic', 'filterDropdownVisibleNsTopic')}
            />
            <Button type="primary" onClick={this.onSearch('topic', 'searchNstopic', 'filterDropdownVisibleNsTopic')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleNsTopic,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisibleNsTopic: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Create Time',
        dataIndex: 'createTime',
        key: 'createTime',
        sorter: (a, b) => {
          if (typeof a.createTime === 'object') {
            return a.createTimeOrigin < b.createTimeOrigin ? -1 : 1
          } else {
            return a.createTime < b.createTime ? -1 : 1
          }
        },
        sortOrder: sortedInfo.columnKey === 'createTime' && sortedInfo.order,
        // filteredValue: filteredInfo.createTime,
        filterDropdown: (
          <div className="custom-filter-dropdown-style">
            <RangePicker
              showTime
              format="YYYY-MM-DD HH:mm:ss"
              placeholder={['Start', 'End']}
              onOpenChange={this.handleEndOpenChange}
              onChange={this.onRangeTimeChange}
              onPressEnter={this.onRangeTimeSearch('createTime', 'createStartTimeText', 'createEndTimeText', 'filterDropdownVisibleCreateTime')}
            />
            <Button type="primary" className="rangeFilter" onClick={this.onRangeTimeSearch('createTime', 'createStartTimeText', 'createEndTimeText', 'filterDropdownVisibleCreateTime')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleCreateTime,
        onFilterDropdownVisibleChange: visible => {
          if (!this.state.filterDatepickerShown) {
            this.setState({ filterDropdownVisibleCreateTime: visible })
          }
        }
      }, {
        title: 'Update Time',
        dataIndex: 'updateTime',
        key: 'updateTime',
        sorter: (a, b) => {
          if (typeof a.updateTime === 'object') {
            return a.updateTimeOrigin < b.updateTimeOrigin ? -1 : 1
          } else {
            return a.updateTime < b.updateTime ? -1 : 1
          }
        },
        sortOrder: sortedInfo.columnKey === 'updateTime' && sortedInfo.order,
        // filteredValue: filteredInfo.updateTime,
        filterDropdown: (
          <div className="custom-filter-dropdown-style">
            <RangePicker
              showTime
              format="YYYY-MM-DD HH:mm:ss"
              placeholder={['Start', 'End']}
              onOpenChange={this.handleEndOpenChange}
              onChange={this.onRangeTimeChange}
              onPressEnter={this.onRangeTimeSearch('updateTime', 'updateStartTimeText', 'updateEndTimeText', 'filterDropdownVisibleUpdateTime')}
            />
            <Button type="primary" className="rangeFilter" onClick={this.onRangeTimeSearch('updateTime', 'updateStartTimeText', 'createEndTimeText', 'filterDropdownVisibleUpdateTime')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleUpdateTime,
        onFilterDropdownVisibleChange: visible => {
          if (!this.state.filterDatepickerShown) {
            this.setState({ filterDropdownVisibleUpdateTime: visible })
          }
        }
      }, {
        title: 'Action',
        key: 'action',
        className: `text-align-center`,
        render: (text, record) => {
          let umsAction = ''
          if (record.nsSys === 'kafka') {
            umsAction = (
              <span className="ant-table-action-column">
                <Tooltip title="Source Schema">
                  <Button shape="circle" type="ghost" onClick={this.showEditUms(record)}>
                    <i className="iconfont icon-icos"></i>
                  </Button>
                </Tooltip>
                <Tooltip title="Sink Schema">
                  <Button shape="circle" type="ghost" onClick={this.showEditSink(record)}>
                    <i className="iconfont icon-ic_Heatsink"></i>
                  </Button>
                </Tooltip>
              </span>
            )
          } else if (record.nsSys === 'es' || record.nsSys === 'mongodb') {
            umsAction = (
              <Tooltip title="Sink Schema">
                <Button shape="circle" type="ghost" onClick={this.showEditSink(record)}>
                  <i className="iconfont icon-ic_Heatsink"></i>
                </Button>
              </Tooltip>
            )
          } else {
            umsAction = ''
          }

          const nsAction = (
            <span className="ant-table-action-column">
              <Tooltip title={<FormattedMessage {...messages.nsTableViewDetails} />}>
                <Popover
                  placement="left"
                  content={
                    <div className="project-name-detail">
                      <p><strong>   Project Names：</strong>{showNsDetail.projectName}</p>
                    </div>}
                  title={<h3><FormattedMessage {...messages.nsTableDetails} /></h3>}
                  trigger="click"
                  onVisibleChange={this.handleVisibleChangeNs(record)}
                >
                  <Button icon="file-text" shape="circle" type="ghost"></Button>
                </Popover>
              </Tooltip>

              <Tooltip title={<FormattedMessage {...messages.nsTableModifyAction} />}>
                <Button icon="edit" shape="circle" type="ghost" onClick={this.showEditNamespace(record)}></Button>
              </Tooltip>

              {umsAction}

              <Popconfirm placement="bottom" title={<FormattedMessage {...messages.nsModalSureDeleteTable} />} okText="Yes" cancelText="No" onConfirm={this.deleteNsBtn(record)}>
                <Tooltip title={<FormattedMessage {...messages.nsModalDeleteTable} />}>
                  <Button icon="delete" shape="circle" type="ghost"></Button>
                </Tooltip>
              </Popconfirm>
            </span>
          )

          let actionHtml = ''
          if (roleType === 'admin') {
            actionHtml = namespaceClassHide === 'hide' ? umsAction : nsAction
          } else if (roleType === 'user') {
            actionHtml = umsAction
          }
          return actionHtml
        }
      }]

    const pagination = {
      defaultPageSize: this.state.pageSize,
      showSizeChanger: true,
      onChange: (current) => console.log('current', current)
    }

    const helmetHide = namespaceClassHide !== 'hide'
      ? (<Helmet title="Namespace" />)
      : (<Helmet title="Workbench" />)

    let sourceFooter = null
    let sinkFooter = null
    if (roleType === 'admin') {
      sourceFooter = namespaceClassHide === 'hide'
        ? null
        : [
          <Button
            key="jsonFormat"
            type="primary"
            className={`json-format ${this.state.umsTypeSeleted === 'ums' ? 'hide' : ''}`}
            onClick={this.onJsonFormat}
          >
            <FormattedMessage {...messages.nsTableJsonFormat} />
          </Button>, <Button
            key="cancel"
            size="large"
            onClick={this.hideSchemaModal}
          >
            <FormattedMessage {...messages.nsModalCancel} />
          </Button>, <Button
            key="submit"
            size="large"
            type="primary"
            onClick={this.onSchemaModalOk}
          >
            <FormattedMessage {...messages.nsModalSave} />
          </Button>
        ]

      sinkFooter = namespaceClassHide === 'hide'
        ? null
        : [
          <Button
            key="jsonFormat"
            type="primary"
            onClick={this.onSinkJsonFormat}
          >
            <FormattedMessage {...messages.nsTableJsonFormat} />
          </Button>, <Button
            key="noJson"
            type="primary"
            onClick={this.onSinkNoJson}
          >
            <FormattedMessage {...messages.nsTableJsonClear} />
          </Button>, <Button
            key="cancel"
            size="large"
            onClick={this.hideSinkSchemaModal}
          >
            <FormattedMessage {...messages.nsModalCancel} />
          </Button>, <Button
            key="submit"
            size="large"
            type="primary"
            onClick={this.onSinkSchemaModalOk}
          >
            <FormattedMessage {...messages.nsModalSave} />
          </Button>
        ]
    } else if (roleType === 'user') {
      sinkFooter = null
      sourceFooter = null
    }

    const modalTitle = this.state.namespaceFormType === 'add'
      ? <FormattedMessage {...messages.nsTableCreate} />
      : <FormattedMessage {...messages.nsTableModify} />

    return (
      <div>
        {helmetHide}
        <div className="ri-workbench-table ri-common-block">
          <h3 className="ri-common-block-title">
            <Icon type="bars" /> Namespace <FormattedMessage {...messages.nsTableTitle} />
          </h3>
          <div className="ri-common-block-tools">
            <Button icon="plus" type="primary" className={namespaceClassHide} onClick={this.showAddNamespace}>
              <FormattedMessage {...messages.nsTableCreateBtn} />
            </Button>
            <Button icon="poweroff" type="ghost" className="refresh-button-style" loading={refreshNsLoading} onClick={this.refreshNamespace}>{refreshNsText}</Button>
          </div>
          <Table
            dataSource={currentNamespaces || []}
            columns={columns}
            onChange={this.handleNamespaceChange}
            pagination={pagination}
            className="ri-workbench-table-container"
            bordered>
          </Table>
        </div>
        <Modal
          title={modalTitle}
          okText="保存"
          wrapClassName="db-form-style"
          visible={formVisible}
          onCancel={this.hideForm}
          afterClose={this.resetModal}
          footer={[
            <Button
              key="cancel"
              size="large"
              type="ghost"
              onClick={this.hideForm}
            >
              <FormattedMessage {...messages.nsModalCancel} />
            </Button>,
            <Button
              key="submit"
              size="large"
              type="primary"
              loading={this.props.modalLoading}
              onClick={this.onModalOk}
            >
              <FormattedMessage {...messages.nsModalSave} />
            </Button>
          ]}
        >
          <NamespaceForm
            namespaceFormType={namespaceFormType}
            queryConnUrl={queryConnUrl}
            onInitNamespaceUrlValue={this.onInitNamespaceUrlValue}
            namespaceUrlValue={this.props.dbUrlValue}
            onInitDatabaseSelectValue={this.onInitDatabaseSelectValue}
            onInitNsNameInputValue={this.onInitNsNameInputValue}
            onInitNsKeyInputValue={this.onInitNsKeyInputValue}
            databaseSelectValue={databaseSelectValue}
            namespaceTableSource={namespaceTableSource}
            deleteTableClass={deleteTableClass}
            addTableClass={addTableClass}
            addTableClassTable={addTableClassTable}
            addBtnDisabled={addBtnDisabled}
            onAddTable={this.onAddTable}
            onDeleteTable={this.onDeleteTable}
            cleanNsTableData={this.cleanNsTableData}
            count={count}
            ref={(f) => { this.namespaceForm = f }}
          />
        </Modal>
        {/* Source Schema Config Modal */}
        <Modal
          title="Source Schema Config"
          okText="保存"
          wrapClassName="schema-config-modal ums-modal"
          visible={schemaModalVisible}
          onCancel={this.hideSchemaModal} // "X" 按钮
          footer={sourceFooter}
        >
          <SchemaTypeConfig
            umsFieldTypeSelectOk={this.umsFieldTypeSelectOk}
            initChangeSelected={this.initChangeSelected}
            initChangeUmsType={this.initChangeUmsType}
            onChangeJsonToTable={this.onChangeUmsJsonToTable}
            emitUmSopable={this.getUmSopable}
            initEditRename={this.initEditRename}
            initSelectUmsIdTs={this.initSelectUmsIdTs}
            initUmsopOther2Tuple={this.initUmsopOther2Tuple}
            initSelectUmsop={this.initSelectUmsop}
            cancelSelectUmsId={this.cancelSelectUmsId}
            initCheckUmsOp={this.initCheckUmsOp}
            initCancelUmsOp={this.initCancelUmsOp}
            initRowSelectedAll={this.initRowSelectedAll}
            umsTableDataSource={this.state.umsTableDataSource}
            beforesepratorValue={this.state.beforesepratorValue}
            umsTypeSeleted={this.state.umsTypeSeleted}
            repeatRenameArr={this.state.repeatRenameArr}
            repeatArrayArr={this.state.repeatArrayArr}
            selectAllState={this.state.selectAllState}
            umsopable={this.state.umsopable}
            umsopKey={this.state.umsopKey}
            namespaceClassHide={namespaceClassHide}
            ref={(f) => { this.schemaTypeConfig = f }}
          />
        </Modal>
        {/* Sink Schema Config Modal */}
        <Modal
          title="Sink Schema Config"
          okText="保存"
          wrapClassName="schema-config-modal ums-modal"
          visible={sinkSchemaModalVisible}
          onCancel={this.hideSinkSchemaModal}
          footer={sinkFooter}
        >
          <SinkSchemaTypeConfig
            sinkTableDataSource={this.state.sinkTableDataSource}
            sinkSelectAllState={this.state.sinkSelectAllState}
            initSinkRowSelectedAll={this.initSinkRowSelectedAll}
            initSinkChangeSelected={this.initSinkChangeSelected}
            onChangeSinkJsonToTable={this.onChangeSinkJsonToTable}
            initChangeSinkType={this.initChangeSinkType}
            namespaceClassHide={namespaceClassHide}
            ref={(f) => { this.sinkSchemaTypeConfig = f }}
          />
        </Modal>
      </div>
    )
  }
}

Namespace.propTypes = {
  modalLoading: PropTypes.bool,
  tableNameExited: PropTypes.bool,
  dbUrlValue: PropTypes.oneOfType([
    PropTypes.bool,
    PropTypes.array
  ]),
  onLoadAdminAllNamespaces: PropTypes.func,
  onLoadUserNamespaces: PropTypes.func,
  projectIdGeted: PropTypes.string,
  namespaceClassHide: PropTypes.string,
  onLoadSelectNamespaces: PropTypes.func,
  onLoadDatabasesInstance: PropTypes.func,
  onLoadNamespaceDatabase: PropTypes.func,
  onLoadTableNameExist: PropTypes.func,
  onAddNamespace: PropTypes.func,
  onEditNamespace: PropTypes.func,
  onLoadSingleNamespace: PropTypes.func,
  onLoadSingleInstance: PropTypes.func,
  onSetSchema: PropTypes.func,
  onQuerySchemaConfig: PropTypes.func,
  onDeleteNs: PropTypes.func,
  roleType: PropTypes.string,
  locale: PropTypes.string
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadAdminAllNamespaces: (resolve) => dispatch(loadAdminAllNamespaces(resolve)),
    onLoadUserNamespaces: (projectId, resolve) => dispatch(loadUserNamespaces(projectId, resolve)),
    onLoadSelectNamespaces: (projectId, resolve) => dispatch(loadSelectNamespaces(projectId, resolve)),
    onAddNamespace: (value, resolve) => dispatch(addNamespace(value, resolve)),
    onEditNamespace: (value, resolve) => dispatch(editNamespace(value, resolve)),
    onLoadDatabasesInstance: (value, resolve) => dispatch(loadDatabasesInstance(value, resolve)),
    onLoadNamespaceDatabase: (value, resolve) => dispatch(loadNamespaceDatabase(value, resolve)),
    onLoadTableNameExist: (value, resolve, reject) => dispatch(loadTableNameExist(value, resolve, reject)),
    onLoadSingleNamespace: (namespaceId, resolve) => dispatch(loadSingleNamespace(namespaceId, resolve)),
    onLoadSingleInstance: (namespaceId, resolve) => dispatch(loadSingleInstance(namespaceId, resolve)),
    onSetSchema: (namespaceId, value, type, resolve) => dispatch(setSchema(namespaceId, value, type, resolve)),
    onQuerySchemaConfig: (ids, value, type, resolve) => dispatch(querySchemaConfig(ids, value, type, resolve)),
    onDeleteNs: (namespaceId, resolve, reject) => dispatch(deleteNs(namespaceId, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  namespaces: selectNamespaces(),
  error: selectError(),
  modalLoading: selectModalLoading(),
  tableNameExited: selectTableNameExited(),
  dbUrlValue: selectDbUrlValue(),
  roleType: selectRoleType(),
  locale: selectLocale()
})

export default connect(mapStateToProps, mapDispatchToProps)(Namespace)
