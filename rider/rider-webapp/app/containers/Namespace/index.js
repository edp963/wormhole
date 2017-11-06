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

import NamespaceForm from './NamespaceForm'
import Table from 'antd/lib/table'
import Icon from 'antd/lib/icon'
import Input from 'antd/lib/input'
import Button from 'antd/lib/button'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { loadDatabasesInstance } from '../../containers/DataBase/action'
import { selectDbUrlValue } from '../../containers/DataBase/selectors'
import { loadSingleInstance } from '../../containers/Instance/action'
import { loadAdminAllNamespaces, loadUserNamespaces, loadSelectNamespaces, loadNamespaceDatabase, addNamespace, editNamespace, loadTableNameExist, loadSingleNamespace } from './action'
import { selectNamespaces, selectError, selectModalLoading, selectTableNameExited } from './selectors'

export class Namespace extends React.PureComponent {
  constructor (props) {
    super(props)
    this.state = {
      formVisible: false,
      namespaceFormType: 'add',
      refreshNsLoading: false,
      refreshNsText: 'Refresh',

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
      nsInstanceVal: ''
    }
  }

  componentWillMount () {
    this.loadNamespaceData()
  }

  componentWillReceiveProps (props) {
    if (props.namespaces) {
      const originNamespaces = props.namespaces.map(s => {
        s.key = s.id
        s.visible = false
        return s
      })
      this.setState({
        originNamespaces: originNamespaces.slice(),
        currentNamespaces: originNamespaces.slice()
      })
    }
  }

  refreshNamespace = () => {
    this.setState({
      refreshNsLoading: true,
      refreshNsText: 'Refreshing'
    })
    this.loadNamespaceData()
  }

  loadNamespaceData () {
    if (localStorage.getItem('loginRoleType') === 'admin') {
      this.props.namespaceClassHide === 'hide'
        ? this.props.onLoadSelectNamespaces(this.props.projectIdGeted, () => { this.nsRefreshState() })
        : this.props.onLoadAdminAllNamespaces(() => { this.nsRefreshState() })
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      this.props.onLoadUserNamespaces(this.props.projectIdGeted, () => { this.nsRefreshState() })
    }
  }

  nsRefreshState () {
    this.setState({
      refreshNsLoading: false,
      refreshNsText: 'Refresh'
    })
  }

  handleNamespaceChange = (pagination, filters, sorter) => {
    this.setState({
      filteredInfo: filters,
      sortedInfo: sorter
    })
  }

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      [visible]: false,
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
  }

  onInputChange = (value) => (e) => this.setState({ [value]: e.target.value })

  handleEndOpenChange = (status) => {
    this.setState({
      filterDatepickerShown: status
    })
  }

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
      [visible]: false,
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
            nsDatabase: [
              result.nsDatabase,
              result.permission
            ],
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
              permission: result.permission,
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
            this.namespaceForm.setFieldsValue({
              connectionUrl: result.connUrl
            })
          })
        })
    })
  }

  // 点击遮罩层或右上角叉或取消按钮的回调
  hideForm = () => {
    this.setState({
      formVisible: false
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
    this.setState({
      namespaceTableSource: []
    })
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
    const addValues = Object.assign({}, requestOthers, { nsTables: requestNsTables })
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

    const addValues = Object.assign({}, requestOthers, { nsTables: requestNsTables })
    this.nsAdd(addValues)
  }

  nsTableAdd (requestNsTables, addTableValue, addKeyValue, requestOthers) {
    const { namespaceTableSource } = this.state

    requestNsTables.push({
      table: addTableValue,
      key: addKeyValue
    })

    if (namespaceTableSource.find(i => i.nsModalTable === addTableValue)) {
      this.nsErrorMsg('Table 重名')
    } else {
      const addValues = Object.assign({}, requestOthers, { nsTables: requestNsTables })
      this.nsAdd(addValues)
    }
  }

  onModalOk = () => {
    const { namespaceTableSource, databaseSelectValue, namespaceFormType, exitedNsTableValue, editNamespaceData, nsInstanceVal } = this.state
    const { tableNameExited } = this.props

    this.namespaceForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (tableNameExited === true) {
          this.nsErrorMsg(`${exitedNsTableValue} 已存在`)
        } else {
          const selDatabase = databaseSelectValue.find(s => s.id === Number(values.nsDatabase))
          const addTableValue = values.nsSingleTableName
          const addKeyValue = values.nsSingleKeyValue

          if (namespaceFormType === 'add') {
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
              if (addTableValue === undefined || addTableValue === '') {
                this.nsErrorMsg(values.dataBaseDataSystem === 'es' ? '请填写 Type' : '请填写 Table')
              } else {
                if (values.dataBaseDataSystem === 'hbase') {
                  this.namespaceForm.setFields({
                    nsTables: {
                      errors: []
                    }
                  })
                  this.nsKeyAdd(addTableValue, addKeyValue, requestOthers)
                } else {
                  if (addKeyValue === undefined || addKeyValue === '') {
                    this.nsErrorMsg('请填写 Key')
                  } else {
                    this.nsKeyAdd(addTableValue, addKeyValue, requestOthers)
                  }
                }
              }
            } else {
              if (values.dataBaseDataSystem === 'hbase') {
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
                  this.nsErrorMsg(values.dataBaseDataSystem === 'es' ? 'Type & Key 填写同步' : 'Table & Key 填写同步')
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
          } else if (namespaceFormType === 'edit') {
            const editKeysValue = values.nsSingleKeyValue

            if (values.dataBaseDataSystem === 'hbase') {
              this.props.onEditNamespace(Object.assign({}, editNamespaceData, { keys: '' }), () => {
                this.hideForm()
                message.success('Namespace 修改成功！', 3)
              })
            } else {
              if (editKeysValue === '') {
                this.nsErrorMsg('请填写 Key')
              } else {
                this.props.onEditNamespace(Object.assign({}, editNamespaceData, { keys: editKeysValue }), () => {
                  this.hideForm()
                  message.success('Namespace 修改成功！', 3)
                })
              }
            }
          }
        }
      }
    })
  }

  /**
   *  新增时，通过选择不同的 data system 显示不同的 Instance 内容
   * */
  onInitNamespaceUrlValue = (value) => {
    this.setState({
      addTableClassTable: 'hide'
    })

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

  onInitDatabaseSelectValue = (value) => {
    this.props.onLoadNamespaceDatabase(value, (result) => {
      this.setState({
        databaseSelectValue: result
      })
      this.cleanNsTableData()
    })
  }

  onDeleteTable = (index) => (e) => {
    const { namespaceTableSource } = this.state
    this.namespaceForm.setFields({
      nsTables: {
        errors: []
      }
    })
    namespaceTableSource.splice(index, 1)
    this.setState({
      namespaceTableSource: [...namespaceTableSource]
    }, () => {
      if (namespaceTableSource.length === 0) {
        this.setState({
          addTableClassTable: 'hide'
        })
      }
    })
  }

  onAddTable = () => {
    const { namespaceTableSource, exitedNsTableValue, nsDsVal } = this.state
    const { tableNameExited } = this.props

    const moadlTempVal = this.namespaceForm.getFieldsValue()

    if (moadlTempVal.dataBaseDataSystem === undefined || moadlTempVal.connectionUrl === undefined || moadlTempVal.instance === undefined || moadlTempVal.nsDatabase === undefined) {
      this.nsErrorMsg('请先选择其他项')
    } else if (moadlTempVal.nsSingleTableName === '' || moadlTempVal.nsSingleTableName === undefined) {
      this.nsErrorMsg(nsDsVal === 'es' ? '请填写 Type' : '请填写 Table')
    } else if (tableNameExited === true) {
      this.nsErrorMsg(`${exitedNsTableValue} 已存在`)
    } else if (namespaceTableSource.find(i => i.nsModalTable === moadlTempVal.nsSingleTableName)) {
      this.nsErrorMsg('Table 重名')
    } else {
      if (nsDsVal === 'hbase') {
        this.addTableTemp('')
      } else {
        if (moadlTempVal.nsSingleKeyValue === '' || moadlTempVal.nsSingleKeyValue === undefined) {
          this.nsErrorMsg('请填写 Key')
        } else {
          this.addTableTemp(moadlTempVal.nsSingleKeyValue)
        }
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
        nsTables: {
          errors: []
        }
      })
    })
  }

  /***
   * 新增时，验证 table name 是否存在，不存在时，才能新增
   * */
  onInitNsNameInputValue = (val) => {
    const formValues = this.namespaceForm.getFieldsValue()

    const requestValues = {
      instanceId: Number(formValues.instance),
      databaseId: Number(formValues.nsDatabase),
      tableNames: val
    }

    this.props.onLoadTableNameExist(requestValues, () => {
      this.namespaceForm.setFields({
        nsTables: {
          errors: []
        }
      })
    }, () => {
      this.nsErrorMsg(`${val} 已存在`)
      this.setState({
        exitedNsTableValue: val
      })
    })
  }

  /**
   * table key 不为空时
   */
  onInitNsKeyInputValue = (val) => {
    if (val !== '') {
      this.namespaceForm.setFields({
        nsTables: {
          errors: []
        }
      })
    }
  }

  render () {
    const { refreshNsLoading, refreshNsText } = this.state

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
        filters: [
          {text: 'oracle', value: 'oracle'},
          {text: 'mysql', value: 'mysql'},
          {text: 'es', value: 'es'},
          {text: 'hbase', value: 'hbase'},
          {text: 'phoenix', value: 'phoenix'},
          {text: 'cassandra', value: 'cassandra'},
          // {text: 'log', value: 'log'},
          {text: 'kafka', value: 'kafka'},
          {text: 'postgresql', value: 'postgresql'},
          {text: 'mongodb', value: 'mongodb'}
        ],
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
        title: 'Permission',
        dataIndex: 'permission',
        key: 'permission',
        sorter: (a, b) => {
          if (typeof a.permission === 'object') {
            return a.permissionOrigin < b.permissionOrigin ? -1 : 1
          } else {
            return a.permission < b.permission ? -1 : 1
          }
        },
        sortOrder: sortedInfo.columnKey === 'permission' && sortedInfo.order,
        filters: [
          {text: 'ReadOnly', value: 'ReadOnly'},
          {text: 'ReadWrite', value: 'ReadWrite'}
        ],
        filteredValue: filteredInfo.permission,
        onFilter: (value, record) => record.permission.includes(value)
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
        filteredValue: filteredInfo.createTime,
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
        filteredValue: filteredInfo.updateTime,
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
        className: `text-align-center ${this.props.namespaceClassHide}`,
        render: (text, record) => (
          <span className="ant-table-action-column">
            <Tooltip title="查看详情">
              <Popover
                placement="left"
                content={<div className="project-name-detail">
                  <p><strong>   Project Names：</strong>{record.projectName}</p>
                </div>}
                title={<h3>详情</h3>}
                trigger="click">
                <Button icon="file-text" shape="circle" type="ghost"></Button>
              </Popover>
            </Tooltip>
            <Tooltip title="修改">
              <Button icon="edit" shape="circle" type="ghost" onClick={this.showEditNamespace(record)}></Button>
            </Tooltip>
          </span>
        )
      }]

    const pagination = {
      defaultPageSize: this.state.pageSize,
      showSizeChanger: true,
      onShowSizeChange: (current, pageSize) => {
        this.setState({
          pageIndex: current,
          pageSize: pageSize
        })
      },
      onChange: (current) => {
        this.setState({
          pageIndex: current
        })
      }
    }

    const helmetHide = this.props.namespaceClassHide !== 'hide'
      ? (<Helmet title="Namespace" />)
      : (<Helmet title="Workbench" />)

    return (
      <div>
        {helmetHide}
        <div className="ri-workbench-table ri-common-block">
          <h3 className="ri-common-block-title">
            <Icon type="bars" /> Namespace 列表
          </h3>
          <div className="ri-common-block-tools">
            <Button icon="poweroff" type="ghost" className="refresh-button-style" loading={refreshNsLoading} onClick={this.refreshNamespace}>{refreshNsText}</Button>
            <Button icon="plus" type="primary" className={this.props.namespaceClassHide} onClick={this.showAddNamespace}>新建</Button>
          </div>
          <Table
            dataSource={this.state.currentNamespaces || []}
            columns={columns}
            onChange={this.handleNamespaceChange}
            pagination={pagination}
            className="ri-workbench-table-container"
            bordered>
          </Table>
        </div>
        <Modal
          title={`${this.state.namespaceFormType === 'add' ? '新建' : '修改'} Namespace`}
          okText="保存"
          wrapClassName="db-form-style"
          visible={this.state.formVisible}
          onCancel={this.hideForm}
          afterClose={this.resetModal}
          footer={[
            <Button
              key="cancel"
              size="large"
              type="ghost"
              onClick={this.hideForm}
            >
              取消
            </Button>,
            <Button
              key="submit"
              size="large"
              type="primary"
              loading={this.props.modalLoading}
              onClick={this.onModalOk}
            >
              保存
            </Button>
          ]}
        >
          <NamespaceForm
            namespaceFormType={this.state.namespaceFormType}
            onInitNamespaceUrlValue={this.onInitNamespaceUrlValue}
            namespaceUrlValue={this.props.dbUrlValue}
            onInitDatabaseSelectValue={this.onInitDatabaseSelectValue}
            onInitNsNameInputValue={this.onInitNsNameInputValue}
            onInitNsKeyInputValue={this.onInitNsKeyInputValue}
            databaseSelectValue={this.state.databaseSelectValue}
            namespaceTableSource={this.state.namespaceTableSource}
            deleteTableClass={this.state.deleteTableClass}
            addTableClass={this.state.addTableClass}
            addTableClassTable={this.state.addTableClassTable}
            addBtnDisabled={this.state.addBtnDisabled}
            onAddTable={this.onAddTable}
            onDeleteTable={this.onDeleteTable}
            cleanNsTableData={this.cleanNsTableData}
            count={this.state.count}
            ref={(f) => { this.namespaceForm = f }}
          />
        </Modal>
      </div>
    )
  }
}

Namespace.propTypes = {
  modalLoading: React.PropTypes.bool,
  tableNameExited: React.PropTypes.bool,
  dbUrlValue: React.PropTypes.oneOfType([
    React.PropTypes.bool,
    React.PropTypes.array
  ]),
  onLoadAdminAllNamespaces: React.PropTypes.func,
  onLoadUserNamespaces: React.PropTypes.func,
  projectIdGeted: React.PropTypes.string,
  namespaceClassHide: React.PropTypes.string,
  onLoadSelectNamespaces: React.PropTypes.func,
  onLoadDatabasesInstance: React.PropTypes.func,
  onLoadNamespaceDatabase: React.PropTypes.func,
  onLoadTableNameExist: React.PropTypes.func,
  onAddNamespace: React.PropTypes.func,
  onEditNamespace: React.PropTypes.func,
  onLoadSingleNamespace: React.PropTypes.func,
  onLoadSingleInstance: React.PropTypes.func
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
    onLoadSingleInstance: (namespaceId, resolve) => dispatch(loadSingleInstance(namespaceId, resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  namespaces: selectNamespaces(),
  error: selectError(),
  modalLoading: selectModalLoading(),
  tableNameExited: selectTableNameExited(),
  dbUrlValue: selectDbUrlValue()
})

export default connect(mapStateToProps, mapDispatchToProps)(Namespace)
