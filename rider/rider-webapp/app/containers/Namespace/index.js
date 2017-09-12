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
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { loadDatabasesInstance } from '../../containers/DataBase/action'
import { loadSingleInstance } from '../../containers/Instance/action'
import { loadAdminAllNamespaces, loadUserNamespaces, loadSelectNamespaces, loadNamespaceDatabase, addNamespace, editNamespace, loadTableNameExist, loadSingleNamespace } from './action'
import { selectNamespaces, selectError, selectModalLoading } from './selectors'

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

      searchTextNsProject: '',
      filterDropdownVisibleNsProject: false,
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

      tableNameExited: false,
      namespaceUrlValue: [],
      databaseSelectValue: [],
      deleteTableClass: 'hide',
      addTableClass: '',
      addTableClassTable: '',
      addBtnDisabled: false,
      count: 0,
      namespaceTableSource: [],

      editNamespaceData: {},
      exitedNsTableValue: ''
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
      addTableClassTable: '',
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

  onModalOk = () => {
    const { namespaceTableSource, databaseSelectValue, namespaceFormType, tableNameExited, exitedNsTableValue, editNamespaceData } = this.state

    this.namespaceForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const selDatabase = databaseSelectValue.find(s => s.id === Number(values.nsDatabase))

        if (namespaceFormType === 'add') {
          let requestNsTables = []
          if (namespaceTableSource.length === 0) {
            this.namespaceForm.setFields({
              nsTables: {
                errors: [new Error('Tables 暂无数据')]
              }
            })
          } else if (tableNameExited === true) {
            this.namespaceForm.setFields({
              nsTables: {
                errors: [new Error(`${exitedNsTableValue} 已存在`)]
              }
            })
          } else {
            namespaceTableSource.map(i => {
              requestNsTables.push({
                table: i.nsModalTable,
                key: i.nsModalKey
              })
              return i
            })

            const addValues = {
              nsDatabase: selDatabase.nsDatabase,
              nsDatabaseId: Number(values.nsDatabase),
              nsInstance: values.instance,
              nsInstanceId: Number(values.connectionUrl),
              nsSys: values.dataBaseDataSystem,
              nsTables: requestNsTables
            }

            this.props.onAddNamespace(addValues, () => {
              this.hideForm()
              message.success('Namespace 添加成功！', 3)
            })
          }
        } else if (namespaceFormType === 'edit') {
          const editKeysValue = values.nsSingleKeyValue
          if (editKeysValue === '') {
            this.namespaceForm.setFields({
              nsTables: {
                errors: [new Error('请填写 Key')]
              }
            })
          } else {
            this.props.onEditNamespace(Object.assign({}, editNamespaceData, { keys: editKeysValue }), () => {
              this.hideForm()
              message.success('Namespace 修改成功！', 3)
            })
          }
        }
      }
    })
  }

  /**
   *  新增时，通过选择不同的 data system 显示不同的 Connection url内容
   * */
  onInitNamespaceUrlValue = (value) => {
    this.props.onLoadDatabasesInstance(value, (result) => {
      this.setState({
        namespaceUrlValue: result,
        databaseSelectValue: []
      })
      // namespaceForm 的 placeholder
      this.namespaceForm.setFieldsValue({
        connectionUrl: undefined,
        instance: '',
        nsDatabase: undefined
      })
      this.cleanNsTableData()
    }, () => {})
  }

  /***
   * 新增时，通过 instance id 显示database下拉框内容
   * */
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
    })
  }

  onAddTable = () => {
    const { count, namespaceTableSource, tableNameExited, exitedNsTableValue } = this.state

    const moadlTempVal = this.namespaceForm.getFieldsValue()

    if (moadlTempVal.dataBaseDataSystem === undefined || moadlTempVal.connectionUrl === undefined || moadlTempVal.instance === undefined || moadlTempVal.nsDatabase === undefined) {
      this.namespaceForm.setFields({
        nsTables: {
          errors: [new Error('请先选择其他项')]
        }
      })
    } else if (moadlTempVal.nsSingleTableName === '' || moadlTempVal.nsSingleTableName === undefined) {
      this.namespaceForm.setFields({
        nsTables: {
          errors: [new Error('请填写 Table')]
        }
      })
    } else if (tableNameExited === true) {
      this.namespaceForm.setFields({
        nsTables: {
          errors: [new Error(`${exitedNsTableValue} 已存在`)]
        }
      })
    } else if (namespaceTableSource.find(i => i.nsModalTable === moadlTempVal.nsSingleTableName)) {
      this.namespaceForm.setFields({
        nsTables: {
          errors: [new Error('Table 重名')]
        }
      })
    } else if (moadlTempVal.nsSingleKeyValue === '' || moadlTempVal.nsSingleKeyValue === undefined) {
      this.namespaceForm.setFields({
        nsTables: {
          errors: [new Error('请填写 Key')]
        }
      })
    } else {
      const nsTableSourceTemp = {
        key: count,
        nsModalTable: moadlTempVal.nsSingleTableName,
        nsModalKey: moadlTempVal.nsSingleKeyValue === undefined ? '' : moadlTempVal.nsSingleKeyValue
      }

      this.setState({
        namespaceTableSource: [...namespaceTableSource, nsTableSourceTemp],
        count: count + 1,
        deleteTableClass: ''
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
  }

  /***
   * 新增时，验证 table name 是否存在，不存在时，才能新增
   * */
  onInitNsNameInputValue = (val) => {
    const formValues = this.namespaceForm.getFieldsValue()

    const requestValues = {
      instanceId: Number(formValues.connectionUrl),
      databaseId: Number(formValues.nsDatabase),
      tableNames: val
    }

    this.props.onLoadTableNameExist(requestValues, () => {
      this.setState({
        tableNameExited: false
      })
      this.namespaceForm.setFields({
        nsTables: {
          errors: []
        }
      })
    }, () => {
      this.namespaceForm.setFields({
        nsTables: {
          errors: [new Error(`${val} 已存在`)]
        }
      })
      this.setState({
        tableNameExited: true,
        exitedNsTableValue: val
      })
    })
  }

  render () {
    const { refreshNsLoading, refreshNsText } = this.state

    let { sortedInfo, filteredInfo } = this.state
    let { namespaceClassHide } = this.props
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const columns = [
      {
        title: 'Project',
        dataIndex: 'projectName',
        key: 'projectName',
        className: `${namespaceClassHide}`,
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
              placeholder="Project Name"
              value={this.state.searchTextNsProject}
              onChange={this.onInputChange('searchTextNsProject')}
              onPressEnter={this.onSearch('projectName', 'searchTextNsProject', 'filterDropdownVisibleNsProject')}
            />
            <Button type="primary" onClick={this.onSearch('projectName', 'searchTextNsProject', 'filterDropdownVisibleNsProject')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleNsProject,
        onFilterDropdownVisibleChange: visible => this.setState({ filterDropdownVisibleNsProject: visible })
      }, {
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
          {text: 'log', value: 'log'},
          {text: 'kafka', value: 'kafka'}
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
              placeholder="Instance"
              value={this.state.searchNsInstance}
              onChange={this.onInputChange('searchNsInstance')}
              onPressEnter={this.onSearch('nsInstance', 'searchNsInstance', 'filterDropdownVisibleNsInstance')}
            />
            <Button type="primary" onClick={this.onSearch('nsInstance', 'searchNsInstance', 'filterDropdownVisibleNsInstance')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleNsInstance,
        onFilterDropdownVisibleChange: visible => this.setState({ filterDropdownVisibleNsInstance: visible })
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
              placeholder="Database"
              value={this.state.searchNsDatabase}
              onChange={this.onInputChange('searchNsDatabase')}
              onPressEnter={this.onSearch('nsDatabase', 'searchNsDatabase', 'filterDropdownVisibleNsDatabase')}
            />
            <Button type="primary" onClick={this.onSearch('nsDatabase', 'searchNsDatabase', 'filterDropdownVisibleNsDatabase')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleNsDatabase,
        onFilterDropdownVisibleChange: visible => this.setState({ filterDropdownVisibleNsDatabase: visible })
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
              placeholder="Table"
              value={this.state.searchNsTable}
              onChange={this.onInputChange('searchNsTable')}
              onPressEnter={this.onSearch('nsTable', 'searchNsTable', 'filterDropdownVisibleNsTable')}
            />
            <Button type="primary" onClick={this.onSearch('nsTable', 'searchNsTable', 'filterDropdownVisibleNsTable')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleNsTable,
        onFilterDropdownVisibleChange: visible => this.setState({ filterDropdownVisibleNsTable: visible })
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
              placeholder="Key"
              value={this.state.searchNsKey}
              onChange={this.onInputChange('searchNsKey')}
              onPressEnter={this.onSearch('keys', 'searchNsKey', 'filterDropdownVisibleNsKey')}
            />
            <Button type="primary" onClick={this.onSearch('keys', 'searchNsKey', 'filterDropdownVisibleNsKey')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleNsKey,
        onFilterDropdownVisibleChange: visible => this.setState({ filterDropdownVisibleNsKey: visible })
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
              placeholder="Topic"
              value={this.state.searchNstopic}
              onChange={this.onInputChange('searchNstopic')}
              onPressEnter={this.onSearch('topic', 'searchNstopic', 'filterDropdownVisibleNsTopic')}
            />
            <Button type="primary" onClick={this.onSearch('topic', 'searchNstopic', 'filterDropdownVisibleNsTopic')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleNsTopic,
        onFilterDropdownVisibleChange: visible => this.setState({ filterDropdownVisibleNsTopic: visible })
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
          <Tooltip title="修改">
            <Button icon="edit" shape="circle" type="ghost" onClick={this.showEditNamespace(record)}></Button>
          </Tooltip>
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
            namespaceUrlValue={this.state.namespaceUrlValue}
            onInitDatabaseSelectValue={this.onInitDatabaseSelectValue}
            onInitNsNameInputValue={this.onInitNsNameInputValue}
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
    onLoadDatabasesInstance: (value, resolve, reject) => dispatch(loadDatabasesInstance(value, resolve, reject)),
    onLoadNamespaceDatabase: (value, resolve) => dispatch(loadNamespaceDatabase(value, resolve)),
    onLoadTableNameExist: (value, resolve, reject) => dispatch(loadTableNameExist(value, resolve, reject)),
    onLoadSingleNamespace: (namespaceId, resolve) => dispatch(loadSingleNamespace(namespaceId, resolve)),
    onLoadSingleInstance: (namespaceId, resolve) => dispatch(loadSingleInstance(namespaceId, resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  namespaces: selectNamespaces(),
  error: selectError(),
  modalLoading: selectModalLoading()
})

export default connect(mapStateToProps, mapDispatchToProps)(Namespace)
