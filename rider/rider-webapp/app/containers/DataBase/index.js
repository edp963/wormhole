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

import DBForm from './DBForm'
import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Icon from 'antd/lib/icon'
import Tooltip from 'antd/lib/tooltip'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
import Input from 'antd/lib/input'
import Popover from 'antd/lib/popover'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { loadDatabases, addDatabase, editDatabase, loadDatabasesInstance, loadNameExist, loadSingleDatabase } from './action'
import { selectDatabases, selectError, selectModalLoading, selectDatabaseNameExited, selectDbUrlValue } from './selectors'

export class DataBase extends React.PureComponent {
  constructor (props) {
    super(props)
    this.state = {
      formVisible: false,
      formType: 'add',
      refreshDbLoading: false,
      refreshDbText: 'Refresh',

      currentDatabases: [],
      originDatabases: [],

      filteredInfo: null,
      sortedInfo: null,

      searchTextDBInstance: '',
      filterDropdownVisibleDBInstance: false,
      searchTextDatabase: '',
      filterDropdownVisibleDatabase: false,
      searchTextConnUrl: '',
      filterDropdownVisibleConnUrl: false,
      filterDatepickerShown: false,
      startTimeText: '',
      endTimeText: '',
      createStartTimeText: '',
      createEndTimeText: '',
      filterDropdownVisibleCreateTime: false,
      updateStartTimeText: '',
      updateEndTimeText: '',
      filterDropdownVisibleUpdateTime: false,

      editDatabaseData: {},
      databaseDSType: ''
    }
  }

  componentWillMount () {
    this.props.onLoadDatabases(() => { this.refreshDbState() })
  }

  componentWillReceiveProps (props) {
    if (props.databases) {
      const originDatabases = props.databases.map(s => {
        s.key = s.id
        s.visible = false
        return s
      })
      this.setState({
        originDatabases: originDatabases.slice(),
        currentDatabases: originDatabases.slice()
      })
    }
  }

  refreshDatabase = () => {
    this.setState({
      refreshDbLoading: true,
      refreshDbText: 'Refreshing'
    })
    this.props.onLoadDatabases(() => { this.refreshDbState() })
  }

  refreshDbState () {
    this.setState({
      refreshDbLoading: false,
      refreshDbText: 'Refresh'
    })
  }

  showAddDB = () => {
    this.setState({
      formVisible: true,
      formType: 'add'
    })
  }

  // 回显
  showEditDB = (database) => (e) => {
    this.props.onLoadSingleDatabase(database.id, (result) => {
      this.setState({
        formVisible: true,
        formType: 'edit',
        editDatabaseData: {
          active: result.active,
          createBy: result.createBy,
          createTime: result.createTime,
          id: result.id,
          nsInstanceId: result.nsInstanceId,
          updateBy: result.updateBy,
          updateTime: result.updateTime
        }
      }, () => {
        if (result.nsSys === 'oracle' || result.nsSys === 'mysql' || result.nsSys === 'postgresql' || result.nsSys === 'mongodb') {
          this.dBForm.setFieldsValue({
            userRequired: result.user,
            passwordRequired: result.pwd
          })
        } else {
          this.dBForm.setFieldsValue({
            user: result.user,
            password: result.pwd
          })
        }

        let conFinal = ''
        if (result.config.indexOf(',') > -1 && result.config.indexOf('=') > -1) {
          conFinal = result.config.replace(/,/g, '\n')
        } else {
          conFinal = result.config
        }

        this.dBForm.setFieldsValue({
          dataBaseDataSystem: result.nsSys,
          instance: result.nsInstance,
          connectionUrl: result.connUrl,
          permission: result.permission,
          nsDatabase: result.nsDatabase,
          config: conFinal,
          description: result.desc,
          partition: result.partitions
        })
      })
    })
  }

  // 点击遮罩层或右上角叉或取消按钮的回调
  hideForm = () => this.setState({ formVisible: false })

  // Modal 完全关闭后的回调
  resetModal = () => this.dBForm.resetFields()

  /**
   *  JSON 格式校验
   *  如果JSON.parse能转换成功；并且字符串中包含 { 时，那么该字符串就是JSON格式的字符串。
   */
  isJSON (type, str) {
    if (typeof str === 'string') {
      if (type === 'oracle') {
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
      } else {
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
    }
    return false
  }

  /**
   * Config 格式校验
   * key=value&key=value一行或多行（多行时用 & 连接） 或 key=value 多行（用 , 连接）
   */
  onConfigValue (val) {
    let configVal = ''
    if (val.indexOf('&') > -1) {
      // key=value&key=value
      if (val.indexOf('=') > -1) {
        configVal = val.replace(/\n/g, '&')
      } else {
        configVal = val
      }
    } else {
      if (val.indexOf('=') > -1) {
        // 多行输入 key=value
        const conTempStr = val.trim()
        const numArr = (conTempStr.split('=')).length - 1

        if (numArr === 1) {
          configVal = val
        } else {
          configVal = val.replace(/\n/g, ',')
        }
      } else {
        configVal = val
      }
    }
    return configVal
  }

  onModalOk = () => {
    const { formType, editDatabaseData } = this.state
    const { databaseNameExited } = this.props

    this.dBForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (formType === 'add') {
          if (databaseNameExited === true) {
            this.dBForm.setFields({
              nsDatabase: {
                value: values.nsDatabase,
                errors: [new Error('该 Name 已存在')]
              }
            })
          } else if (values.dataBaseDataSystem === 'oracle') {
            if (values.config === undefined || (values.config.indexOf('service_name') < 0)) {
              this.dBForm.setFields({
                config: {
                  value: values.config,
                  errors: [new Error('Oracle时, 必须包含"service_name"字段')]
                }
              })
            } else {
              const addValues = {
                nsDatabase: values.nsDatabase,
                desc: values.description === undefined ? '' : values.description,
                nsInstanceId: Number(values.instance),
                permission: values.permission,
                user: values.userRequired,
                pwd: values.passwordRequired,
                partitions: 0,
                config: this.onConfigValue(values.config)
              }
              this.props.onAddDatabase(addValues, () => {
                this.hideForm()
                message.success('Database 添加成功！', 3)
              }, (result) => {
                message.error(result, 3)
              })
            }
          } else {
            let valuesUser = ''
            let valuesPwd = ''
            let valuesConfig = ''
            if (values.dataBaseDataSystem === 'kafka') {
              valuesUser = ''
              valuesPwd = ''
              valuesConfig = values.config
            } else if (values.dataBaseDataSystem === 'mysql' || values.dataBaseDataSystem === 'postgresql' || values.dataBaseDataSystem === 'mongodb') {
              valuesUser = values.userRequired
              valuesPwd = values.passwordRequired
              valuesConfig = values.config
            } else {
              if (values.user === undefined) {
                valuesUser = ''
              } else if (values.password === undefined) {
                valuesPwd = ''
              } else if (values.config === undefined) {
                valuesConfig = ''
              } else {
                valuesUser = values.user
                valuesPwd = values.password
                valuesConfig = values.config
              }
            }

            const addValues = {
              nsDatabase: values.nsDatabase,
              desc: values.description === undefined ? '' : values.description,
              nsInstanceId: Number(values.instance),
              permission: values.dataBaseDataSystem === 'kafka' ? 'ReadWrite' : values.permission,
              user: valuesUser,
              pwd: valuesPwd,
              partitions: values.dataBaseDataSystem === 'kafka' ? Number(values.partition) : 0,
              config: valuesConfig === '' ? '' : this.onConfigValue(valuesConfig)
            }

            this.props.onAddDatabase(addValues, () => {
              this.hideForm()
              message.success('Database 添加成功！', 3)
            }, (result) => {
              message.error(result, 3)
            })
          }
        } else if (formType === 'edit') {
          if (values.dataBaseDataSystem === 'oracle') {
            if (values.config === undefined || (values.config.indexOf('service_name') < 0)) {
              this.dBForm.setFields({
                config: {
                  value: values.config,
                  errors: [new Error('Oracle时, 必须包含"service_name"字段')]
                }
              })
            } else {
              const editValues = {
                permission: values.permission,
                user: values.userRequired,
                pwd: values.passwordRequired,
                config: this.isJSON('oracle', values.config) === true ? values.config : this.onConfigValue(values.config),
                desc: values.description,
                nsDatabase: values.nsDatabase,
                partitions: 0
              }

              this.props.onEditDatabase(Object.assign({}, editDatabaseData, editValues), () => {
                this.hideForm()
                message.success('Database 修改成功！', 3)
              }, (result) => {
                message.error(result, 3)
              })
            }
          } else {
            let editUser = ''
            let editPwd = ''
            if (values.dataBaseDataSystem === 'kafka') {
              editUser = ''
              editPwd = ''
            } else if (values.dataBaseDataSystem === 'mysql' || values.dataBaseDataSystem === 'postgresql' || values.dataBaseDataSystem === 'mongodb') {
              editUser = values.userRequired
              editPwd = values.passwordRequired
            } else {
              editUser = values.user
              editPwd = values.password
            }

            const editValues = {
              permission: values.dataBaseDataSystem === 'kafka' ? 'ReadWrite' : values.permission,
              user: editUser,
              pwd: editPwd,
              config: this.isJSON('others', values.config) === true ? values.config : this.onConfigValue(values.config),
              desc: values.description,
              nsDatabase: values.nsDatabase,
              partitions: values.dataBaseDataSystem === 'kafka' ? values.partition : 0
            }

            this.props.onEditDatabase(Object.assign({}, editDatabaseData, editValues), () => {
              this.hideForm()
              message.success('Database 修改成功！', 3)
            }, (result) => {
              message.error(result, 3)
            })
          }
        }
      }
    })
  }

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      [visible]: false,
      currentDatabases: this.state.originDatabases.map((record) => {
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

  handleDatabaseChange = (pagination, filters, sorter) => {
    this.setState({
      filteredInfo: filters,
      sortedInfo: sorter
    })
  }

  onRangeIdSearch = (columnName, startText, endText, visible) => () => {
    this.setState({
      [visible]: false,
      currentDatabases: this.state.originDatabases.map((record) => {
        const match = record[columnName]
        if ((match < parseInt(this.state[startText])) || (match > parseInt(this.state[endText]))) {
          return null
        }
        return record
      }).filter(record => !!record),
      filteredInfo: this.state[startText] || this.state[endText] ? { id: [0] } : { id: [] }
    })
  }

  onInputChange = (value) => (e) => this.setState({ [value]: e.target.value })

  /**
   *  新增时，通过选择不同的 data system 显示不同的 Instance 下拉框内容
   * */
  onInitDatabaseUrlValue = (value) => {
    this.props.onLoadDatabasesInstance(value, () => {
      this.setState({
        databaseDSType: value
      })
      // dbForm 的 placeholder
      this.dBForm.setFieldsValue({
        connectionUrl: '',
        instance: undefined,
        permission: '',
        // nsDatabase: value === 'hbase' ? 'default' : '',
        nsDatabase: '',
        user: '',
        password: '',
        userRequired: '',
        passwordRequired: '',
        partition: '',
        config: '',
        description: ''
      })
    })
  }

  /***
   * 新增时，验证 database name／topic name 是否存在，不存在时，才能新增
   * */
  onInitDatabaseInputValue = (value) => {
    const formValues = this.dBForm.getFieldsValue()
    const requestValues = {
      nsInstanceId: Number(formValues.instance),
      permission: formValues.permission,
      nsDatabaseName: value,
      dsType: formValues.dataBaseDataSystem
    }
    this.props.onLoadNameExist(requestValues, () => {}, () => {
      this.dBForm.setFields({
        nsDatabase: {
          value: value,
          errors: [new Error('该 Name 已存在')]
        }
      })
    })
  }

  // 当存在 service_name 时，报错提示去掉
  onInitDatabaseConfigValue = (value) => {
    const formValues = this.dBForm.getFieldsValue()
    if (formValues.dataBaseDataSystem === 'oracle') {
      if (value.indexOf('service_name') > 0 || value.indexOf('service_name') === 0) {
        this.dBForm.setFieldsValue({
          config: value
        })
      }
    }
  }

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
      currentDatabases: this.state.originDatabases.map((record) => {
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

  render () {
    const { refreshDbLoading, refreshDbText } = this.state

    let { sortedInfo, filteredInfo } = this.state
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const columns = [{
      title: 'Data System',
      dataIndex: 'nsSys',
      key: 'nsSys',
      sorter: (a, b) => {
        if (typeof a.nsSys === 'object') {
          return a.nsSysOrigin < b.nsSysOrigin ? -1 : 1
        } else {
          return a.nsSys < b.nsSys ? -1 : 1
        }
      },
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
            value={this.state.searchTextDBInstance}
            onChange={this.onInputChange('searchTextDBInstance')}
            onPressEnter={this.onSearch('nsInstance', 'searchTextDBInstance', 'filterDropdownVisibleDBInstance')}
          />
          <Button type="primary" onClick={this.onSearch('nsInstance', 'searchTextDBInstance', 'filterDropdownVisibleDBInstance')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleDBInstance,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleDBInstance: visible
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
            value={this.state.searchTextDatabase}
            onChange={this.onInputChange('searchTextDatabase')}
            onPressEnter={this.onSearch('nsDatabase', 'searchTextDatabase', 'filterDropdownVisibleDatabase')}
          />
          <Button type="primary" onClick={this.onSearch('nsDatabase', 'searchTextDatabase', 'filterDropdownVisibleDatabase')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleDatabase,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleDatabase: visible
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
      title: 'Connection URL',
      dataIndex: 'connUrl',
      key: 'connUrl',
      sorter: (a, b) => {
        if (typeof a.connUrl === 'object') {
          return a.connUrlOrigin < b.connUrlOrigin ? -1 : 1
        } else {
          return a.connUrl < b.connUrl ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'connUrl' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="URL"
            value={this.state.searchTextConnUrl}
            onChange={this.onInputChange('searchTextConnUrl')}
            onPressEnter={this.onSearch('connUrl', 'searchTextConnUrl', 'filterDropdownVisibleConnUrl')}
          />
          <Button type="primary" onClick={this.onSearch('connUrl', 'searchTextConnUrl', 'filterDropdownVisibleConnUrl')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleConnUrl,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleConnUrl: visible
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
      className: 'text-align-center',
      render: (text, record) => {
        const nsSysKafka = record.nsSys === 'kafka'
          ? ''
          : (
            <div>
              <p><strong>User：</strong>{record.user}</p>
              <p><strong>Password：</strong>{record.pwd}</p>
            </div>
          )

        return (
          <span className="ant-table-action-column">
            <Tooltip title="查看详情">
              <Popover
                placement="left"
                content={<div>
                  <p><strong>Description：</strong>{record.desc}</p>
                  <p><strong>Config：</strong>{record.config}</p>
                  <p><strong>Partitions：</strong>{record.partitions}</p>
                  {nsSysKafka}
                </div>
                }
                title={<h3>详情</h3>}
                trigger="click">
                <Button icon="file-text" shape="circle" type="ghost"></Button>
              </Popover>
            </Tooltip>

            <Tooltip title="修改">
              <Button icon="edit" shape="circle" type="ghost" onClick={this.showEditDB(record)} />
            </Tooltip>
          </span>
        )
      }
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

    return (
      <div>
        <Helmet title="Database" />
        <div className="ri-workbench-table ri-common-block">
          <h3 className="ri-common-block-title">
            <Icon type="bars" /> Datebase 列表
          </h3>
          <div className="ri-common-block-tools">
            <Button icon="poweroff" type="ghost" className="refresh-button-style" loading={refreshDbLoading} onClick={this.refreshDatabase}>{refreshDbText}</Button>
            <Button icon="plus" type="primary" onClick={this.showAddDB}>新建</Button>
          </div>
          <Table
            dataSource={this.state.currentDatabases}
            columns={columns}
            onChange={this.handleDatabaseChange}
            pagination={pagination}
            className="ri-workbench-table-container"
            bordered>
          </Table>
        </div>
        <Modal
          title={`${this.state.formType === 'add' ? '新建' : '修改'} Database`}
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
          <DBForm
            databaseFormType={this.state.formType}
            onInitDatabaseUrlValue={this.onInitDatabaseUrlValue}
            databaseUrlValue={this.props.dbUrlValue}
            onInitDatabaseInputValue={this.onInitDatabaseInputValue}
            onInitDatabaseConfigValue={this.onInitDatabaseConfigValue}
            ref={(f) => { this.dBForm = f }}
          />
        </Modal>
      </div>
    )
  }
}

DataBase.propTypes = {
  modalLoading: React.PropTypes.bool,
  databaseNameExited: React.PropTypes.bool,
  dbUrlValue: React.PropTypes.oneOfType([
    React.PropTypes.bool,
    React.PropTypes.array
  ]),
  onLoadDatabases: React.PropTypes.func,
  onAddDatabase: React.PropTypes.func,
  onEditDatabase: React.PropTypes.func,
  onLoadDatabasesInstance: React.PropTypes.func,
  onLoadNameExist: React.PropTypes.func,
  onLoadSingleDatabase: React.PropTypes.func
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadDatabases: (resolve) => dispatch(loadDatabases(resolve)),
    onAddDatabase: (database, resolve, reject) => dispatch(addDatabase(database, resolve, reject)),
    onEditDatabase: (database, resolve, reject) => dispatch(editDatabase(database, resolve, reject)),
    onLoadDatabasesInstance: (value, resolve) => dispatch(loadDatabasesInstance(value, resolve)),
    onLoadNameExist: (value, resolve, reject) => dispatch(loadNameExist(value, resolve, reject)),
    onLoadSingleDatabase: (databaseId, resolve) => dispatch(loadSingleDatabase(databaseId, resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  databases: selectDatabases(),
  error: selectError(),
  modalLoading: selectModalLoading(),
  databaseNameExited: selectDatabaseNameExited(),
  dbUrlValue: selectDbUrlValue()
})

export default connect(mapStateToProps, mapDispatchToProps)(DataBase)
