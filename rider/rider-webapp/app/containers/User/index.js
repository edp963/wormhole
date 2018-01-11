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

import UserForm from './UserForm'
import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Icon from 'antd/lib/icon'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
import Popconfirm from 'antd/lib/popconfirm'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
import Input from 'antd/lib/input'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { loadAdminAllUsers, loadUserUsers, addUser, editUser, loadEmailInputValue,
  loadSelectUsers, loadUserDetail, deleteUser } from './action'
import { selectUsers, selectError, selectModalLoading, selectEmailExited } from './selectors'

export class User extends React.PureComponent {
  constructor (props) {
    super(props)
    this.state = {
      formVisible: false,
      formType: 'add',
      refreshUserLoading: false,
      refreshUserText: 'Refresh',

      currentUsers: [],
      originUsers: [],

      filteredInfo: null,
      sortedInfo: null,

      searchStartIdText: '',
      searchEndIdText: '',
      filterDropdownVisibleId: false,
      searchTextUserProject: '',
      filterDropdownVisibleUserProject: false,
      searchName: '',
      filterDropdownVisibleName: false,
      searchEmail: '',
      filterDropdownVisibleEmail: false,
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

      editUsersMsgData: {},
      editUsersPswData: {},
      showUserDetail: {}
    }
  }

  componentWillMount () {
    this.refreshUser()
  }

  componentWillReceiveProps (props) {
    if (props.users) {
      const originUsers = props.users.map(s => {
        s.key = s.id
        s.visible = false
        return s
      })
      this.setState({
        originUsers: originUsers.slice(),
        currentUsers: originUsers.slice()
      })
    }
  }

  refreshUser = () => {
    this.setState({
      refreshUserLoading: true,
      refreshUserText: 'Refreshing'
    })
    this.loadUserData()
  }

  loadUserData () {
    if (localStorage.getItem('loginRoleType') === 'admin') {
      this.props.userClassHide === 'hide'
        ? this.props.onLoadSelectUsers(this.props.projectIdGeted, () => { this.userRefreshState() })
        : this.props.onLoadAdminAllUsers(() => { this.userRefreshState() })
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      this.props.onLoadUserUsers(this.props.projectIdGeted, () => { this.userRefreshState() })
    }
  }

  userRefreshState () {
    this.setState({
      refreshUserLoading: false,
      refreshUserText: 'Refresh'
    })

    const { columnNameText, valueText, visibleBool } = this.state
    const { paginationInfo, filteredInfo, sortedInfo } = this.state
    const { startTimeTextState, endTimeTextState } = this.state

    this.handleUserChange(paginationInfo, filteredInfo, sortedInfo)
    if (columnNameText !== '') {
      this.onSearch(columnNameText, valueText, visibleBool)()

      if (columnNameText === 'createTime' || columnNameText === 'updateTime') {
        this.onRangeTimeSearch(columnNameText, startTimeTextState, endTimeTextState, visibleBool)()
      }
    }
  }

  showAdd = () => {
    this.setState({
      formVisible: true,
      formType: 'add'
    })
  }

  showDetail = (user) => (e) => {
    this.setState({
      formVisible: true,
      formType: 'editMsg'
    }, () => {
      this.props.onLoadUserDetail(user.id, (result) => {
        this.setState({
          editUsersMsgData: {
            active: result.active,
            createBy: result.createBy,
            createTime: result.createTime,
            roleType: result.roleType,
            updateBy: result.updateBy,
            updateTime: result.updateTime,
            password: result.password
          }
        })

        this.userForm.setFieldsValue({
          id: result.id,
          email: result.email,
          name: result.name,
          roleType: result.roleType
        })
      })
    })
  }

  showDetailPsw = (user) => (e) => {
    this.setState({
      formVisible: true,
      formType: 'editPsw'
    }, () => {
      this.props.onLoadUserDetail(user.id, (result) => {
        this.setState({
          editUsersPswData: {
            active: result.active,
            createBy: result.createBy,
            createTime: result.createTime,
            email: result.email,
            id: result.id,
            name: result.name,
            roleType: result.roleType,
            updateBy: result.updateBy,
            updateTime: result.updateTime
          }
        })

        this.userForm.setFieldsValue({
          email: result.email
        })
      })
    })
  }

  hideForm = () => {
    this.setState({
      formVisible: false
    })
    this.userForm.resetFields()
  }

  onModalOk = () => {
    const { formType, editUsersMsgData, editUsersPswData } = this.state
    const { onAddUser, onEditUser, emailExited } = this.props

    this.userForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (formType === 'add') {
          if (emailExited) {
            this.userForm.setFields({
              email: {
                value: values.email,
                errors: [new Error('该 Email 已存在')]
              }
            })
          } else {
            onAddUser(values, () => {
              this.hideForm()
              message.success('User 添加成功！', 3)
            })
          }
        } else if (formType === 'editMsg') {
          onEditUser(Object.assign({}, editUsersMsgData, values), () => {
            this.hideForm()
            message.success('用户信息修改成功！', 3)
          })
        } else if (formType === 'editPsw') {
          onEditUser(Object.assign({}, editUsersPswData, { password: values.password }), () => {
            this.hideForm()
            message.success('密码修改成功！', 3)
          })
        }
      }
    })
  }

  /***
   * 新增时，判断email是否已存在
   * */
  onInitEmailInputValue = (value) => {
    this.props.onLoadEmailInputValue(value, () => {}, () => {
      this.userForm.setFields({
        email: {
          value: value,
          errors: [new Error('该 Email 已存在')]
        }
      })
    })
  }

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      filteredInfo: {roleType: []}
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        valueText: value,
        visibleBool: visible,
        currentUsers: this.state.originUsers.map((record) => {
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

  handleUserChange = (pagination, filters, sorter) => {
    if (filters) {
      if (filters.roleType) {
        if (filters.roleType.length !== 0) {
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
      filteredInfo: {roleType: []}
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        startTimeTextState: startTimeText,
        endTimeTextState: endTimeText,
        visibleBool: visible,
        currentUsers: this.state.originUsers.map((record) => {
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

  handleVisibleChangeUser = (record) => (visible) => {
    if (visible) {
      this.setState({
        visible
      }, () => {
        this.props.onLoadUserDetail(record.id, (result) => this.setState({ showUserDetail: result }))
      })
    }
  }

  deleteUserBtn = (record) => (e) => {
    this.props.onDeleteUser(record.id, () => {
      message.success('删除成功！', 3)
    }, (result) => {
      message.error(`删除失败： ${result}`, 5)
    })
  }

  render () {
    const { refreshUserLoading, refreshUserText, formType, showUserDetail } = this.state

    let { sortedInfo, filteredInfo } = this.state
    let { userClassHide } = this.props
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const columns = [
      {
        title: 'ID',
        dataIndex: 'id',
        key: 'id',
        sorter: (a, b) => a.id - b.id,
        sortOrder: sortedInfo.columnKey === 'id' && sortedInfo.order
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
              value={this.state.searchName}
              onChange={this.onInputChange('searchName')}
              onPressEnter={this.onSearch('name', 'searchName', 'filterDropdownVisibleName')}
            />
            <Button type="primary" onClick={this.onSearch('name', 'searchName', 'filterDropdownVisibleName')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleName,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisibleName: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Email',
        dataIndex: 'email',
        key: 'email',
        sorter: (a, b) => {
          if (typeof a.email === 'object') {
            return a.emailOrigin < b.emailOrigin ? -1 : 1
          } else {
            return a.email < b.email ? -1 : 1
          }
        },
        sortOrder: sortedInfo.columnKey === 'email' && sortedInfo.order,
        filterDropdown: (
          <div className="custom-filter-dropdown">
            <Input
              ref={ele => { this.searchInput = ele }}
              placeholder="Email"
              value={this.state.searchEmail}
              onChange={this.onInputChange('searchEmail')}
              onPressEnter={this.onSearch('email', 'searchEmail', 'filterDropdownVisibleEmail')}
            />
            <Button type="primary" onClick={this.onSearch('email', 'searchEmail', 'filterDropdownVisibleEmail')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleEmail,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisibleEmail: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Role Type',
        dataIndex: 'roleType',
        key: 'roleType',
        // className: 'text-align-center',
        sorter: (a, b) => a.roleType < b.roleType ? -1 : 1,
        sortOrder: sortedInfo.columnKey === 'roleType' && sortedInfo.order,
        filters: [
          {text: 'admin', value: 'admin'},
          {text: 'user', value: 'user'},
          {text: 'app', value: 'app'}
        ],
        filteredValue: filteredInfo.roleType,
        onFilter: (value, record) => record.roleType.includes(value)
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
        className: `text-align-center ${userClassHide}`,
        render: (text, record) => (
          <span className="ant-table-action-column">
            <Tooltip title="查看详情">
              <Popover
                placement="left"
                content={<div className="project-name-detail">
                  <p><strong>   Project Names：</strong>{showUserDetail.projectNames}</p>
                </div>}
                title={<h3>详情</h3>}
                trigger="click"
                onVisibleChange={this.handleVisibleChangeUser(record)}
              >
                <Button icon="file-text" shape="circle" type="ghost"></Button>
              </Popover>
            </Tooltip>
            <Tooltip title="修改用户信息">
              <Button icon="user" shape="circle" type="ghost" onClick={this.showDetail(record)} />
            </Tooltip>

            <Tooltip title="修改密码">
              <Button icon="key" shape="circle" type="ghost" onClick={this.showDetailPsw(record)} />
            </Tooltip>

            {
              localStorage.getItem('loginRoleType') === 'admin'
                ? (
                  <Popconfirm placement="bottom" title="确定删除吗？" okText="Yes" cancelText="No" onConfirm={this.deleteUserBtn(record)}>
                    <Tooltip title="删除">
                      <Button icon="delete" shape="circle" type="ghost"></Button>
                    </Tooltip>
                  </Popconfirm>
                )
                : ''
            }
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

    const helmetHide = this.props.userClassHide !== 'hide'
      ? (<Helmet title="User" />)
      : (<Helmet title="Workbench" />)

    let userTitle = ''
    if (formType === 'add') {
      userTitle = '新建 User'
    } else if (formType === 'editMsg') {
      userTitle = '修改用户信息'
    } else if (formType === 'editPsw') {
      userTitle = '修改密码'
    }

    return (
      <div>
        {helmetHide}
        <div className="ri-workbench-table ri-common-block">
          <h3 className="ri-common-block-title">
            <Icon type="bars" /> User 列表
          </h3>
          <div className="ri-common-block-tools">
            <Button icon="plus" type="primary" onClick={this.showAdd} className={userClassHide}>新建</Button>
            <Button icon="poweroff" type="ghost" className="refresh-button-style" loading={refreshUserLoading} onClick={this.refreshUser}>{refreshUserText}</Button>
          </div>
          <Table
            dataSource={this.state.currentUsers}
            columns={columns}
            onChange={this.handleUserChange}
            pagination={pagination}
            className="ri-workbench-table-container"
            bordered>
          </Table>
        </div>
        <Modal
          title={userTitle}
          okText="保存"
          wrapClassName="ant-modal-small"
          visible={this.state.formVisible}
          onCancel={this.hideForm}
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
          <UserForm
            type={this.state.formType}
            onInitEmailInputValue={this.onInitEmailInputValue}
            ref={(f) => { this.userForm = f }}
          />
        </Modal>
      </div>
    )
  }
}

User.propTypes = {
  // users: React.PropTypes.oneOfType([
  //   React.PropTypes.array,
  //   React.PropTypes.bool
  // ]),
  // error: React.PropTypes.bool,
  modalLoading: React.PropTypes.bool,
  emailExited: React.PropTypes.bool,
  projectIdGeted: React.PropTypes.string,
  userClassHide: React.PropTypes.string,
  onLoadAdminAllUsers: React.PropTypes.func,
  onLoadUserUsers: React.PropTypes.func,
  onLoadSelectUsers: React.PropTypes.func,
  onAddUser: React.PropTypes.func,
  onEditUser: React.PropTypes.func,
  onLoadEmailInputValue: React.PropTypes.func,
  onLoadUserDetail: React.PropTypes.func,
  onDeleteUser: React.PropTypes.func
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadAdminAllUsers: (resolve) => dispatch(loadAdminAllUsers(resolve)),
    onLoadUserUsers: (projectId, resolve) => dispatch(loadUserUsers(projectId, resolve)),
    onLoadSelectUsers: (projectId, resolve) => dispatch(loadSelectUsers(projectId, resolve)),
    onAddUser: (user, resolve) => dispatch(addUser(user, resolve)),
    onEditUser: (user, resolve) => dispatch(editUser(user, resolve)),
    onLoadEmailInputValue: (value, resolve, reject) => dispatch(loadEmailInputValue(value, resolve, reject)),
    onLoadUserDetail: (userId, resolve) => dispatch(loadUserDetail(userId, resolve)),
    onDeleteUser: (userId, resolve, reject) => dispatch(deleteUser(userId, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  users: selectUsers(),
  error: selectError(),
  modalLoading: selectModalLoading(),
  emailExited: selectEmailExited()
})

export default connect(mapStateToProps, mapDispatchToProps)(User)
