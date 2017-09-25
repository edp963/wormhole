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

import Table from 'antd/lib/table'
import Input from 'antd/lib/input'
import Button from 'antd/lib/button'

export class ProjectUsersTable extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      selectedRowKeys: [],
      originUsers: [],
      currentUsers: [],

      filteredInfo: null,
      sortedInfo: null,

      searchTextEmail: '',
      filterDropdownVisibleEmail: false,
      searchTextName: '',
      filterDropdownVisibleName: false
    }
  }

  componentWillReceiveProps (props) {
    if (props.dataUsers !== []) {
      const originUsers = props.dataUsers.filter(s => s.roleType === 'user' || s.roleType === 'app')
        .map(s => {
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

  onSelectChange = (selectedRowKeys) => {
    this.setState({ selectedRowKeys })
  }

  handleChange = (pagination, filters, sorter) => {
    this.setState({
      filteredInfo: filters,
      sortedInfo: sorter
    })
  }

  onInputChange = (value) => (e) => this.setState({ [value]: e.target.value })

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      [visible]: false,
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
  }

  render () {
    const { selectedRowKeys } = this.state
    const rowSelection = {
      selectedRowKeys,
      onChange: this.onSelectChange,
      onShowSizeChange: this.onShowSizeChange
    }

    let { sortedInfo, filteredInfo } = this.state
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const pagination = {
      defaultPageSize: 10,
      showSizeChanger: true,
      onShowSizeChange: (current, pageSize) => {
        console.log('Current: ', current, '; PageSize: ', pageSize)
      }
    }

    const columnsProject = [{
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
            value={this.state.searchTextEmail}
            onChange={this.onInputChange('searchTextEmail')}
            onPressEnter={this.onSearch('email', 'searchTextEmail', 'filterDropdownVisibleEmail')}
          />
          <Button type="primary" onClick={this.onSearch('email', 'searchTextEmail', 'filterDropdownVisibleEmail')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleEmail,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleEmail: visible
      }, () => this.searchInput.focus())
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
            value={this.state.searchTextName}
            onChange={this.onInputChange('searchTextName')}
            onPressEnter={this.onSearch('name', 'searchTextName', 'filterDropdownVisibleName')}
          />
          <Button type="primary" onClick={this.onSearch('name', 'searchTextName', 'filterDropdownVisibleName')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleName,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleName: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Role Type',
      dataIndex: 'roleType',
      key: 'roleType',
      className: 'text-align-center',
      sorter: (a, b) => a.roleType < b.roleType ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'roleType' && sortedInfo.order,
      filters: [
        {text: 'user', value: 'user'},
        {text: 'app', value: 'app'}
      ],
      filteredValue: filteredInfo.roleType,
      onFilter: (value, record) => record.roleType.includes(value)
    }]

    return (
      <Table
        bordered
        title={() => (<h3 className="required-style">用户权限</h3>)}
        columns={columnsProject}
        dataSource={this.state.currentUsers}
        pagination={pagination}
        rowSelection={rowSelection}
        onChange={this.handleChange}
      />
    )
  }
}

ProjectUsersTable.propTypes = {
  // dataUsers: React.PropTypes.oneOfType([
  //   React.PropTypes.array,
  //   React.PropTypes.bool
  // ])
}

export default ProjectUsersTable
