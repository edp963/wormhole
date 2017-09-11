/*-
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

export class ProjectNSTable extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      originNameSpace: [],
      currentNameSpace: [],

      filteredInfo: null,
      sortedInfo: null,

      searchTextPermission: '',
      filterDropdownVisiblePermission: false,
      searchTextProjectNs: '',
      filterDropdownVisibleProjectNs: false
    }
  }

  componentWillReceiveProps (props) {
    if (props.dataNameSpace !== []) {
      const originNameSpace = props.dataNameSpace.map(s => {
        const projectNs = [s.nsSys, s.nsInstance, s.nsDatabase, s.nsTable].join('.')
        const projectNamespaceTbale = {
          active: s.active,
          createBy: s.createBy,
          createTime: s.createTime,
          id: s.id,
          key: s.key,
          keys: s.keys,
          nsDatabaseId: s.nsDatabaseId,
          nsDbpar: s.nsDbpar,
          nsInstanceId: s.nsInstanceId,
          nsTablepar: s.nsTablepar,
          nsVersion: s.nsVersion,
          permission: s.permission,
          projectName: s.projectName,
          updateBy: s.updateBy,
          updateTime: s.updateTime,
          visible: s.visible,
          projectNs: projectNs
        }

        projectNamespaceTbale.key = projectNamespaceTbale.id
        projectNamespaceTbale.visible = false
        return projectNamespaceTbale
      })
      this.setState({
        originNameSpace: originNameSpace.slice(),
        currentNameSpace: originNameSpace.slice(),

        title: true,
        showHeader: true
      })
    }
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
      currentNameSpace: this.state.originNameSpace.map((record) => {
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

  onChangeAllSelect = () => {
    this.props.onInitSwitch(this.state.currentNameSpace)
  }

  onSelectChange = (selectedRowKeys) => {
    this.props.initSelectedRowKeys(selectedRowKeys)
  }

  render () {
    const { selectedRowKeys } = this.props
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
      title: 'Namespace',
      dataIndex: 'projectNs',
      key: 'projectNs',
      sorter: (a, b) => {
        if (typeof a.projectNs === 'object') {
          return a.projectNsOrigin < b.projectNsOrigin ? -1 : 1
        } else {
          return a.projectNs < b.projectNs ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'projectNs' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            placeholder="Namespace"
            value={this.state.searchTextProjectNs}
            onChange={this.onInputChange('searchTextProjectNs')}
            onPressEnter={this.onSearch('projectNs', 'searchTextProjectNs', 'filterDropdownVisibleProjectNs')}
          />
          <Button type="primary" onClick={this.onSearch('projectNs', 'searchTextProjectNs', 'filterDropdownVisibleProjectNs')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleProjectNs,
      onFilterDropdownVisibleChange: visible => this.setState({ filterDropdownVisibleProjectNs: visible })
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
    }]

    const { selectIcon, selectType } = this.props

    return (
      <Table
        bordered
        title={() => (<div className="required-style"><span className="project-ns-h3">Namespace 权限</span>
          <span className="project-ns-switch">
            <Button icon={selectIcon} type={selectType} onClick={this.onChangeAllSelect} size="small">全选</Button>
          </span>
        </div>)}
        columns={columnsProject}
        dataSource={this.state.currentNameSpace}
        pagination={pagination}
        rowSelection={rowSelection}
        onChange={this.handleChange}
      />
    )
  }
}

ProjectNSTable.propTypes = {
  // dataNameSpace: React.PropTypes.oneOfType([
  //   React.PropTypes.array,
  //   React.PropTypes.bool
  // ])
  initSelectedRowKeys: React.PropTypes.func,
  onInitSwitch: React.PropTypes.func,
  selectedRowKeys: React.PropTypes.array,
  selectType: React.PropTypes.string,
  selectIcon: React.PropTypes.string
}

export default ProjectNSTable
