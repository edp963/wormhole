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
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import Table from 'antd/lib/table'
import Input from 'antd/lib/input'
import Button from 'antd/lib/button'

export class ProjectUdfTable extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      selectedRowKeys: [],
      originUdfs: [],
      currentUdfs: [],

      filteredInfo: null,
      sortedInfo: null,

      searchTextFunctionName: '',
      filterDropdownVisibleFunctionName: false,
      searchTextFullClassName: '',
      filterDropdownVisibleFullClassName: false,
      searchTextJarName: '',
      filterDropdownVisibleJarName: false
    }
  }

  componentWillReceiveProps (props) {
    if (props.dataUdf !== []) {
      const originUdfs = props.dataUdf.map(s => {
        s.key = s.id
        s.visible = false
        return s
      })
      this.setState({
        originUdfs: originUdfs.slice(),
        currentUdfs: originUdfs.slice()
      })
    }
  }

  onSelectChange = (selectedRowKeys) => this.setState({ selectedRowKeys })

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
      currentUdfs: this.state.originUdfs.map((record) => {
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

    let { sortedInfo } = this.state
    sortedInfo = sortedInfo || {}

    const pagination = {
      defaultPageSize: 10,
      showSizeChanger: true,
      onShowSizeChange: (current, pageSize) => {
        console.log('Current: ', current, '; PageSize: ', pageSize)
      }
    }

    const columnsProject = [{
      title: 'Function Name',
      dataIndex: 'functionName',
      key: 'functionName',
      sorter: (a, b) => {
        if (typeof a.functionName === 'object') {
          return a.functionNameOrigin < b.functionNameOrigin ? -1 : 1
        } else {
          return a.functionName < b.functionName ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'functionName' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Function Name"
            value={this.state.searchTextFunctionName}
            onChange={this.onInputChange('searchTextFunctionName')}
            onPressEnter={this.onSearch('functionName', 'searchTextFunctionName', 'filterDropdownVisibleFunctionName')}
          />
          <Button type="primary" onClick={this.onSearch('functionName', 'searchTextFunctionName', 'filterDropdownVisibleFunctionName')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleFunctionName,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleFunctionName: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Full Class Name',
      dataIndex: 'fullClassName',
      key: 'fullClassName',
      sorter: (a, b) => {
        if (typeof a.fullClassName === 'object') {
          return a.fullClassNameOrigin < b.fullClassNameOrigin ? -1 : 1
        } else {
          return a.fullClassName < b.fullClassName ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'fullClassName' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Full Class Name"
            value={this.state.searchTextFullClassName}
            onChange={this.onInputChange('searchTextFullClassName')}
            onPressEnter={this.onSearch('fullClassName', 'searchTextFullClassName', 'filterDropdownVisibleFullClassName')}
          />
          <Button type="primary" onClick={this.onSearch('fullClassName', 'searchTextFullClassName', 'filterDropdownVisibleFullClassName')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleFullClassName,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleFullClassName: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Jar Name',
      dataIndex: 'jarName',
      key: 'jarName',
      sorter: (a, b) => {
        if (typeof a.jarName === 'object') {
          return a.jarNameOrigin < b.jarNameOrigin ? -1 : 1
        } else {
          return a.jarName < b.jarName ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'jarName' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Jar Name"
            value={this.state.searchTextJarName}
            onChange={this.onInputChange('searchTextJarName')}
            onPressEnter={this.onSearch('jarName', 'searchTextJarName', 'filterDropdownVisibleJarName')}
          />
          <Button type="primary" onClick={this.onSearch('jarName', 'searchTextJarName', 'filterDropdownVisibleJarName')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleJarName,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleJarName: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Stream Type',
      dataIndex: 'streamType',
      key: 'streamType',
      // className: 'text-align-center',
      sorter: (a, b) => a.streamType < b.streamType ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'streamType' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Stream Type"
            value={this.state.searchTextStreamType}
            onChange={this.onInputChange('searchTextStreamType')}
            onPressEnter={this.onSearch('streamType', 'searchTextStreamType', 'filterDropdownVisibleStreamType')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('streamType', 'searchTextStreamType', 'filterDropdownVisibleStreamType')}
          >Search
          </Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleStreamType,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleStreamType: visible
      }, () => this.searchInput.focus())
    },
    {
      title: 'Map Or Agg',
      dataIndex: 'mapOrAgg',
      key: 'mapOrAgg',
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="map or agg"
            value={this.state.searchTextMapOrAgg}
            onChange={this.onInputChange('searchTextMapOrAgg')}
            onPressEnter={this.onSearch('mapOrAgg', 'searchTextMapOrAgg', 'filterDropdownVisibleMapOrAgg')}
          />
          <Button
            type="primary"
            onClick={this.onSearch('mapOrAgg', 'searchTextMapOrAgg', 'filterDropdownVisibleMapOrAgg')}
          >Search
          </Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleMapOrAgg,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleMapOrAgg: visible
      }, () => this.searchInput.focus())
    }]

    return (
      <Table
        bordered
        title={() => (<h3><FormattedMessage {...messages.projectUdfAuthority} /></h3>)}
        columns={columnsProject}
        dataSource={this.state.currentUdfs}
        pagination={pagination}
        rowSelection={rowSelection}
        onChange={this.handleChange}
      />
    )
  }
}

ProjectUdfTable.propTypes = {
  // dataUdfs: React.PropTypes.oneOfType([
  //   React.PropTypes.array,
  //   React.PropTypes.bool
  // ])
}

export default ProjectUdfTable
