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
import { createStructuredSelector } from 'reselect'
import { connect } from 'react-redux'

import Table from 'antd/lib/table'
import Popover from 'antd/lib/popover'
import Input from 'antd/lib/input'
import Button from 'antd/lib/button'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { selectFlows } from './selectors'

export class Source2LogTable extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      originFlows: [],
      currentFlows: [],

      filteredInfo: null,
      sortedInfo: null,

      searchNamespace: '',
      filterDropdownVisibleNamespace: false,
      startTimeText: '',
      endTimeText: '',
      filterDatepickerShown: false,
      filterDropdownVisibleRecordedTime: false,
      searchPrimaryKey: '',
      filterDropdownVisiblePrimaryKey: false,
      searchDiffResultType: '',
      filterDropdownVisibleDiffResultType: false,
      searchCompareScale: '',
      filterDropdownVisibleCompareScale: false
    }
  }

  componentWillMount () {
    this.initialData(this.props)
  }

  componentWillReceiveProps (props) {
    this.initialData(props)
  }

  initialData = (props) => {
    if (props.data) {
      const originFlows = props.data.map(s => {
        s.key = s.id
        s.visible = false
        return s
      })
      this.setState({
        originFlows: originFlows.slice(),
        currentFlows: originFlows.slice()
      })
    }
  }

  onInputChange = (value) => (e) => this.setState({ [value]: e.target.value })

  onRangeTimeChange = (value, dateString) => {
    this.setState({
      startTimeText: dateString[0],
      endTimeText: dateString[1]
    })
  }

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      [visible]: false,
      currentFlows: this.state.originFlows.map((record) => {
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

  onRangeTimeSearch = (columnName, startTimeText, endTimeText, visible) => () => {
    const startTime = (new Date(this.state.startTimeText)).getTime()
    const endTime = (new Date(this.state.endTimeText)).getTime()

    this.setState({
      [visible]: false,
      currentFlows: this.state.originFlows.map((record) => {
        const match = (new Date(record[columnName])).getTime()
        if ((match < startTime) || (match > endTime)) {
          return null
        }
        return record
      }).filter(record => !!record),
      filteredInfo: startTime || endTime ? { recordedTime: [0] } : { recordedTime: [] }
    })
  }

  handleEndOpenChange = (status) => {
    this.setState({
      filterDatepickerShown: status
    })
  }

  handleCompareDiffChange = (pagination, filters, sorter) => {
    this.setState({
      filteredInfo: filters,
      sortedInfo: sorter
    })
  }

  onRangeIdSearch = (columnName, startText, endText, visible) => () => {
    this.setState({
      [visible]: false,
      currentFlows: this.state.originFlows.map((record) => {
        const match = record[columnName]
        if ((match < parseInt(this.state[startText])) || (match > parseInt(this.state[endText]))) {
          return null
        }
        return record
      }).filter(record => !!record),
      filteredInfo: this.state[startText] || this.state[endText] ? { id: [0] } : { id: [] }
    })
  }

  render () {
    const { pageSize, total, onChange } = this.props
    const pagination = {
      defaultPageSize: pageSize,
      showSizeChanger: true,
      total: total,
      onShowSizeChange: (current, pageSize) => {
        onChange(current, pageSize)
      },
      onChange: (current) => {
        onChange(current, pageSize)
      }
    }

    let { sortedInfo, filteredInfo } = this.state
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const columnsCompareDiff = [{
      title: 'Namespace',
      dataIndex: 'namespace',
      key: 'namespace',
      sorter: (a, b) => {
        if (typeof a.namespace === 'object') {
          return a.namespaceOrigin < b.namespaceOrigin ? -1 : 1
        } else {
          return a.namespace < b.namespace ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'namespace' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Namespace"
            value={this.state.searchNamespace}
            onChange={this.onInputChange('searchNamespace')}
            onPressEnter={this.onSearch('namespace', 'searchNamespace', 'filterDropdownVisibleNamespace')}
          />
          <Button type="primary" onClick={this.onSearch('namespace', 'searchNamespace', 'filterDropdownVisibleNamespace')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleNamespace,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleNamespace: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Primary Key',
      dataIndex: 'primaryKey',
      key: 'primaryKey',
      sorter: (a, b) => {
        if (typeof a.primaryKey === 'object') {
          return a.primaryKeyOrigin < b.primaryKeyOrigin ? -1 : 1
        } else {
          return a.primaryKey < b.primaryKey ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'primaryKey' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Primary Key"
            value={this.state.searchPrimaryKey}
            onChange={this.onInputChange('searchPrimaryKey')}
            onPressEnter={this.onSearch('primaryKey', 'searchPrimaryKey', 'filterDropdownVisiblePrimaryKey')}
          />
          <Button type="primary" onClick={this.onSearch('primaryKey', 'searchPrimaryKey', 'filterDropdownVisiblePrimaryKey')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisiblePrimaryKey,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisiblePrimaryKey: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Diff Result Type',
      dataIndex: 'diffResultType',
      key: 'diffResultType',
      sorter: (a, b) => {
        if (typeof a.diffResultType === 'object') {
          return a.diffResultTypeOrigin < b.diffResultTypeOrigin ? -1 : 1
        } else {
          return a.diffResultType < b.diffResultType ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'diffResultType' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Diff Result Type"
            value={this.state.searchDiffResultType}
            onChange={this.onInputChange('searchDiffResultType')}
            onPressEnter={this.onSearch('diffResultType', 'searchDiffResultType', 'filterDropdownVisibleDiffResultType')}
          />
          <Button type="primary" onClick={this.onSearch('diffResultType', 'searchDiffResultType', 'filterDropdownVisibleDiffResultType')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleDiffResultType,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleDiffResultType: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Compare Scale',
      dataIndex: 'compareScale',
      key: 'compareScale',
      sorter: (a, b) => {
        if (typeof a.compareScale === 'object') {
          return a.compareScaleOrigin < b.compareScaleOrigin ? -1 : 1
        } else {
          return a.compareScale < b.compareScale ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'compareScale' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Compare Scale"
            value={this.state.searchCompareScale}
            onChange={this.onInputChange('searchCompareScale')}
            onPressEnter={this.onSearch('compareScale', 'searchCompareScale', 'filterDropdownVisibleCompareScale')}
          />
          <Button type="primary" onClick={this.onSearch('compareScale', 'searchCompareScale', 'filterDropdownVisibleCompareScale')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleCompareScale,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleCompareScale: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Recorded Time',
      dataIndex: 'recordedTime',
      key: 'recordedTime',
      sorter: (a, b) => {
        if (typeof a.recordedTime === 'object') {
          return a.recordedTimeOrigin < b.recordedTimeOrigin ? -1 : 1
        } else {
          return a.recordedTime < b.recordedTime ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'recordedTime' && sortedInfo.order,
      filteredValue: filteredInfo.recordedTime,
      filterDropdown: (
        <div className="custom-filter-dropdown-style">
          <RangePicker
            showTime
            format="YYYY-MM-DD HH:mm:ss"
            placeholder={['Start Time', 'End Time']}
            onOpenChange={this.handleEndOpenChange}
            onChange={this.onRangeTimeChange}
            onPressEnter={this.onRangeTimeSearch('recordedTime', 'startTimeText', 'endTimeText', 'filterDropdownVisibleRecordedTime')}
          />
          <Button type="primary" className="rangeFilter" onClick={this.onRangeTimeSearch('recordedTime', 'startTimeText', 'endTimeText', 'filterDropdownVisibleRecordedTime')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleRecordedTime,
      onFilterDropdownVisibleChange: visible => {
        if (!this.state.filterDatepickerShown) {
          this.setState({ filterDropdownVisibleRecordedTime: visible })
        }
      }
    }, {
      title: 'Action',
      key: 'actionCompareDiff',
      className: 'text-align-center',
      render: (text, record) => (
        <span className="ant-table-action-column">
          <Popover
            placement="left"
            title={<h3>RowDatas</h3>}
            content={<div className="content-style">
              <p><strong>source data：</strong><span>{record.sourceRowDatas}</span></p>
              <p><strong>sink data：</strong><span>{record.sinkRowDatas}</span></p>
            </div>}
            trigger="click">
            <Button>RowDatas</Button>
          </Popover>
        </span>
      )
    }]

    return (
      <Table
        columns={columnsCompareDiff}
        dataSource={this.state.currentFlows || []}
        pagination={pagination}
        onChange={this.handleCompareDiffChange}
      />
    )
  }
}

Source2LogTable.propTypes = {
  pageSize: React.PropTypes.number,
  total: React.PropTypes.number,
  onChange: React.PropTypes.func
}

const mapStateToProps = createStructuredSelector({
  flows: selectFlows()
})

export default connect(mapStateToProps, null)(Source2LogTable)
