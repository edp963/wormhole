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
import Button from 'antd/lib/button'
import Input from 'antd/lib/input'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { selectFlows } from './selectors'

export class Source2SinkTable extends React.Component {
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
      sinkStartTimeText: '',
      sinkEndTimeText: '',
      filterDatepickerShown: false,
      searchRecordedTime: '',
      filterDropdownVisibleSinkRecordedTime: false,
      searchSourceNamespace: '',
      filterDropdownVisibleSourceNamespace: false,
      searchSinkNamespace: '',
      filterDropdownVisibleSinkNamespace: false,
      searchUniqueKey: '',
      filterDropdownVisibleUniqueKey: false
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

  onRangeTimeChange = (value, dateString) => {
    this.setState({
      sinkStartTimeText: dateString[0],
      sinkEndTimeText: dateString[1]
    })
  }

  onRangeTimeSearch = (columnName, sinkStartTimeText, sinkEndTimeText, visible) => () => {
    const startTime = (new Date(this.state.sinkStartTimeText)).getTime()
    const endTime = (new Date(this.state.sinkEndTimeText)).getTime()
    this.setState({
      [visible]: false,
      currentFlows: this.state.originFlows.map((record) => {
        const match = (new Date(record[columnName])).getTime()
        if ((match < startTime) || (match > endTime)) {
          return null
        }
        return {
          ...record,
          [columnName]: (
            <span>{record[columnName]}</span>
          )
        }
      }).filter(record => !!record),
      filteredInfo: startTime || endTime ? { recordedTime: [0] } : { recordedTime: [] }
    })
  }

  handleEndOpenChange = (status) => {
    this.setState({
      filterDatepickerShown: status
    })
  }

  onInputChange = (value) => (e) => this.setState({ [value]: e.target.value })

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

  handleSinkTableChange = (pagination, filters, sorter) => {
    this.setState({
      filteredInfo: filters,
      sortedInfo: sorter
    })
  }

  render () {
    const { pageSize, total, onChange } = this.props

    let { sortedInfo, filteredInfo } = this.state
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

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

    const columnsSource2Sink = [{
      title: 'Source Namespace',
      dataIndex: 'sourceNamespace',
      key: 'sourceNamespace',
      sorter: (a, b) => {
        if (typeof a.sourceNamespace === 'object') {
          return a.sourceNamespaceOrigin < b.sourceNamespaceOrigin ? -1 : 1
        } else {
          return a.sourceNamespace < b.sourceNamespace ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'sourceNamespace' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Source Namespace"
            value={this.state.searchSourceNamespace}
            onChange={this.onInputChange('searchSourceNamespace')}
            onPressEnter={this.onSearch('sourceNamespace', 'searchSourceNamespace', 'filterDropdownVisibleSourceNamespace')}
          />
          <Button type="primary" onClick={this.onSearch('sourceNamespace', 'searchSourceNamespace', 'filterDropdownVisibleSourceNamespace')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleSourceNamespace,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleSourceNamespace: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Sink Namespace',
      dataIndex: 'sinkNamespace',
      key: 'sinkNamespace',
      sorter: (a, b) => {
        if (typeof a.sinkNamespace === 'object') {
          return a.sinkNamespaceOrigin < b.sinkNamespaceOrigin ? -1 : 1
        } else {
          return a.sinkNamespace < b.sinkNamespace ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'sinkNamespace' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Sink Namespace"
            value={this.state.searchSinkNamespace}
            onChange={this.onInputChange('searchSinkNamespace')}
            onPressEnter={this.onSearch('sinkNamespace', 'searchSinkNamespace', 'filterDropdownVisibleSinkNamespace')}
          />
          <Button type="primary" onClick={this.onSearch('sinkNamespace', 'searchSinkNamespace', 'filterDropdownVisibleSinkNamespace')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleSinkNamespace,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleSinkNamespace: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Unique Key',
      dataIndex: 'uniqueKey',
      key: 'uniqueKey',
      sorter: (a, b) => {
        if (typeof a.uniqueKey === 'object') {
          return a.uniqueKeyOrigin < b.uniqueKeyOrigin ? -1 : 1
        } else {
          return a.uniqueKey < b.uniqueKey ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'uniqueKey' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Unique Key"
            value={this.state.searchUniqueKey}
            onChange={this.onInputChange('searchUniqueKey')}
            onPressEnter={this.onSearch('uniqueKey', 'searchUniqueKey', 'filterDropdownVisibleUniqueKey')}
          />
          <Button type="primary" onClick={this.onSearch('uniqueKey', 'searchUniqueKey', 'filterDropdownVisibleUniqueKey')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleUniqueKey,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleUniqueKey: visible
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
            onPressEnter={this.onRangeTimeSearch('recordedTime', 'sinkStartTimeText', 'sinkEndTimeText', 'filterDropdownVisibleSinkRecordedTime')}
          />
          <Button type="primary" className="rangeFilter" onClick={this.onRangeTimeSearch('recordedTime', 'sinkStartTimeText', 'sinkEndTimeText', 'filterDropdownVisibleSinkRecordedTime')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleSinkRecordedTime,
      onFilterDropdownVisibleChange: visible => {
        if (!this.state.filterDatepickerShown) {
          this.setState({ filterDropdownVisibleSinkRecordedTime: visible })
        }
      }
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
      title: 'Manage Status',
      dataIndex: 'manageStatus',
      key: 'manageStatus'
    }, {
      title: 'Action',
      key: 'actionAutoConfig',
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
      )}
    ]

    return (
      <Table
        columns={columnsSource2Sink}
        dataSource={this.state.currentFlows || []}
        pagination={pagination}
        onChange={this.handleSinkTableChange}
      />
    )
  }
}

Source2SinkTable.propTypes = {
  pageSize: React.PropTypes.number,
  total: React.PropTypes.number,
  onChange: React.PropTypes.func
}

const mapStateToProps = createStructuredSelector({
  flows: selectFlows()
})

export default connect(mapStateToProps, null)(Source2SinkTable)
