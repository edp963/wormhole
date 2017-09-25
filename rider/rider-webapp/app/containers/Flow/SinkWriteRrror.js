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
import Button from 'antd/lib/button'
import Input from 'antd/lib/input'
import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { selectFlows } from './selectors'

export class SinkWriteRrror extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      originFlows: [],
      currentFlows: [],

      filteredInfo: null,
      sortedInfo: null,

      startTimeText: '',
      endTimeText: '',
      actionStartTimeText: '',
      actionEndTimeText: '',
      whProcessedStartTimeText: '',
      whProcessedEndTimeText: '',
      filterDatepickerShown: false,
      filterDropdownVisibleActionEventTime: false,
      filterDropdownVisibleWhProcessedTime: false
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
      startTimeText: dateString[0],
      endTimeText: dateString[1]
    })
  }

  onRangeTimeSearch = (columnName, startTimeText, endTimeText, visible) => () => {
    const startTime = (new Date(this.state.startTimeText)).getTime()
    const endTime = (new Date(this.state.endTimeText)).getTime()

    let actionOrWhprocess = ''
    if (columnName === 'actionEventTime') {
      actionOrWhprocess = startTime || endTime ? { actionEventTime: [0] } : { actionEventTime: [] }
    } else if (columnName === 'whProcessedTime') {
      actionOrWhprocess = startTime || endTime ? { whProcessedTime: [0] } : { whProcessedTime: [] }
    }

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
      filteredInfo: actionOrWhprocess
    })
  }

  handleEndOpenChange = (status) => {
    this.setState({
      filterDatepickerShown: status
    })
  }

  handleSinkWriteErrorChange = (pagination, filters, sorter) => {
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

    const columnsSource2Log = [{
      title: 'Id',
      dataIndex: 'id',
      key: 'id',
      sorter: (a, b) => a.id - b.id,
      sortOrder: sortedInfo.columnKey === 'id' && sortedInfo.order,
      filteredValue: filteredInfo.id,
      filterDropdown: (
        <div className="custom-filter-dropdown custom-filter-dropdown-ps">
          <Form>
            <Row>
              <Col span={9}>
                <Input
                  ref={ele => { this.searchInput = ele }}
                  placeholder="Start ID"
                  onChange={this.onInputChange('searchStartIdText')}
                />
              </Col>
              <Col span={1}>
                <p className="ant-form-split">-</p>
              </Col>
              <Col span={9}>
                <Input
                  placeholder="End ID"
                  onChange={this.onInputChange('searchEndIdText')}
                />
              </Col>
              <Col span={5} className="text-align-center">
                <Button type="primary" onClick={this.onRangeIdSearch('id', 'searchStartIdText', 'searchEndIdText', 'filterDropdownVisibleId')}>Search</Button>
              </Col>
            </Row>
          </Form>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleId,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleId: visible
      }, () => this.searchInput.focus())
    }, {
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
      title: 'Sink Primary key',
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
            placeholder="Sink Primary key"
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
      title: 'UMS ID',
      dataIndex: 'umsID',
      key: 'umsID',
      sorter: (a, b) => {
        if (typeof a.umsID === 'object') {
          return a.umsIDOrigin < b.umsIDOrigin ? -1 : 1
        } else {
          return a.umsID < b.umsID ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'umsID' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="UMS ID"
            value={this.state.searchUmsID}
            onChange={this.onInputChange('searchUmsID')}
            onPressEnter={this.onSearch('umsID', 'searchUmsID', 'filterDropdownVisibleUmsID')}
          />
          <Button type="primary" onClick={this.onSearch('umsID', 'searchUmsID', 'filterDropdownVisibleUmsID')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleUmsID,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleUmsID: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'UMS Operation',
      dataIndex: 'umsOP',
      key: 'umsOP',
      sorter: (a, b) => {
        if (typeof a.umsOP === 'object') {
          return a.umsOPOrigin < b.umsOPOrigin ? -1 : 1
        } else {
          return a.umsOP < b.umsOP ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'umsOP' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="UMS Operation"
            value={this.state.searchUmssearchUmsOperation}
            onChange={this.onInputChange('searchUmssearchUmsOperation')}
            onPressEnter={this.onSearch('umsOP', 'searchUmssearchUmsOperation', 'filterDropdownVisibleUmsOperation')}
          />
          <Button type="primary" onClick={this.onSearch('umsOP', 'searchUmssearchUmsOperation', 'filterDropdownVisibleUmsOperation')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleUmsOperation,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleUmsOperation: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Event Time',
      dataIndex: 'actionEventTime',
      key: 'actionEventTime',
      sorter: (a, b) => {
        if (typeof a.actionEventTime === 'object') {
          return a.actionEventTimeOrigin < b.actionEventTimeOrigin ? -1 : 1
        } else {
          return a.actionEventTime < b.actionEventTime ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'actionEventTime' && sortedInfo.order,
      filteredValue: filteredInfo.actionEventTime,
      filterDropdown: (
        <div className="custom-filter-dropdown-style">
          <RangePicker
            showTime
            format="YYYY-MM-DD HH:mm:ss"
            placeholder={['Start Time', 'End Time']}
            onOpenChange={this.handleEndOpenChange}
            onChange={this.onRangeTimeChange}
            onPressEnter={this.onRangeTimeSearch('actionEventTime', 'actionStartTimeText', 'actionEndTimeText', 'filterDropdownVisibleActionEventTime')}
          />
          <Button type="primary" className="rangeFilter" onClick={this.onRangeTimeSearch('actionEventTime', 'actionStartTimeText', 'actionEndTimeText', 'filterDropdownVisibleActionEventTime')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleActionEventTime,
      onFilterDropdownVisibleChange: visible => {
        if (!this.state.filterDatepickerShown) {
          this.setState({ filterDropdownVisibleActionEventTime: visible })
        }
      }
    }, {
      title: 'Processed Time',
      dataIndex: 'whProcessedTime',
      key: 'whProcessedTime',
      sorter: (a, b) => {
        if (typeof a.whProcessedTime === 'object') {
          return a.whProcessedTimeOrigin < b.whProcessedTimeOrigin ? -1 : 1
        } else {
          return a.whProcessedTime < b.whProcessedTime ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'whProcessedTime' && sortedInfo.order,
      filteredValue: filteredInfo.whProcessedTime,
      filterDropdown: (
        <div className="custom-filter-dropdown-style">
          <RangePicker
            showTime
            format="YYYY-MM-DD HH:mm:ss"
            placeholder={['Start Time', 'End Time']}
            onOpenChange={this.handleEndOpenChange}
            onChange={this.onRangeTimeChange}
            onPressEnter={this.onRangeTimeSearch('whProcessedTime', 'whProcessedStartTimeText', 'whProcessedEndTimeText', 'filterDropdownVisibleWhProcessedTime')}
          />
          <Button type="primary" className="rangeFilter" onClick={this.onRangeTimeSearch('whProcessedTime', 'whProcessedStartTimeText', 'whProcessedEndTimeText', 'filterDropdownVisibleWhProcessedTime')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleWhProcessedTime,
      onFilterDropdownVisibleChange: visible => {
        if (!this.state.filterDatepickerShown) {
          this.setState({ filterDropdownVisibleWhProcessedTime: visible })
        }
      }
    }, {
      title: 'Already Auto Retry Times',
      dataIndex: 'alreadyRetryTimes',
      key: 'alreadyRetryTimes',
      sorter: (a, b) => {
        if (typeof a.alreadyRetryTimes === 'object') {
          return a.alreadyRetryTimesOrigin < b.alreadyRetryTimesOrigin ? -1 : 1
        } else {
          return a.alreadyRetryTimes < b.alreadyRetryTimes ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'alreadyRetryTimes' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Already Auto Retry Times"
            value={this.state.searchAlreadyRetryTimes}
            onChange={this.onInputChange('searchAlreadyRetryTimes')}
            onPressEnter={this.onSearch('alreadyRetryTimes', 'searchAlreadyRetryTimes', 'filterDropdownVisibleAlreadyRetryTimes')}
          />
          <Button type="primary" onClick={this.onSearch('alreadyRetryTimes', 'searchAlreadyRetryTimes', 'filterDropdownVisibleAlreadyRetryTimes')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleAlreadyRetryTimes,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleAlreadyRetryTimes: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Manage Status',
      dataIndex: 'manageStatus',
      key: 'manageStatus'
    }]

    return (
      <Table
        columns={columnsSource2Log}
        dataSource={this.state.currentFlows || []}
        pagination={pagination}
        onChange={this.handleSinkWriteErrorChange}
      />
    )
  }
}

SinkWriteRrror.propTypes = {
  pageSize: React.PropTypes.number,
  total: React.PropTypes.number,
  onChange: React.PropTypes.func
}

const mapStateToProps = createStructuredSelector({
  flows: selectFlows()
})

export default connect(mapStateToProps, null)(SinkWriteRrror)
