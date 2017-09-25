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
import { uuid } from '../../utils/util'

import Icon from 'antd/lib/icon'
import Table from 'antd/lib/table'
import Form from 'antd/lib/form'
import Input from 'antd/lib/input'
import Button from 'antd/lib/button'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'

import { loadResources } from './action'
import { selectResources } from './selectors'

export class Resource extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      visible: false,
      originResources: [],
      currentResources: [],

      searchResourceName: '',
      filterDropdownVisibleResourceName: false,

      searchStartDCText: '',
      searchEndDCText: '',
      filterDropdownVisibleDC: false,
      searchStartPECText: '',
      searchEndPECText: '',
      filterDropdownVisiblePEC: false,
      searchStartDMText: '',
      searchEndDMText: '',
      filterDropdownVisibleDM: false,
      searchStartPEMText: '',
      searchEndPEMText: '',
      filterDropdownVisiblePEM: false,
      searchStartENText: '',
      searchEndENText: '',
      filterDropdownVisibleEN: false
    }
  }

  componentWillMount () {
    if (localStorage.getItem('loginRoleType') === 'admin') {
      this.props.onLoadResources(this.props.projectIdGeted, 'admin')
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      this.props.onLoadResources(this.props.projectIdGeted, 'user')
    }
  }

  componentWillReceiveProps (props) {
    const resourceStream = props.resources.stream
    if (props.resources) {
      const originResources = resourceStream.map(s => {
        s.key = uuid()
        s.visible = false
        return s
      })
      this.setState({
        originResources: originResources.slice(),
        currentResources: originResources.slice()
      })
    }
  }

  onInputChange = (value) => (e) => this.setState({ [value]: e.target.value })

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      [visible]: false,
      currentResources: this.state.originResources.map((record) => {
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

  handleUserChange = (pagination, filters, sorter) => {
    this.setState({
      filteredInfo: filters,
      sortedInfo: sorter
    })
  }

  onRangeNumSearch = (columnName, startText, endText, visible) => () => {
    let filteredInfoFinal = ''
    if (columnName === 'driverCores') {
      filteredInfoFinal = this.state[startText] || this.state[endText] ? {driverCores: [0]} : {driverCores: []}
    } else if (columnName === 'perExecutorCores') {
      filteredInfoFinal = this.state[startText] || this.state[endText] ? {perExecutorCores: [0]} : {perExecutorCores: []}
    } else if (columnName === 'driverMemory') {
      filteredInfoFinal = this.state[startText] || this.state[endText] ? {driverMemory: [0]} : {driverMemory: []}
    } else if (columnName === 'perExecutorMemory') {
      filteredInfoFinal = this.state[startText] || this.state[endText] ? {perExecutorMemory: [0]} : {perExecutorMemory: []}
    } else if (columnName === 'executorNums') {
      filteredInfoFinal = this.state[startText] || this.state[endText] ? {executorNums: [0]} : {executorNums: []}
    }

    this.setState({
      [visible]: false,
      currentResources: this.state.originResources.map((record) => {
        const match = record[columnName]
        if ((match < parseInt(this.state[startText])) || (match > parseInt(this.state[endText]))) {
          return null
        }
        return record
      }).filter(record => !!record),
      filteredInfo: filteredInfoFinal
    })
  }

  render () {
    const { resources } = this.props
    let { sortedInfo, filteredInfo } = this.state
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const columns = [
      {
        title: 'Stream Name',
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
              placeholder="Stream Name"
              value={this.state.searchResourceName}
              onChange={this.onInputChange('searchResourceName')}
              onPressEnter={this.onSearch('name', 'searchResourceName', 'filterDropdownVisibleResourceName')}
            />
            <Button type="primary" onClick={this.onSearch('name', 'searchResourceName', 'filterDropdownVisibleResourceName')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleResourceName,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisibleResourceName: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Driver Cores',
        dataIndex: 'driverCores',
        key: 'driverCores',
        sorter: (a, b) => a.driverCores - b.driverCores,
        sortOrder: sortedInfo.columnKey === 'driverCores' && sortedInfo.order,
        filteredValue: filteredInfo.driverCores,
        filterDropdown: (
          <div className="custom-filter-dropdown custom-filter-dropdown-ps">
            <Form>
              <Row>
                <Col span={9}>
                  <Input
                    ref={ele => { this.searchInput = ele }}
                    placeholder="Start"
                    onChange={this.onInputChange('searchStartDCText')}
                  />
                </Col>
                <Col span={1}>
                  <p className="ant-form-split">-</p>
                </Col>
                <Col span={9}>
                  <Input
                    placeholder="End"
                    onChange={this.onInputChange('searchEndDCText')}
                  />
                </Col>
                <Col span={5} className="text-align-center">
                  <Button type="primary" onClick={this.onRangeNumSearch('driverCores', 'searchStartDCText', 'searchEndDCText', 'filterDropdownVisibleDC')}>Search</Button>
                </Col>
              </Row>
            </Form>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleDC,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisibleDC: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Driver Memory',
        dataIndex: 'driverMemory',
        key: 'driverMemory',
        sorter: (a, b) => a.driverMemory - b.driverMemory,
        sortOrder: sortedInfo.columnKey === 'driverMemory' && sortedInfo.order,
        filteredValue: filteredInfo.driverMemory,
        filterDropdown: (
          <div className="custom-filter-dropdown custom-filter-dropdown-ps">
            <Form>
              <Row>
                <Col span={9}>
                  <Input
                    ref={ele => { this.searchInput = ele }}
                    placeholder="Start"
                    onChange={this.onInputChange('searchStartDMText')}
                  />
                </Col>
                <Col span={1}>
                  <p className="ant-form-split">-</p>
                </Col>
                <Col span={9}>
                  <Input
                    placeholder="End"
                    onChange={this.onInputChange('searchEndDMText')}
                  />
                </Col>
                <Col span={5} className="text-align-center">
                  <Button type="primary" onClick={this.onRangeNumSearch('driverMemory', 'searchStartDMText', 'searchEndDMText', 'filterDropdownVisibleDM')}>Search</Button>
                </Col>
              </Row>
            </Form>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleDM,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisibleDM: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Per Executor Cores',
        dataIndex: 'perExecutorCores',
        key: 'perExecutorCores',
        sorter: (a, b) => a.perExecutorCores - b.perExecutorCores,
        sortOrder: sortedInfo.columnKey === 'perExecutorCores' && sortedInfo.order,
        filteredValue: filteredInfo.perExecutorCores,
        filterDropdown: (
          <div className="custom-filter-dropdown custom-filter-dropdown-ps">
            <Form>
              <Row>
                <Col span={9}>
                  <Input
                    ref={ele => { this.searchInput = ele }}
                    placeholder="Start"
                    onChange={this.onInputChange('searchStartPECText')}
                  />
                </Col>
                <Col span={1}>
                  <p className="ant-form-split">-</p>
                </Col>
                <Col span={9}>
                  <Input
                    placeholder="End"
                    onChange={this.onInputChange('searchEndPECText')}
                  />
                </Col>
                <Col span={5} className="text-align-center">
                  <Button type="primary" onClick={this.onRangeNumSearch('perExecutorCores', 'searchStartPECText', 'searchEndPECText', 'filterDropdownVisiblePEC')}>Search</Button>
                </Col>
              </Row>
            </Form>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisiblePEC,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisiblePEC: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Per Executor Memory',
        dataIndex: 'perExecutorMemory',
        key: 'perExecutorMemory',
        sorter: (a, b) => a.perExecutorMemory - b.perExecutorMemory,
        sortOrder: sortedInfo.columnKey === 'perExecutorMemory' && sortedInfo.order,
        filteredValue: filteredInfo.perExecutorMemory,
        filterDropdown: (
          <div className="custom-filter-dropdown custom-filter-dropdown-ps">
            <Form>
              <Row>
                <Col span={9}>
                  <Input
                    ref={ele => { this.searchInput = ele }}
                    placeholder="Start"
                    onChange={this.onInputChange('searchStartPEMText')}
                  />
                </Col>
                <Col span={1}>
                  <p className="ant-form-split">-</p>
                </Col>
                <Col span={9}>
                  <Input
                    placeholder="End"
                    onChange={this.onInputChange('searchEndPEMText')}
                  />
                </Col>
                <Col span={5} className="text-align-center">
                  <Button type="primary" onClick={this.onRangeNumSearch('perExecutorMemory', 'searchStartPEMText', 'searchEndPEMText', 'filterDropdownVisiblePEM')}>Search</Button>
                </Col>
              </Row>
            </Form>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisiblePEM,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisiblePEM: visible
        }, () => this.searchInput.focus())
      }, {
        title: 'Executor Numbers',
        dataIndex: 'executorNums',
        key: 'executorNums',
        sorter: (a, b) => a.executorNums - b.executorNums,
        sortOrder: sortedInfo.columnKey === 'executorNums' && sortedInfo.order,
        filteredValue: filteredInfo.executorNums,
        filterDropdown: (
          <div className="custom-filter-dropdown custom-filter-dropdown-ps">
            <Form>
              <Row>
                <Col span={9}>
                  <Input
                    ref={ele => { this.searchInput = ele }}
                    placeholder="Start"
                    onChange={this.onInputChange('searchStartENText')}
                  />
                </Col>
                <Col span={1}>
                  <p className="ant-form-split">-</p>
                </Col>
                <Col span={9}>
                  <Input
                    placeholder="End"
                    onChange={this.onInputChange('searchEndENText')}
                  />
                </Col>
                <Col span={5} className="text-align-center">
                  <Button type="primary" onClick={this.onRangeNumSearch('executorNums', 'searchStartENText', 'searchEndENText', 'filterDropdownVisibleEN')}>Search</Button>
                </Col>
              </Row>
            </Form>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleEN,
        onFilterDropdownVisibleChange: visible => this.setState({
          filterDropdownVisibleEN: visible
        }, () => this.searchInput.focus())
      }]

    const pagination = {
      defaultPageSize: this.state.pageSize,
      showSizeChanger: true,
      onShowSizeChange: (current, pageSize) => {},
      onChange: (current) => {}
    }

    return (
      <div className={`ri-workbench-table ri-common-block`}>
        <Helmet title="Workbench" />
        <h3 className="ri-common-block-title">
          <Icon type="bars" /> Resource 列表
        </h3>

        <Table
          title={() => (<h4>
            <span style={{ marginRight: '5%' }}>Total Cores: {resources.totalCores}</span>
            <span style={{ marginRight: '5%' }}>Total Memory: {resources.totalMemory}</span>
            <span style={{ marginRight: '5%' }}>Remain Cores: {resources.remainCores}</span>
            <span style={{ marginRight: '5%' }}>Remain Memory: {resources.remainMemory}</span>
          </h4>)}
          dataSource={this.state.currentResources}
          columns={columns}
          onChange={this.handleUserChange}
          pagination={pagination}
          className="ri-workbench-table-container resource-table"
          bordered>
        </Table>
      </div>
    )
  }
}

Resource.propTypes = {
  projectIdGeted: React.PropTypes.string,

  onLoadResources: React.PropTypes.func,
  resources: React.PropTypes.oneOfType([
    React.PropTypes.object,
    React.PropTypes.bool
  ])
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadResources: (projectId, roleType) => dispatch(loadResources(projectId, roleType))
  }
}

const mapStateToProps = createStructuredSelector({
  resources: selectResources()
})

export default connect(mapStateToProps, mapDispatchToProps)(Resource)
