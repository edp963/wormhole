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
import PropTypes from 'prop-types'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Helmet from 'react-helmet'
import { FormattedMessage } from 'react-intl'
import messages from './messages'

import InstanceForm from './InstanceForm'
import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Icon from 'antd/lib/icon'
import Tooltip from 'antd/lib/tooltip'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
import Input from 'antd/lib/input'
import Popover from 'antd/lib/popover'
import Popconfirm from 'antd/lib/popconfirm'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import {
  loadInstances,
  addInstance,
  checkUrl,
  loadSingleInstance,
  editInstance,
  deleteInstace
} from './action'
import {
  selectInstances,
  selectError,
  selectModalLoading
} from './selectors'
import { selectRoleType } from '../App/selectors'
import { selectLocale } from '../LanguageProvider/selectors'

import { operateLanguageText, isJSON } from '../../utils/util'
import { filterDataSystemData } from '../../components/DataSystemSelector/dataSystemFunction'

export class Instance extends React.PureComponent {
  constructor (props) {
    super(props)
    this.state = {
      formVisible: false,
      instanceFormType: 'add',
      refreshInstanceLoading: false,
      refreshInstanceText: 'Refresh',
      showInstanceDetails: {},

      currentInstances: [],
      originInstances: [],

      filteredInfo: null,
      sortedInfo: null,

      searchTextInstance: '',
      filterDropdownVisibleInstance: false,
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

      columnNameText: '',
      valueText: '',
      visibleBool: false,
      startTimeTextState: '',
      endTimeTextState: '',
      paginationInfo: null,

      editInstanceData: {},
      eidtConnUrl: '',
      InstanceSourceDsVal: ''
    }
  }

  componentWillMount () {
    this.refreshInstance()
  }

  // componentWillUpdate (props) {
  //   console.log('props', props.instances)
  //   if (props.instances) {
  //     const originInstances = props.instances.map(s => {
  //       s.key = s.id
  //       s.visible = false
  //       return s
  //     })
  //     this.state.originInstances = originInstances.slice()
  //     this.state.currentInstances = originInstances.slice()
  //   }
  // }

  componentWillReceiveProps (props) {
    if (props.instances) {
      const originInstances = props.instances.map(s => {
        s.key = s.id
        s.visible = false
        return s
      })
      this.setState({ originInstances: originInstances.slice() })

      this.state.columnNameText === ''
        ? this.setState({ currentInstances: originInstances.slice() })
        : this.searchOperater()  // action 后仍显示table搜索后的数据
    }
  }

  searchOperater () {
    const { columnNameText, valueText, visibleBool, startTimeTextState, endTimeTextState } = this.state

    if (columnNameText !== '') {
      this.onSearch(columnNameText, valueText, visibleBool)()

      if (columnNameText === 'createTime' || columnNameText === 'updateTime') {
        this.onRangeTimeSearch(columnNameText, startTimeTextState, endTimeTextState, visibleBool)()
      }
    }
  }

  refreshInstance = () => {
    this.setState({
      refreshInstanceLoading: true,
      refreshInstanceText: 'Refreshing'
    })
    this.props.onLoadInstances(() => this.instanceRefreshState())
  }

  instanceRefreshState () {
    this.setState({
      refreshInstanceLoading: false,
      refreshInstanceText: 'Refresh'
    })
    const { paginationInfo, filteredInfo, sortedInfo } = this.state
    this.handleInstanceChange(paginationInfo, filteredInfo, sortedInfo)
    this.searchOperater()
  }

  showAddInstance = () => {
    this.setState({
      formVisible: true,
      instanceFormType: 'add'
    })
  }

  showEditInstance = (instance) => (e) => {
    this.props.onLoadSingleInstance(instance.id, ({
      connUrl, active, createBy, createTime, id, nsInstance, nsSys, updateBy, updateTime, desc, connConfig
                                                  }) => {
      this.setState({
        formVisible: true,
        instanceFormType: 'edit',
        eidtConnUrl: connUrl,
        editInstanceData: {
          active: active,
          createBy: createBy,
          createTime: createTime,
          id: id,
          nsInstance: nsInstance,
          nsSys: nsSys,
          updateBy: updateBy,
          updateTime: updateTime
        }
      }, () => {
        this.instanceForm.setFieldsValue({
          instanceDataSystem: nsSys,
          connectionUrl: connUrl,
          instance: nsInstance,
          description: desc,
          connConfig
        })
      })
    })
  }

  hideForm = () => {
    this.setState({ formVisible: false })
    this.instanceForm.resetFields()
  }

  onModalOk = () => {
    const { instanceFormType } = this.state
    const { locale } = this.props
    const createFormat = locale === 'en' ? 'Instance is created successfully!' : 'Instance 新建成功！'
    const modifyFormat = locale === 'en' ? 'Instance is modified successfully!' : 'Instance 修改成功！'

    this.instanceForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const connConfig = values.connConfig
        if (connConfig && !isJSON(connConfig)) {
          message.error('Connection Config Format Error, Must be JSON', 3)
          return
        }
        switch (instanceFormType) {
          case 'add':
            this.props.onAddInstance(values, () => {
              this.hideForm()
              message.success(createFormat, 3)
            }, (msg) => {
              this.loadResult(values.connectionUrl, msg)
            })
            break
          case 'edit':
            this.props.onEditInstance(Object.assign(this.state.editInstanceData, {
              desc: values.description,
              connUrl: values.connectionUrl,
              connConfig
            }), () => {
              this.hideForm()
              message.success(modifyFormat, 3)
            }, (msg) => {
              this.loadResult(values.connectionUrl, msg)
            })
            break
        }
      }
    })
  }

  onSearch = (columnName, value, visible) => () => {
    this.setState({
      filteredInfo: {nsSys: []} // 清除 type filter
    }, () => {
      const reg = new RegExp(this.state[value], 'gi')

      this.setState({
        [visible]: false,
        columnNameText: columnName,
        valueText: value,
        visibleBool: visible,
        currentInstances: this.state.originInstances.map((record) => {
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

  handleInstanceChange = (pagination, filters, sorter) => {
    // 不影响分页和排序的数据源，数据源是搜索后的
    // 清除 text search，否则当文本搜索后，再类型搜索时的数据源是文本搜索的
    if (filters) {
      if (filters.nsSys) {
        if (filters.nsSys.length !== 0) {
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

  onInitInstanceSourceDs = (value) => {
    this.setState({
      InstanceSourceDsVal: value
    })
    if (this.state.instanceFormType === 'add') {
      this.instanceForm.setFieldsValue({
        connectionUrl: '',
        instance: ''
      })
    }
  }

  // 新增时，验证 Connection Url 是否存在
  onInitCheckUrl = (value) => {
    const { eidtConnUrl, InstanceSourceDsVal } = this.state

    if (eidtConnUrl !== value) {
      const requestVal = {
        type: InstanceSourceDsVal,
        conn_url: value
      }

      this.props.onLoadCheckUrl(requestVal, () => {}, (result) => {
        this.loadResult(value, result)
      })
    }
  }

  loadResult (value, result) {
    this.instanceForm.setFields({
      connectionUrl: {
        value: value,
        errors: [new Error(result)]
      }
    })
  }

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
      filteredInfo: {nsSys: []} // 清除 type filter
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        startTimeTextState: startTimeText,
        endTimeTextState: endTimeText,
        visibleBool: visible,
        currentInstances: this.state.originInstances.map((record) => {
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

  handleVisibleChangeInstance = (record) => (visible) => {
    if (visible) {
      this.setState({
        visible
      }, () => {
        this.props.onLoadSingleInstance(record.id, (result) => this.setState({ showInstanceDetails: result }))
      })
    }
  }

  deleteInstanceBtn = (record) => (e) => {
    this.props.onDeleteInstace(record.id, () => {
      message.success(operateLanguageText('success', 'delete'), 3)
    }, (result) => {
      message.error(`${operateLanguageText('fail', 'delete')} ${result}`, 5)
    })
  }

  render () {
    const { refreshInstanceLoading, refreshInstanceText, showInstanceDetails, currentInstances, instanceFormType, formVisible } = this.state
    const { modalLoading, roleType } = this.props

    let { sortedInfo, filteredInfo } = this.state
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const columns = [
      {
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
        filters: filterDataSystemData(),
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
              value={this.state.searchTextInstance}
              onChange={this.onInputChange('searchTextInstance')}
              onPressEnter={this.onSearch('nsInstance', 'searchTextInstance', 'filterDropdownVisibleInstance')}
            />
            <Button type="primary" onClick={this.onSearch('nsInstance', 'searchTextInstance', 'filterDropdownVisibleInstance')}>Search</Button>
          </div>
        ),
        filterDropdownVisible: this.state.filterDropdownVisibleInstance,
        onFilterDropdownVisibleChange: visible => {
          this.setState({
            // searchTextInstance: '', // 搜索框弹出时，清除内容
            filterDropdownVisibleInstance: visible
          }, () => this.searchInput.focus())
        }
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
          // searchTextConnUrl: '',
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
        render: (text, record) => (
          <span className="ant-table-action-column">
            <Tooltip title={<FormattedMessage {...messages.instanceViewDetailsBtn} />}>
              <Popover
                placement="left"
                content={<p><strong>Description：</strong>{showInstanceDetails.desc}</p>}
                title={<h3><FormattedMessage {...messages.instanceDetails} /></h3>}
                trigger="click"
                onVisibleChange={this.handleVisibleChangeInstance(record)}>
                <Button icon="file-text" shape="circle" type="ghost"></Button>
              </Popover>
            </Tooltip>

            <Tooltip title={<FormattedMessage {...messages.instanceModify} />}>
              <Button icon="edit" shape="circle" type="ghost" onClick={this.showEditInstance(record)} />
            </Tooltip>

            {
              roleType === 'admin'
                ? (
                  <Popconfirm placement="bottom" title={<FormattedMessage {...messages.instanceSureDelete} />} okText="Yes" cancelText="No" onConfirm={this.deleteInstanceBtn(record)}>
                    <Tooltip title={<FormattedMessage {...messages.instanceDelete} />}>
                      <Button icon="delete" shape="circle" type="ghost"></Button>
                    </Tooltip>
                  </Popconfirm>
                )
                : ''
            }
          </span>
        )
      }
    ]

    const pagination = {
      showSizeChanger: true,
      onChange: (current) => this.setState({ pageIndex: current })
    }

    const modalTitle = instanceFormType === 'add'
      ? <FormattedMessage {...messages.instanceTableCreate} />
      : <FormattedMessage {...messages.instanceTableModify} />

    return (
      <div>
        <Helmet title="Instance" />
        <div className="ri-workbench-table ri-common-block">
          <h3 className="ri-common-block-title">
            <Icon type="bars" /> Instance <FormattedMessage {...messages.instanceTableList} />
          </h3>
          <div className="ri-common-block-tools">
            <Button icon="plus" type="primary" onClick={this.showAddInstance}>
              <FormattedMessage {...messages.instanceCreate} />
            </Button>
            <Button icon="reload" type="ghost" className="refresh-button-style" loading={refreshInstanceLoading} onClick={this.refreshInstance}>{refreshInstanceText}</Button>
          </div>
          <Table
            dataSource={currentInstances}
            columns={columns}
            onChange={this.handleInstanceChange}
            pagination={pagination}
            className="ri-workbench-table-container"
            bordered>
          </Table>
        </div>
        <Modal
          title={modalTitle}
          okText="保存"
          wrapClassName="instance-form-style"
          visible={formVisible}
          onCancel={this.hideForm}
          footer={[
            <Button
              key="cancel"
              size="large"
              type="ghost"
              onClick={this.hideForm}
            >
              <FormattedMessage {...messages.instanceModalCancel} />
            </Button>,
            <Button
              key="submit"
              size="large"
              type="primary"
              loading={modalLoading}
              onClick={this.onModalOk}
            >
              <FormattedMessage {...messages.instanceModalSave} />
            </Button>
          ]}
        >
          <InstanceForm
            instanceFormType={instanceFormType}
            onInitCheckUrl={this.onInitCheckUrl}
            onInitInstanceExited={this.onInitInstanceExited}
            onInitInstanceSourceDs={this.onInitInstanceSourceDs}
            ref={(f) => { this.instanceForm = f }}
          />
        </Modal>
      </div>
    )
  }
}

Instance.propTypes = {
  modalLoading: PropTypes.bool,
  onLoadInstances: PropTypes.func,
  onAddInstance: PropTypes.func,
  onLoadSingleInstance: PropTypes.func,
  onEditInstance: PropTypes.func,
  onDeleteInstace: PropTypes.func,
  onLoadCheckUrl: PropTypes.func,
  roleType: PropTypes.string,
  locale: PropTypes.string
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadInstances: (resolve) => dispatch(loadInstances(resolve)),
    onAddInstance: (instance, resolve, reject) => dispatch(addInstance(instance, resolve, reject)),
    onLoadSingleInstance: (instanceId, resolve) => dispatch(loadSingleInstance(instanceId, resolve)),
    onEditInstance: (value, resolve, reject) => dispatch(editInstance(value, resolve, reject)),
    onDeleteInstace: (value, resolve, reject) => dispatch(deleteInstace(value, resolve, reject)),
    onLoadCheckUrl: (value, resolve, reject) => dispatch(checkUrl(value, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  instances: selectInstances(),
  error: selectError(),
  modalLoading: selectModalLoading(),
  roleType: selectRoleType(),
  locale: selectLocale()
})

export default connect(mapStateToProps, mapDispatchToProps)(Instance)
