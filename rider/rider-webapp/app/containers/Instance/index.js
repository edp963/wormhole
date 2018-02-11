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

import { changeLocale } from '../../containers/LanguageProvider/actions'
import { loadInstances, addInstance, loadInstanceInputValue, loadInstanceExit,
  loadSingleInstance, editInstance, deleteInstace } from './action'
import { selectInstances, selectError, selectModalLoading, selectConnectUrlExisted, selectInstanceExisted } from './selectors'

import { operateLanguageText } from '../../utils/util'

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
    this.props.onChangeLanguage(localStorage.getItem('preferredLanguage'))
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
    const { columnNameText, valueText, visibleBool } = this.state
    const { startTimeTextState, endTimeTextState } = this.state

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
      connUrl, active, createBy, createTime, id, nsInstance, nsSys, updateBy, updateTime, desc
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
          description: desc
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
    const { instanceExisted } = this.props
    const languageText = localStorage.getItem('preferredLanguage')
    const instanceExist = languageText === 'en' ? 'This instance already exists.' : '该 Instance 已存在。'
    const createFormat = languageText === 'en' ? 'Instance is created successfully!' : 'Instance 新建成功！'
    const modifyFormat = languageText === 'en' ? 'Instance is modified successfully!' : 'Instance 修改成功！'

    this.instanceForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (instanceFormType === 'add') {
          if (instanceExisted) {
            this.instanceForm.setFields({
              instance: {
                value: values.instance,
                errors: [new Error(instanceExist)]
              }
            })
          } else {
            this.props.onAddInstance(values, () => {
              this.hideForm()
              message.success(createFormat, 3)
            })
          }
        } else if (instanceFormType === 'edit') {
          this.props.onEditInstance(Object.assign({}, this.state.editInstanceData, {
            desc: values.description,
            connUrl: values.connectionUrl
          }), () => {
            this.hideForm()
            message.success(modifyFormat, 3)
          })
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

  /***
   * 新增时，验证 Connection Url 是否存在
   * */
  onInitInstanceInputValue = (value) => {
    const { eidtConnUrl } = this.state

    if (eidtConnUrl !== value) {
      const requestVal = {
        type: this.state.InstanceSourceDsVal,
        conn_url: value
      }

      this.props.onLoadInstanceInputValue(requestVal, () => {}, (result) => {
        this.loadResult(value, result)
      })
    }
  }

  loadResult (value, result) {
    const { instanceFormType, InstanceSourceDsVal } = this.state
    const languageText = localStorage.getItem('preferredLanguage')
    const existText = languageText === 'en'
      ? `The connection url already exists, confirm ${instanceFormType === 'add' ? 'create' : 'modify'}?`
      : `该 Connection URL 已存在，确定${instanceFormType === 'add' ? '新建' : '修改'}吗？`
    const esText = languageText === 'en'
      ? 'if it acts as sink, fill in http://localhost:9200; if it acts as lookup, fill in localhost:9300'
      : '作为sink，填写 http://localhost:9200；作为lookup，填写localhost:9300'

    let errMsg = ''
    if (result.includes('exists')) {
      errMsg = [new Error(existText)]
    } else {
      if (InstanceSourceDsVal === 'es') {
        errMsg = [new Error(esText)]
      } else if (InstanceSourceDsVal === 'oracle' ||
        InstanceSourceDsVal === 'mysql' ||
        InstanceSourceDsVal === 'postgresql'
      ) {
        errMsg = [new Error('ip:port')]
      } else if (InstanceSourceDsVal === 'hbase') {
        errMsg = [new Error('zookeeper url list, localhost:2181/hbase,localhost1:2181/hbase')]
      } else if (InstanceSourceDsVal === 'phoenix') {
        errMsg = [new Error('zookeeper url list')]
      } else if (InstanceSourceDsVal === 'kafka') {
        errMsg = [new Error('localhost:9092,localhost1:9092')]
      } else if (InstanceSourceDsVal === 'cassandra' ||
        InstanceSourceDsVal === 'mongodb' ||
        InstanceSourceDsVal === 'redis'
      ) {
        errMsg = [new Error('ip:port list')]
      }
      // else if (InstanceSourceDsVal === 'log') {
      //   errMsg = ''
      // }
    }

    this.instanceForm.setFields({
      connectionUrl: {
        value: value,
        errors: errMsg
      }
    })
  }

  /***
   * 新增时，验证 Instance 是否存在
   * */
  onInitInstanceExited = (value) => {
    const requestVal = {
      type: this.state.InstanceSourceDsVal,
      nsInstance: value
    }
    this.props.onLoadInstanceExit(requestVal, () => {}, (result) => {
      const languageText = localStorage.getItem('preferredLanguage')
      this.instanceForm.setFields({
        instance: {
          value: value,
          errors: [new Error(languageText === 'en' ? 'The instance already exists.' : '该 Instance 已存在。')]
        }
      })
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
    const { refreshInstanceLoading, refreshInstanceText, showInstanceDetails } = this.state

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
        filters: [
          {text: 'oracle', value: 'oracle'},
          {text: 'mysql', value: 'mysql'},
          {text: 'es', value: 'es'},
          {text: 'hbase', value: 'hbase'},
          {text: 'phoenix', value: 'phoenix'},
          {text: 'cassandra', value: 'cassandra'},
          {text: 'kafka', value: 'kafka'},
          {text: 'postgresql', value: 'postgresql'},
          {text: 'mongodb', value: 'mongodb'},
          {text: 'redis', value: 'redis'}
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
              localStorage.getItem('loginRoleType') === 'admin'
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

    const { currentInstances, instanceFormType, formVisible } = this.state
    const { modalLoading } = this.props

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
            onInitInstanceInputValue={this.onInitInstanceInputValue}
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
  modalLoading: React.PropTypes.bool,
  instanceExisted: React.PropTypes.bool,
  onLoadInstances: React.PropTypes.func,
  onAddInstance: React.PropTypes.func,
  onLoadInstanceInputValue: React.PropTypes.func,
  onLoadInstanceExit: React.PropTypes.func,
  onLoadSingleInstance: React.PropTypes.func,
  onEditInstance: React.PropTypes.func,
  onDeleteInstace: React.PropTypes.func,
  onChangeLanguage: React.PropTypes.func
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadInstances: (resolve) => dispatch(loadInstances(resolve)),
    onAddInstance: (instance, resolve) => dispatch(addInstance(instance, resolve)),
    onLoadInstanceInputValue: (value, resolve, reject) => dispatch(loadInstanceInputValue(value, resolve, reject)),
    onLoadInstanceExit: (value, resolve, reject) => dispatch(loadInstanceExit(value, resolve, reject)),
    onLoadSingleInstance: (instanceId, resolve) => dispatch(loadSingleInstance(instanceId, resolve)),
    onEditInstance: (value, resolve) => dispatch(editInstance(value, resolve)),
    onDeleteInstace: (value, resolve, reject) => dispatch(deleteInstace(value, resolve, reject)),
    onChangeLanguage: (type) => dispatch(changeLocale(type))
  }
}

const mapStateToProps = createStructuredSelector({
  instances: selectInstances(),
  error: selectError(),
  modalLoading: selectModalLoading(),
  connectUrlExisted: selectConnectUrlExisted(),
  instanceExisted: selectInstanceExisted()
})

export default connect(mapStateToProps, mapDispatchToProps)(Instance)
