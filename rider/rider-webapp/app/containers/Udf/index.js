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

import UdfForm from './UdfForm'
import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Icon from 'antd/lib/icon'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
// import Popconfirm from 'antd/lib/popconfirm'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
import Input from 'antd/lib/input'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { loadUdfs, loadSingleUdf, addUdf, editUdf } from './action'
import { selectUdfs, selectError, selectModalLoading } from './selectors'

export class Udf extends React.PureComponent {
  constructor (props) {
    super(props)
    this.state = {
      formVisible: false,
      formType: 'add',
      refreshUdfLoading: false,
      refreshUdfText: '',

      currentudfs: [],
      originUdfs: [],

      filteredInfo: null,
      sortedInfo: null,

      searchTextFunctionName: '',
      filterDropdownVisibleFunctionName: false,
      searchTextFullClassName: '',
      filterDropdownVisibleFullClassName: false,
      searchTextJarName: '',
      filterDropdownVisibleJarName: false,
      searchTextDesc: '',
      filterDropdownVisibleDesc: false,
      searchTextPublic: '',
      filterDropdownVisiblePublic: false,
      startStartTimeText: '',
      startEndTimeText: '',
      createStartTimeText: '',
      createEndTimeText: '',
      filterDropdownVisibleCreateTime: false,
      filterDropdownVisibleUpdateTime: false,
      searchStartByText: '',
      searchEndByText: '',
      filterDropdownVisibleBy: false,

      queryUdfVal: {}
    }
  }

  componentWillMount () {
    this.loadUdfData()
  }

  componentWillReceiveProps (props) {
    if (props.udfs) {
      const originUdfs = props.udfs.map(s => {
        s.pubic = s.pubic === true ? 'true' : 'false'
        s.key = s.id
        s.visible = false
        return s
      })
      this.setState({
        originUdfs: originUdfs.slice(),
        currentudfs: originUdfs.slice()
      })
    }
  }

  refreshUdf = () => {
    this.setState({
      refreshUdfLoading: true,
      refreshUdfText: 'Refreshing'
    })
    this.loadUdfData()
  }

  udfRefreshState () {
    this.setState({
      refreshUdfLoading: false,
      refreshUdfText: 'Refresh'
    })
  }

  loadUdfData () {
    const { projectIdGeted, udfClassHide } = this.props
    if (localStorage.getItem('loginRoleType') === 'admin') {
      udfClassHide === 'hide'
        ? this.props.onLoadSingleUdf(projectIdGeted, 'admin', () => { this.udfRefreshState() })
        : this.props.onLoadUdfs(() => { this.udfRefreshState() })
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      this.props.onLoadSingleUdf(projectIdGeted, 'user', () => { this.udfRefreshState() })
    }
  }

  showAddUdf = () => {
    this.setState({
      formVisible: true,
      formType: 'add'
    })
  }

  copySingleUdf = (record) => (e) => {
    this.setState({
      formVisible: true,
      formType: 'copy'
    }, () => {
      this.udfForm.setFieldsValue({
        functionName: record.functionName,
        fullName: record.fullClassName,
        jarName: record.jarName,
        desc: record.desc,
        public: record.pubic
      })
    })
  }

  onShowEditUdf = (record) => (e) => {
    this.setState({
      formVisible: true,
      formType: 'edit',
      queryUdfVal: {
        createTime: record.createTime,
        createBy: record.createBy,
        updateTime: record.updateTime,
        updateBy: record.updateBy
      }
    }, () => {
      this.udfForm.setFieldsValue({
        id: record.id,
        functionName: record.functionName,
        fullName: record.fullClassName,
        jarName: record.jarName,
        desc: record.desc,
        public: record.pubic
      })
    })
  }

  // deleteSingleUdf = (record) => (e) => {
  //   const requestParam = {
  //     id: record.id,
  //     functionName: record.functionName,
  //     fullClassName: record.fullClassName,
  //     jarName: record.jarName,
  //     createTime: record.createTime,
  //     createBy: record.createBy
  //   }
  //   this.props.onDeleteUdf(requestParam, () => {
  //
  //   })
  // }

  // 点击遮罩层或右上角叉或取消按钮的回调
  hideForm = () => this.setState({ formVisible: false })

  // Modal 完全关闭后的回调
  resetModal = () => this.udfForm.resetFields()

  onModalOk = () => {
    const { formType, queryUdfVal } = this.state

    this.udfForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (formType === 'add' || formType === 'copy') {
          this.props.onAddUdf(values, () => {
            message.success(`UDF ${formType === 'add' ? '新建' : '复制'}成功！`, 3)
            this.setState({
              formVisible: false
            })
          }, (result) => {
            message.error(`${formType === 'add' ? '新建' : '复制'}失败：${result}`, 3)
          })
        } else if (formType === 'edit') {
          this.props.onEditUdf(Object.assign({}, values, queryUdfVal), () => {
            message.success('修改成功！', 3)
            this.setState({
              formVisible: false
            })
          }, (result) => {
            message.error(`修改失败：${result}`, 3)
          })
        }
      }
    })
  }

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      [visible]: false,
      currentudfs: this.state.originUdfs.map((record) => {
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

  handleUdfChange = (pagination, filters, sorter) => {
    this.setState({
      filteredInfo: filters,
      sortedInfo: sorter
    })
  }

  onInputChange = (value) => (e) => this.setState({ [value]: e.target.value })

  handleEndOpenChange = (status) => {
    this.setState({
      filterDatepickerShown: status
    })
  }

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
      [visible]: false,
      currentUdfs: this.state.originUdfs.map((record) => {
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
  }

  onRangeBySearch = (columnName, startText, endText, visible) => () => {
    this.setState({
      [visible]: false,
      currentUdfs: this.state.originUdfs.map((record) => {
        const match = record[columnName]
        if ((match < parseInt(this.state[startText])) || (match > parseInt(this.state[endText]))) {
          return null
        }
        return record
      }).filter(record => !!record),
      filteredInfo: this.state[startText] || this.state[endText] ? {createBy: [0]} : {createBy: []}
    })
  }

  render () {
    const { udfClassHide } = this.props
    const { refreshUdfLoading, refreshUdfText } = this.state

    let { sortedInfo, filteredInfo } = this.state
    sortedInfo = sortedInfo || {}
    filteredInfo = filteredInfo || {}

    const columns = [{
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
      title: 'Description',
      dataIndex: 'desc',
      key: 'desc',
      sorter: (a, b) => {
        if (typeof a.desc === 'object') {
          return a.descOrigin < b.descOrigin ? -1 : 1
        } else {
          return a.desc < b.desc ? -1 : 1
        }
      },
      sortOrder: sortedInfo.columnKey === 'desc' && sortedInfo.order,
      filterDropdown: (
        <div className="custom-filter-dropdown">
          <Input
            ref={ele => { this.searchInput = ele }}
            placeholder="Description"
            value={this.state.searchTextDesc}
            onChange={this.onInputChange('searchTextDesc')}
            onPressEnter={this.onSearch('desc', 'searchTextDesc', 'filterDropdownVisibleDesc')}
          />
          <Button type="primary" onClick={this.onSearch('desc', 'searchTextDesc', 'filterDropdownVisibleDesc')}>Search</Button>
        </div>
      ),
      filterDropdownVisible: this.state.filterDropdownVisibleDesc,
      onFilterDropdownVisibleChange: visible => this.setState({
        filterDropdownVisibleDesc: visible
      }, () => this.searchInput.focus())
    }, {
      title: 'Public',
      dataIndex: 'pubic',
      key: 'pubic',
      // className: 'text-align-center',
      sorter: (a, b) => a.pubic < b.pubic ? -1 : 1,
      sortOrder: sortedInfo.columnKey === 'pubic' && sortedInfo.order,
      filters: [
        {text: 'true', value: 'true'},
        {text: 'false', value: 'false'}
      ],
      filteredValue: filteredInfo.pubic,
      onFilter: (value, record) => record.pubic.includes(value)
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
      filteredValue: filteredInfo.createTime,
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
      filteredValue: filteredInfo.updateTime,
      filterDropdown: (
        <div className="custom-filter-dropdown-style">
          <RangePicker
            showTime
            format="YYYY-MM-DD HH:mm:ss"
            placeholder={['Start', 'End']}
            onOpenChange={this.handleEndOpenChange}
            onChange={this.onRangeTimeChange}
            onPressEnter={this.onRangeTimeSearch('updateTime', 'startStartTimeText', 'startEndTimeText', 'filterDropdownVisibleUpdateTime')}
          />
          <Button type="primary" className="rangeFilter" onClick={this.onRangeTimeSearch('updateTime', 'startStartTimeText', 'startEndTimeText', 'filterDropdownVisibleUpdateTime')}>Search</Button>
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
          <Tooltip title="查看详情">
            <Popover
              placement="left"
              content={<div className="project-name-detail">
                <p><strong>   Project Names：</strong>{record.projectNames}</p>
                <p><strong>   Create By：</strong>{record.createBy}</p>
                <p><strong>   Update By：</strong>{record.updateBy}</p>
              </div>}
              title={<h3>详情</h3>}
              trigger="click">
              <Button icon="file-text" shape="circle" type="ghost"></Button>
            </Popover>
          </Tooltip>

          <Tooltip title="复制">
            <Button icon="copy" shape="circle" type="ghost" onClick={this.copySingleUdf(record)} className={udfClassHide}></Button>
          </Tooltip>
          <Tooltip title="修改">
            <Button icon="edit" shape="circle" type="ghost" onClick={this.onShowEditUdf(record)} className={udfClassHide}></Button>
          </Tooltip>
          {/* <Popconfirm placement="bottom" title="确定删除吗？" okText="Yes" cancelText="No" onConfirm={this.deleteSingleUdf(record)}>
            <Tooltip title="删除">
              <Button icon="delete" shape="circle" type="ghost"></Button>
            </Tooltip>
          </Popconfirm> */}
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

    const { formType } = this.state
    let udfModalTitle = ''
    if (formType === 'add') {
      udfModalTitle = '新建'
    } else if (formType === 'edit') {
      udfModalTitle = '修改'
    } else if (formType === 'copy') {
      udfModalTitle = '复制'
    }

    return (
      <div>
        <Helmet title="Database" />
        <div className="ri-workbench-table ri-common-block">
          <h3 className="ri-common-block-title">
            <Icon type="bars" /> UDF 列表
          </h3>
          <div className="ri-common-block-tools">
            <Button icon="poweroff" type="ghost" className="refresh-button-style" loading={refreshUdfLoading} onClick={this.refreshUdf}>{refreshUdfText}</Button>
            <Button icon="plus" type="primary" onClick={this.showAddUdf} className={udfClassHide}>新建</Button>
          </div>
          <Table
            dataSource={this.state.currentudfs}
            columns={columns}
            onChange={this.handleUdfChange}
            pagination={pagination}
            className="ri-workbench-table-container"
            bordered>
          </Table>
        </div>
        <Modal
          title={`${udfModalTitle} UDF`}
          okText="保存"
          wrapClassName="db-form-style"
          visible={this.state.formVisible}
          onCancel={this.hideForm}
          afterClose={this.resetModal}
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
          <UdfForm
            type={this.state.formType}
            ref={(f) => { this.udfForm = f }}
          />
        </Modal>
      </div>
    )
  }
}

Udf.propTypes = {
  // udfs: React.PropTypes.oneOfType([
  //   React.PropTypes.bool,
  //   React.PropTypes.array
  // ]),
  modalLoading: React.PropTypes.bool,
  projectIdGeted: React.PropTypes.string,
  udfClassHide: React.PropTypes.string,
  onLoadUdfs: React.PropTypes.func,
  onLoadSingleUdf: React.PropTypes.func,
  onAddUdf: React.PropTypes.func,
  onEditUdf: React.PropTypes.func
  // onDeleteUdf: React.PropTypes.func
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadUdfs: (resolve) => dispatch(loadUdfs(resolve)),
    onLoadSingleUdf: (projectId, roleType, resolve) => dispatch(loadSingleUdf(projectId, roleType, resolve)),
    onAddUdf: (values, resolve, reject) => dispatch(addUdf(values, resolve, reject)),
    onEditUdf: (values, resolve, reject) => dispatch(editUdf(values, resolve, reject))
    // onDeleteUdf: (values, resolve) => dispatch(deleteUdf(values, resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  udfs: selectUdfs(),
  error: selectError(),
  modalLoading: selectModalLoading()
})

export default connect(mapStateToProps, mapDispatchToProps)(Udf)
