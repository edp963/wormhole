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

import Table from 'antd/lib/table'
import Button from 'antd/lib/button'
import Icon from 'antd/lib/icon'
import Tooltip from 'antd/lib/tooltip'
import Popover from 'antd/lib/popover'
import Popconfirm from 'antd/lib/popconfirm'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
import Input from 'antd/lib/input'
import DatePicker from 'antd/lib/date-picker'
const { RangePicker } = DatePicker

import { operateLanguageText } from '../../utils/util'
import UdfForm from './UdfForm'
import { loadUdfs, loadSingleUdf, addUdf, loadUdfDetail, editUdf, deleteUdf } from './action'
import { selectUdfs, selectError, selectModalLoading } from './selectors'
import { selectRoleType } from '../App/selectors'
import { selectLocale } from '../LanguageProvider/selectors'

export class Udf extends React.PureComponent {
  constructor (props) {
    super(props)
    this.state = {
      formVisible: false,
      formType: 'add',
      refreshUdfLoading: false,
      refreshUdfText: 'Refresh',

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

      columnNameText: '',
      valueText: '',
      visibleBool: false,
      startTimeTextState: '',
      endTimeTextState: '',
      paginationInfo: null,

      queryUdfVal: {},
      showUdfDetail: {},
      streamType: '',
      mapOrAgg: ''
    }
  }

  componentWillMount () {
    const { roleType, udfClassHide } = this.props
    if (roleType === 'admin') {
      if (!udfClassHide) {
        this.props.onLoadUdfs(() => { this.udfRefreshState() })
      }
    }
  }

  componentWillReceiveProps (props) {
    if (props.udfs) {
      const originUdfs = props.udfs.map(s => {
        s.key = s.id
        s.visible = false
        return s
      })
      this.setState({ originUdfs: originUdfs.slice() })
      this.state.columnNameText === ''
        ? this.setState({ currentudfs: originUdfs.slice() })
        : this.searchOperater()
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

  refreshUdf = () => {
    this.setState({
      refreshUdfLoading: true,
      refreshUdfText: 'Refreshing'
    })
    const { projectIdGeted, udfClassHide, roleType } = this.props
    if (roleType === 'admin') {
      udfClassHide === 'hide'
        ? this.props.onLoadSingleUdf(projectIdGeted, 'admin', () => { this.udfRefreshState() })
        : this.props.onLoadUdfs(() => { this.udfRefreshState() })
    } else if (roleType === 'user') {
      this.props.onLoadSingleUdf(projectIdGeted, 'user', () => { this.udfRefreshState() })
    }
  }

  udfRefreshState () {
    this.setState({
      refreshUdfLoading: false,
      refreshUdfText: 'Refresh'
    })
    const { paginationInfo, filteredInfo, sortedInfo } = this.state
    this.handleUdfChange(paginationInfo, filteredInfo, sortedInfo)
  }

  showAddUdf = () => {
    this.setState({streamType: 'spark'}, () => {
      this.setState({
        formVisible: true,
        formType: 'add'
      })
    })
  }

  copySingleUdf = (record) => (e) => {
    this.setState({
      formVisible: true,
      formType: 'copy'
    }, () => {
      this.props.onLoadUdfDetail(record.id, ({
        functionName, fullClassName, jarName, desc, pubic
                                             }) => {
        this.udfForm.setFieldsValue({
          functionName: functionName,
          fullName: fullClassName,
          jarName: jarName,
          desc: desc,
          public: `${pubic}`
        })
      })
    })
  }

  onShowEditUdf = (record) => (e) => {
    this.props.onLoadUdfDetail(record.id, ({ createTime, createBy, updateTime, updateBy, id, functionName, fullClassName, jarName, desc, pubic, streamType }) => {
      this.setState({
        queryUdfVal: {
          createTime: createTime,
          createBy: createBy,
          updateTime: updateTime,
          updateBy: updateBy
        },
        streamType,
        formVisible: true,
        formType: 'edit'
      }, () => {
        this.udfForm.setFieldsValue({
          id: id,
          functionName: functionName,
          fullName: fullClassName,
          desc: desc,
          public: `${pubic}`,
          streamType
        })
        if (streamType === 'spark') {
          this.udfForm.setFieldsValue({ jarName })
        }
      })
    })
  }

  // 点击遮罩层或右上角叉或取消按钮的回调
  hideForm = () => this.setState({ formVisible: false })

  // Modal 完全关闭后的回调
  resetModal = () => {
    this.udfForm.resetFields()
  }

  onModalOk = () => {
    const { formType, queryUdfVal } = this.state
    const { locale } = this.props
    const createText = locale === 'en' ? 'is created successfully!' : '新建成功！'
    const copyText = locale === 'en' ? 'is copied successfully!' : '复制成功！'
    const createFailText = locale === 'en' ? 'It fails to create UDF:' : '新建失败：'
    const copyFailText = locale === 'en' ? 'It fails to copy UDF:' : '复制失败：'

    this.udfForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        if (formType === 'add' || formType === 'copy') {
          this.props.onAddUdf(values, () => {
            message.success(`UDF ${formType === 'add' ? createText : copyText}`, 3)
            this.setState({ formVisible: false })
          }, (result) => {
            message.error(`${formType === 'add' ? createFailText : copyFailText} ${result}`, 3)
          })
        } else if (formType === 'edit') {
          this.props.onEditUdf(Object.assign(values, queryUdfVal), () => {
            message.success(operateLanguageText('success', 'modify'), 3)
            this.setState({ formVisible: false })
          }, (result) => {
            message.error(`${operateLanguageText('fail', 'modify')} ${result}`, 3)
          })
        }
      }
    })
  }

  changeUdfStreamType = e => {
    let value = e.target.value
    this.setState({
      streamType: value
    })
  }
  changeUdfMapOrAgg = e => {
    let value = e.target.value
    this.setState({
      mapOrAgg: value
    })
  }
  deleteUdfBtn = (record) => (e) => {
    this.props.onDeleteUdf(record.id, () => {
      message.success(operateLanguageText('success', 'delete'), 3)
    }, (result) => {
      message.error(`${operateLanguageText('fail', 'delete')} ${result}`, 5)
    })
  }

  onSearch = (columnName, value, visible) => () => {
    const reg = new RegExp(this.state[value], 'gi')

    this.setState({
      filteredInfo: {pubic: []}
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        valueText: value,
        visibleBool: visible,
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
    })
  }

  handleUdfChange = (pagination, filters, sorter) => {
    if (filters) {
      if (filters.pubic) {
        if (filters.pubic.length !== 0) {
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
      filteredInfo: {pubic: []}
    }, () => {
      this.setState({
        [visible]: false,
        columnNameText: columnName,
        startTimeTextState: startTimeText,
        endTimeTextState: endTimeText,
        visibleBool: visible,
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
    })
  }

  handleVisibleChangeUdf = (record) => (visible) => {
    if (visible) {
      this.setState({
        visible
      }, () => {
        this.props.onLoadUdfDetail(record.id, (result) => this.setState({ showUdfDetail: result }))
      })
    }
  }

  render () {
    const { udfClassHide, modalLoading, roleType } = this.props
    const { formType, formVisible, currentudfs, refreshUdfLoading, refreshUdfText, showUdfDetail, streamType } = this.state

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
    },
    {
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
    },
    {
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
      onFilter: (value, record) => {
        const recordPubic = record.pubic ? 'true' : 'false'
        return recordPubic.includes(value)
      },
      render: (text, record) => text ? 'true' : 'false'
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
      // filteredValue: filteredInfo.createTime,
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
      // filteredValue: filteredInfo.updateTime,
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
      className: `text-align-center ${udfClassHide}`,
      render: (text, record) => (
        <span className="ant-table-action-column">
          <Tooltip title={<FormattedMessage {...messages.udfViewDetails} />}>
            <Popover
              placement="left"
              content={<div className="project-name-detail">
                <p><strong>   Project Names：</strong>{showUdfDetail.projectNames}</p>
                <p><strong>   Create By：</strong>{showUdfDetail.createBy}</p>
                <p><strong>   Update By：</strong>{showUdfDetail.updateBy}</p>
              </div>}
              title={<h3>{<FormattedMessage {...messages.udfDetails} />}</h3>}
              trigger="click"
              onVisibleChange={this.handleVisibleChangeUdf(record)}
            >
              <Button icon="file-text" shape="circle" type="ghost"></Button>
            </Popover>
          </Tooltip>

          <Tooltip title={<FormattedMessage {...messages.udfTableCopy} />}>
            <Button icon="copy" shape="circle" type="ghost" onClick={this.copySingleUdf(record)}></Button>
          </Tooltip>
          <Tooltip title={<FormattedMessage {...messages.udfTableModify} />}>
            <Button icon="edit" shape="circle" type="ghost" onClick={this.onShowEditUdf(record)}></Button>
          </Tooltip>
          {
            roleType === 'admin'
              ? (
                <Popconfirm placement="bottom" title={<FormattedMessage {...messages.udfSureDelete} />} okText="Yes" cancelText="No" onConfirm={this.deleteUdfBtn(record)}>
                  <Tooltip title={<FormattedMessage {...messages.udfTableDelete} />}>
                    <Button icon="delete" shape="circle" type="ghost"></Button>
                  </Tooltip>
                </Popconfirm>
              )
              : ''
          }
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

    let udfModalTitle = ''
    if (formType === 'add') {
      udfModalTitle = <FormattedMessage {...messages.udfModalAdd} />
    } else if (formType === 'edit') {
      udfModalTitle = <FormattedMessage {...messages.udfModalEdit} />
    } else if (formType === 'copy') {
      udfModalTitle = <FormattedMessage {...messages.udfModalCopy} />
    }

    return (
      <div>
        <Helmet title="Database" />
        <div className="ri-workbench-table ri-common-block">
          <h3 className="ri-common-block-title">
            <Icon type="bars" /> UDF <FormattedMessage {...messages.udfTableList} />
          </h3>
          <div className="ri-common-block-tools">
            <Button icon="plus" type="primary" onClick={this.showAddUdf} className={udfClassHide}>
              <FormattedMessage {...messages.udfTableCreate} />
            </Button>
            <Button icon="reload" type="ghost" className="refresh-button-style" loading={refreshUdfLoading} onClick={this.refreshUdf}>{refreshUdfText}</Button>
          </div>
          <Table
            dataSource={currentudfs}
            columns={columns}
            onChange={this.handleUdfChange}
            pagination={pagination}
            className="ri-workbench-table-container"
            bordered>
          </Table>
        </div>
        <Modal
          title={udfModalTitle}
          okText="保存"
          wrapClassName="db-form-style"
          visible={formVisible}
          onCancel={this.hideForm}
          afterClose={this.resetModal}
          footer={[
            <Button
              key="cancel"
              size="large"
              type="ghost"
              onClick={this.hideForm}
            >
              <FormattedMessage {...messages.udfCancel} />
            </Button>,
            <Button
              key="submit"
              size="large"
              type="primary"
              loading={modalLoading}
              onClick={this.onModalOk}
            >
              <FormattedMessage {...messages.udfSave} />
            </Button>
          ]}
        >
          <UdfForm
            type={formType}
            streamType={streamType}
            changeUdfStreamType={this.changeUdfStreamType}
            changeUdfMapOrAgg={this.changeUdfMapOrAgg}
            ref={(f) => { this.udfForm = f }}
          />
        </Modal>
      </div>
    )
  }
}

Udf.propTypes = {
  // udfs: PropTypes.oneOfType([
  //   PropTypes.bool,
  //   PropTypes.array
  // ]),
  modalLoading: PropTypes.bool,
  projectIdGeted: PropTypes.string,
  udfClassHide: PropTypes.string,
  onLoadUdfs: PropTypes.func,
  onLoadSingleUdf: PropTypes.func,
  onAddUdf: PropTypes.func,
  onLoadUdfDetail: PropTypes.func,
  onEditUdf: PropTypes.func,
  onDeleteUdf: PropTypes.func,
  roleType: PropTypes.string,
  locale: PropTypes.string
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadUdfs: (resolve) => dispatch(loadUdfs(resolve)),
    onLoadSingleUdf: (projectId, roleType, resolve) => dispatch(loadSingleUdf(projectId, roleType, resolve)),
    onAddUdf: (values, resolve, reject) => dispatch(addUdf(values, resolve, reject)),
    onLoadUdfDetail: (udfId, resolve) => dispatch(loadUdfDetail(udfId, resolve)),
    onEditUdf: (values, resolve, reject) => dispatch(editUdf(values, resolve, reject)),
    onDeleteUdf: (udfId, resolve, reject) => dispatch(deleteUdf(udfId, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  udfs: selectUdfs(),
  error: selectError(),
  modalLoading: selectModalLoading(),
  roleType: selectRoleType(),
  locale: selectLocale()
})

export default connect(mapStateToProps, mapDispatchToProps)(Udf)
