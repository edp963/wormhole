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

import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Icon from 'antd/lib/icon'
import Button from 'antd/lib/button'
import Modal from 'antd/lib/modal'
import Popconfirm from 'antd/lib/popconfirm'
import Tooltip from 'antd/lib/tooltip'
import message from 'antd/lib/message'

import ProjectForm from './ProjectForm'
import ProjectNSTable from './ProjectNSTable'
import ProjectUdfTable from './ProjectUdfTable'
import ProjectUsersTable from './ProjectUsersTable'

import { selectCurrentProject, selectRoleType } from '../App/selectors'
import { selectProjects, selectModalLoading } from './selectors'
import { selectNamespaces } from '../Namespace/selectors'
import { selectUsers } from '../User/selectors'

import {
  loadProjects, loadUserProjects, addProject, editProject, loadSingleProject, deleteSingleProject
} from './action'
import { loadSelectNamespaces, loadProjectNsAll } from '../Namespace/action'
import { loadSelectUsers, loadProjectUserAll } from '../User/action'
import { loadSingleUdf, loadProjectUdfs } from '../Udf/action'
import { selectLocale } from '../LanguageProvider/selectors'

export class Project extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      formVisible: false,
      projectFormType: 'add',
      projectResult: {},
      projectUserId: '',
      projectNsId: '',
      projectUdfId: '',

      projectNsTableDataSource: [],
      projectUsersTableDataSource: [],
      projectUdfTableDataSource: []
    }
  }

  componentWillMount () {
    const { roleType } = this.props
    if (roleType === 'admin') {
      this.props.onLoadProjects(false)
    } else if (roleType === 'user') {
      this.props.onLoadUserProjects()
    }
  }

  getIntoProject = (project) => () => {
    const routes = this.props.router.routes
    const routePage = routes.length > 2 ? routes[routes.length - 1].name : 'workbench'
    this.props.router.push(`/project/${project.id}/${routePage}`)
  }

  showAdd = () => {
    this.setState({
      formVisible: true,
      projectFormType: 'add'
    })
    // 显示 project modal 所有的 namespaces & users & udfs
    this.props.onLoadProjectNsAll((result) => this.setState({ projectNsTableDataSource: result }))
    this.props.onLoadProjectUserAll((result) => this.setState({ projectUsersTableDataSource: result }))
    this.props.onLoadProjectUdfs((result) => this.setState({ projectUdfTableDataSource: result }))
  }

  showDetail = (project) => (e) => {
    e.stopPropagation()
    this.setState({
      formVisible: true,
      projectFormType: 'edit'
    })
    new Promise((resolve) => {
      this.props.onLoadSingleProject(project.id, (result) => {
        resolve(result)
      })
    })
      .then((result) => {
        this.setState({
          projectResult: {
            active: result.active,
            createTime: result.createTime,
            createBy: result.createBy,
            updateTime: result.updateTime,
            updateBy: result.updateBy,
            pic: result.pic
          }
        }, () => {
          this.projectForm.setFieldsValue({
            id: result.id,
            pic: result.pic,
            name: result.name,
            desc: result.desc,
            resCores: result.resCores,
            resMemoryG: result.resMemoryG
          })

          // 回显 project modal 所有的 users & 选中的 users
          this.props.onLoadProjectUserAll((result) => this.setState({ projectUsersTableDataSource: result }))
          this.props.onLoadSelectUsers(project.id, (selectUsers) => {
            this.projectUsersTable.setState({ selectedRowKeys: selectUsers.map(n => n.id) })
          })

          // 回显 project modal 所有的 namespaces & 选中的 namespaces
          this.props.onLoadProjectNsAll((result) => this.setState({ projectNsTableDataSource: result }))
          this.props.onLoadSelectNamespaces(project.id, (selectNamespaces) => {
            this.projectNSTable.setState({ selectedRowKeys: selectNamespaces.map(n => n.id) })
          })
        })

        // 回显 project modal 所有的 udfs & 选中的 udfs
        this.props.onLoadProjectUdfs((result) => this.setState({ projectUdfTableDataSource: result }))
        this.props.onLoadSingleUdf(project.id, 'adminSelect', (result) => {
          this.projectUdfTable.setState({ selectedRowKeys: result.map(n => n.id) })
        })
      })
  }

  hideForm = () => {
    this.setState({ formVisible: false })
    this.projectForm.resetFields()
    this.projectNSTable.setState({ selectedRowKeys: [] })
    this.projectUsersTable.setState({ selectedRowKeys: [] })
  }

  onModalOk = () => {
    const { projectFormType, projectResult } = this.state
    const { locale } = this.props

    const userSelectKey = this.projectUsersTable.state.selectedRowKeys
    const udfSelectKey = this.projectUdfTable.state.selectedRowKeys
    const userIds = typeof userSelectKey === 'string'
      ? userSelectKey
      : this.projectUsersTable.state.selectedRowKeys.join(',')

    const udfIds = typeof udfSelectKey === 'string'
      ? udfSelectKey
      : this.projectUdfTable.state.selectedRowKeys === []
        ? ''
        : this.projectUdfTable.state.selectedRowKeys.join(',')

    const { selectedRowKeys } = this.projectNSTable.state

    if (selectedRowKeys.length === 0) {
      message.warning(locale === 'en' ? 'Please select Namespace!' : '请选择源表！', 3)
    } else if (userIds.length === 0) {
      message.warning(locale === 'en' ? 'Please select User!' : '请选择用户！', 3)
    } else {
      const namespaceIds = typeof selectedRowKeys === 'string'
        ? selectedRowKeys
        : selectedRowKeys.join(',')

      this.projectForm.validateFieldsAndScroll((err, values) => {
        if (!err) {
          values.desc = values.desc ? values.desc : ''
          values.resCores = Number(values.resCores)
          values.resMemoryG = Number(values.resMemoryG)

          switch (projectFormType) {
            case 'add':
              this.props.onAddProject(Object.assign(values, {
                nsId: namespaceIds,
                userId: userIds,
                udfId: udfIds,
                pic: Math.ceil(Math.random() * 20)
              }), () => {
                this.hideForm()
              }, () => {
                message.success(locale === 'en' ? 'Project is created successfully!' : 'Project 添加成功！', 3)
              })
              break
            case 'edit':
              this.props.onEditProject(Object.assign(values, {
                nsId: namespaceIds,
                userId: userIds,
                udfId: udfIds
              }, projectResult), () => {
                message.success(locale === 'en' ? 'Project is modified successfully!' : 'Project 修改成功！', 3)
                this.hideForm()
              }, (result) => {
                const nsIdArr = []
                result.payload.nsId.split(',').forEach((i) => nsIdArr.push(Number(i)))
                const userArr = []
                result.payload.userId.split(',').forEach((i) => userArr.push(Number(i)))
                const udfArr = []
                result.payload.udfId.split(',').forEach((i) => udfArr.push(Number(i)))

                message.error(result.header.msg, 5)
                this.projectUdfTable.setState({ selectedRowKeys: udfArr })
                this.projectUsersTable.setState({ selectedRowKeys: userArr })
                this.projectNSTable.setState({ selectedRowKeys: nsIdArr })
              })
              break
          }
        }
      })
    }
  }

  editProjectShowOrHide = (p) => (e) => {
    // e.stopPropagation() 停止事件的传播,该节点上处理该事件的处理程序将被调用，事件不再被分派到其他节点。
    e.stopPropagation()

    let showOrHideValues = {}
    p.active = !p.active

    // 一个异步事件依赖上一个异步事件的返回值。需要上一个事件完成了，再进行下一个事件
    new Promise((resolve) => {
      this.props.onLoadSingleProject(p.id, (result) => {
        resolve(result)
      })
    })
      .then((result) => {
        this.setState({
          projectNsId: result.nsId,
          projectUserId: result.userId,
          projectUdfId: result.udfId
        }, () => {
          const { projectNsId, projectUserId, projectUdfId } = this.state
          showOrHideValues = Object.assign(p, {
            nsId: projectNsId,
            userId: projectUserId,
            udfId: projectUdfId
          })
          new Promise((resolve) => {
            this.props.onEditProject(showOrHideValues, () => {
              resolve()
            }, () => {})
          })
            .then(() => {
              this.props.onLoadProjects(false)
            })
        })
      })
  }

  // 阻止事件的传播，避免点击后进入项目内
  deletePro = (e) => e.stopPropagation()

  deleteAdminProject = (p) => (e) => {
    this.props.onDeleteSingleProject(p.id, () => {}, (result) => {
      message.warning(`${this.props.locale === 'en' ? 'This item cannot be deleted:' : '不能删除：'} ${result}`, 5)
    })
  }

  render () {
    const { projects, modalLoading, roleType } = this.props
    const { projectFormType, formVisible, projectNsTableDataSource, projectUsersTableDataSource, projectUdfTableDataSource } = this.state

    const projectList = projects
      ? this.props.projects.map((p) => {
        const showOrHideBtn = p.active
          ? (
            <Tooltip title={<FormattedMessage {...messages.projectHideProject} />}>
              <Button shape="circle" type="ghost" onClick={this.editProjectShowOrHide(p)}>
                <i className="iconfont icon-yincang"></i>
              </Button>
            </Tooltip>
          )
          : (
            <Tooltip title={<FormattedMessage {...messages.projectShowProject} />}>
              <Button shape="circle" type="ghost" onClick={this.editProjectShowOrHide(p)}>
                <i className="iconfont icon-show1"></i>
              </Button>
            </Tooltip>
          )

        let projectAction = ''
        if (roleType === 'admin') {
          projectAction = (
            <div className="ri-project-item-tools">
              <Tooltip title={<FormattedMessage {...messages.projectEditAuthorize} />}>
                <Button icon="edit" shape="circle" type="ghost" onClick={this.showDetail(p)} />
              </Tooltip>
              {showOrHideBtn}
              <Popconfirm placement="bottom" title="确定删除吗？" okText="Yes" cancelText="No" onConfirm={this.deleteAdminProject(p)}>
                <Tooltip title={<FormattedMessage {...messages.projectDeleteProject} />} onClick={this.deletePro}>
                  <Button icon="delete" shape="circle" type="ghost"></Button>
                </Tooltip>
              </Popconfirm>
            </div>
          )
        } else if (roleType === 'user') {
          projectAction = ''
        }

        return (
          <Col
            key={p.id}
            xs={24} sm={12} md={8} lg={6} xl={4}
          >
            <div
              className={`ri-project-item active ${!p.active ? 'project-hide-style' : ''}`}
              style={{backgroundImage: `url(${require(`../../assets/images/bg${Number(p.pic)}.png`)})`}}
              onClick={this.getIntoProject(p)}
            >
              <header>
                <h2 title={p.name}>{p.name}</h2>
                <p>ID：{p.id}</p>
                <p title={p.desc}>{p.desc}</p>
              </header>
              {projectAction}
              <div className="ri-project-item-bottom"></div>
            </div>
          </Col>
        )
      })
      : null

    let addProject = ''
    if (roleType === 'admin') {
      addProject = (
        <Col xs={24} sm={12} md={8} lg={6} xl={4}>
          <div
            className="ri-project-item active"
            onClick={this.showAdd}
            style={{backgroundImage: `url(${require(`../../assets/images/bg0.png`)})`, padding: '15px'}}>
            <div style={{width: '100%', height: '100%'}}>
              <div className="add-project">
                <Icon type="plus-circle" />
                <h3><FormattedMessage {...messages.projectAddProject} /></h3>
              </div>
            </div>
          </div>
        </Col>
      )
    } else if (roleType === 'user') {
      addProject = ''
    }

    const projectLoading = (
      <div className="general-loading">
        <div className="dot-container">
          <div className="dot"></div>
          <div className="dot"></div>
          <div className="dot"></div>
        </div>
      </div>
    )

    const projectContent = projects
      ? (
        <Row gutter={15}>
          {addProject}
          {projectList}
        </Row>
      )
      : projectLoading

    const modalTitle = projectFormType === 'add'
      ? <FormattedMessage {...messages.projectAddAuthorize} />
      : <FormattedMessage {...messages.projectEditAuthorize} />

    return (
      <div>
        <Helmet title="Project" />
        <div className="ri-project">
          {projectContent}
        </div>
        <Modal
          title={modalTitle}
          okText="保存"
          wrapClassName="ant-modal-small ant-modal-xlarge project-modal"
          visible={formVisible}
          onCancel={this.hideForm}
          footer={[
            <Button
              key="cancel"
              size="large"
              type="ghost"
              onClick={this.hideForm}
            >
              <FormattedMessage {...messages.projectCancelProject} />
            </Button>,
            <Button
              key="submit"
              size="large"
              type="primary"
              loading={modalLoading}
              onClick={this.onModalOk}
            >
              <FormattedMessage {...messages.projectSaveProject} />
            </Button>
          ]}
        >
          <Row className="project-table-style">
            <div className="ant-col-11">
              <ProjectForm
                projectFormType={projectFormType}
                ref={(f) => { this.projectForm = f }}
              />
            </div>
            <div className="ant-col-1"></div>
            <div className="ant-col-11 pro-table-class project-udf-table">
              <ProjectUdfTable
                dataUdf={projectUdfTableDataSource}
                ref={(f) => { this.projectUdfTable = f }}
              />
            </div>
          </Row>

          <Row className="project-table-style">
            <div className="ant-col-11 pro-table-class">
              <ProjectNSTable
                dataNameSpace={projectNsTableDataSource}
                ref={(f) => { this.projectNSTable = f }}
              />
            </div>
            <div className="ant-col-1"></div>
            <div className="ant-col-11 pro-table-class">
              <ProjectUsersTable
                dataUsers={projectUsersTableDataSource}
                ref={(f) => { this.projectUsersTable = f }}
              />
            </div>
          </Row>
        </Modal>
      </div>
    )
  }
}

Project.propTypes = {
  router: PropTypes.any,
  projects: PropTypes.oneOfType([
    PropTypes.array,
    PropTypes.bool
  ]),
  modalLoading: PropTypes.bool,
  onLoadProjects: PropTypes.func,
  onLoadUserProjects: PropTypes.func,
  onLoadSingleProject: PropTypes.func,
  onLoadSelectNamespaces: PropTypes.func,
  onLoadSelectUsers: PropTypes.func,
  onLoadSingleUdf: PropTypes.func,
  onAddProject: PropTypes.func,
  onEditProject: PropTypes.func,
  onDeleteSingleProject: PropTypes.func,

  onLoadProjectNsAll: PropTypes.func,
  onLoadProjectUserAll: PropTypes.func,
  onLoadProjectUdfs: PropTypes.func,
  roleType: PropTypes.string,
  locale: PropTypes.string
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadProjects: (visible) => dispatch(loadProjects(visible)),
    onLoadUserProjects: () => dispatch(loadUserProjects()),
    onLoadSingleProject: (id, resolve) => dispatch(loadSingleProject(id, resolve)),
    onLoadSelectNamespaces: (projectId, resolve) => dispatch(loadSelectNamespaces(projectId, resolve)),
    onLoadSelectUsers: (projectId, resolve) => dispatch(loadSelectUsers(projectId, resolve)),
    onLoadSingleUdf: (projectId, roleType, resolve) => dispatch(loadSingleUdf(projectId, roleType, resolve)),
    onAddProject: (project, resolve, final) => dispatch(addProject(project, resolve, final)),
    onEditProject: (project, resolve, reject) => dispatch(editProject(project, resolve, reject)),
    onDeleteSingleProject: (projectId, resolve, reject) => dispatch(deleteSingleProject(projectId, resolve, reject)),

    onLoadProjectNsAll: (resolve) => dispatch(loadProjectNsAll(resolve)),
    onLoadProjectUserAll: (resolve) => dispatch(loadProjectUserAll(resolve)),
    onLoadProjectUdfs: (resolve) => dispatch(loadProjectUdfs(resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  currentProject: selectCurrentProject(),
  projects: selectProjects(),
  modalLoading: selectModalLoading(),
  namespaces: selectNamespaces(),
  users: selectUsers(),
  roleType: selectRoleType(),
  locale: selectLocale()
})

export default connect(mapStateToProps, mapDispatchToProps)(Project)
