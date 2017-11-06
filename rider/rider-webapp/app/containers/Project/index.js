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

import ProjectForm from './ProjectForm'
import ProjectNSTable from './ProjectNSTable'
import ProjectUdfTable from './ProjectUdfTable'
import ProjectUsersTable from './ProjectUsersTable'

import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
import Icon from 'antd/lib/icon'
import Button from 'antd/lib/button'
import Modal from 'antd/lib/modal'
import Popconfirm from 'antd/lib/popconfirm'
import Tooltip from 'antd/lib/tooltip'
import message from 'antd/lib/message'

import { selectCurrentProject } from '../App/selectors'
import { selectProjects, selectModalLoading, selectProjectNameExited } from './selectors'
import { selectNamespaces } from '../Namespace/selectors'
import { selectUsers } from '../User/selectors'

import { loadProjects, loadUserProjects, addProject, editProject, loadProjectNameInputValue, loadSingleProject, deleteSingleProject } from './action'
import { loadSelectNamespaces, loadProjectNsAll } from '../Namespace/action'
import { loadSelectUsers, loadProjectUserAll } from '../User/action'
import { loadSingleUdf, loadProjectUdfs } from '../Udf/action'

export class Project extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      formVisible: false,
      projectFormType: 'add',
      projectResult: {},
      projectUserId: '',
      projectNsId: '',

      projectNsTableDataSource: [],
      projectUsersTableDataSource: [],
      projectUdfTableDataSource: []
    }
  }

  componentWillMount () {
    if (localStorage.getItem('loginRoleType') === 'admin') {
      this.props.onLoadProjects(false)
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      this.props.onLoadUserProjects()
    }
  }

  getIntoProject = (project) => () => {
    const routes = this.props.router.routes
    const routePage = routes.length > 2 ? routes[routes.length - 1].name : 'workbench'
    this.props.router.push(`/project/${project.id}/${routePage}`)
  }

  /***
   * 新增时，验证 project name 是否存在
   * */
  onInitProjectNameInputValue = (value) => {
    this.props.onLoadProjectNameInputValue(value, () => {}, () => {
      this.projectForm.setFields({
        name: {
          value: value,
          errors: [new Error('该 Project Name 已存在')]
        }
      })
    })
  }

  showAdd = () => {
    this.setState({
      formVisible: true,
      projectFormType: 'add'
    })
    // 显示 project modal 所有的 namespaces & users & udfs
    this.props.onLoadProjectNsAll((result) => {
      this.setState({
        projectNsTableDataSource: result
      })
    })
    this.props.onLoadProjectUserAll((result) => {
      this.setState({
        projectUsersTableDataSource: result
      })
    })

    this.props.onLoadProjectUdfs((result) => {
      this.setState({
        projectUdfTableDataSource: result
      })
    })
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
          this.props.onLoadProjectUserAll((result) => {
            this.setState({
              projectUsersTableDataSource: result
            })
          })

          this.props.onLoadSelectUsers(project.id, (selectUsers) => {
            this.projectUsersTable.setState({
              selectedRowKeys: selectUsers.map(n => n.id)
            })
          })

          // 回显 project modal 所有的 namespaces & 选中的 namespaces
          this.props.onLoadProjectNsAll((result) => {
            this.setState({
              projectNsTableDataSource: result
            })
          })

          this.props.onLoadSelectNamespaces(project.id, (selectNamespaces) => {
            this.projectNSTable.setState({
              selectedRowKeys: selectNamespaces.map(n => n.id)
            })
          })
        })

        // 回显 project modal 所有的 udfs & 选中的 udfs
        this.props.onLoadProjectUdfs((result) => {
          this.setState({
            projectUdfTableDataSource: result
          })
        })

        this.props.onLoadSingleUdf(project.id, 'adminSelect', (result) => {
          this.projectUdfTable.setState({
            selectedRowKeys: result.map(n => n.id)
          })
        })
      })
  }

  hideForm = () => {
    this.setState({
      formVisible: false
    })
    this.projectForm.resetFields()
    this.projectNSTable.setState({
      selectedRowKeys: []
    })
    this.projectUsersTable.setState({
      selectedRowKeys: []
    })
  }

  onModalOk = () => {
    const { projectFormType, projectResult } = this.state
    const { projectNameExited } = this.props

    const userIds = this.projectUsersTable.state.selectedRowKeys.join(',')

    const udfIds = this.projectUdfTable.state.selectedRowKeys === []
      ? ''
      : this.projectUdfTable.state.selectedRowKeys.join(',')

    const { selectedRowKeys } = this.projectNSTable.state

    if (selectedRowKeys.length === 0) {
      message.warning('请选择源表！', 3)
    } else if (userIds.length === 0) {
      message.warning('请选择用户！', 3)
    } else {
      const namespaceIds = selectedRowKeys.join(',')

      this.projectForm.validateFieldsAndScroll((err, values) => {
        if (!err) {
          values.desc = values.desc ? values.desc : ''
          values.resCores = Number(values.resCores)
          values.resMemoryG = Number(values.resMemoryG)

          if (projectFormType === 'add') {
            if (projectNameExited === true) {
              this.projectForm.setFields({
                name: {
                  value: values.name,
                  errors: [new Error('该 Project Name 已存在')]
                }
              })
            } else {
              this.props.onAddProject(Object.assign({}, values, {
                nsId: namespaceIds,
                userId: userIds,
                udfId: udfIds,
                pic: Math.ceil(Math.random() * 20)
              }), () => {
                this.hideForm()
              }, () => {
                message.success('Project 添加成功！', 3)
              })
            }
          } else if (projectFormType === 'edit') {
            this.props.onEditProject(Object.assign({}, values, {
              nsId: namespaceIds,
              userId: userIds,
              udfId: udfIds
            }, projectResult), () => {
              this.hideForm()
            }, () => {
              message.success('Project 修改成功！', 3)
            })
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
          projectUserId: result.userId
        }, () => {
          showOrHideValues = Object.assign({}, p, { nsId: this.state.projectNsId }, { userId: this.state.projectUserId })
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
  deletePro = (e) => {
    e.stopPropagation()
  }

  deleteAdminProject = (p) => (e) => {
    this.props.onDeleteSingleProject(p.id, () => {}, (result) => {
      message.warning(`不能删除：${result}`, 5)
    })
  }

  render () {
    const { projects } = this.props

    const projectList = projects
      ? this.props.projects.map((p) => {
        const showOrHideBtn = p.active === true
          ? (
            <Tooltip title="隐藏">
              <Button shape="circle" type="ghost" onClick={this.editProjectShowOrHide(p)}>
                <i className="iconfont icon-yincang"></i>
              </Button>
            </Tooltip>
          )
          : (
            <Tooltip title="显示">
              <Button shape="circle" type="ghost" onClick={this.editProjectShowOrHide(p)}>
                <i className="iconfont icon-show1"></i>
              </Button>
            </Tooltip>
          )

        let projectAction = ''
        if (localStorage.getItem('loginRoleType') === 'admin') {
          projectAction = (
            <div className="ri-project-item-tools">
              <Tooltip title="修改 & 授权">
                <Button icon="edit" shape="circle" type="ghost" onClick={this.showDetail(p)} />
              </Tooltip>
              {showOrHideBtn}
              <Popconfirm placement="bottom" title="确定删除吗？" okText="Yes" cancelText="No" onConfirm={this.deleteAdminProject(p)}>
                <Tooltip title="删除" onClick={this.deletePro}>
                  <Button icon="delete" shape="circle" type="ghost"></Button>
                </Tooltip>
              </Popconfirm>
            </div>
          )
        } else if (localStorage.getItem('loginRoleType') === 'user') {
          projectAction = ''
        }

        return (
          <Col
            key={p.id}
            xs={24} sm={12} md={8} lg={6} xl={4}
          >
            <div
              className={`ri-project-item active ${p.active === false ? 'project-hide-style' : ''}`}
              // style={{backgroundImage: `url(${require(`../../assets/images/bg20.png`)})`}}
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
    if (localStorage.getItem('loginRoleType') === 'admin') {
      addProject = (
        <Col xs={24} sm={12} md={8} lg={6} xl={4}>
          <div
            className="ri-project-item active"
            onClick={this.showAdd}
            style={{backgroundImage: `url(${require(`../../assets/images/bg0.png`)})`, padding: '15px'}}>
            <div style={{width: '100%', height: '100%'}}>
              <div className="add-project">
                <Icon type="plus-circle" />
                <h3>新建项目</h3>
              </div>
            </div>
          </div>
        </Col>
      )
    } else if (localStorage.getItem('loginRoleType') === 'user') {
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

    const { projectFormType, formVisible, projectNsTableDataSource, projectUsersTableDataSource, projectUdfTableDataSource } = this.state

    return (
      <div>
        <Helmet title="Project" />
        <div className="ri-project">
          {projectContent}
        </div>
        <Modal
          title={`${projectFormType === 'add' ? '新建' : '修改'} & 授权`}
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
          <Row className="project-table-style">
            <div className="ant-col-11">
              <ProjectForm
                projectFormType={projectFormType}
                onInitProjectNameInputValue={this.onInitProjectNameInputValue}
                ref={(f) => { this.projectForm = f }}
              />
            </div>
            <div className="ant-col-1"></div>
            <div className="ant-col-11 pro-table-class">
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
  router: React.PropTypes.any,
  projects: React.PropTypes.oneOfType([
    React.PropTypes.array,
    React.PropTypes.bool
  ]),
  modalLoading: React.PropTypes.bool,
  projectNameExited: React.PropTypes.bool,
  onLoadProjects: React.PropTypes.func,
  onLoadUserProjects: React.PropTypes.func,
  onLoadSingleProject: React.PropTypes.func,
  onLoadSelectNamespaces: React.PropTypes.func,
  onLoadSelectUsers: React.PropTypes.func,
  onLoadSingleUdf: React.PropTypes.func,
  onAddProject: React.PropTypes.func,
  onEditProject: React.PropTypes.func,
  onLoadProjectNameInputValue: React.PropTypes.func,
  onDeleteSingleProject: React.PropTypes.func,

  onLoadProjectNsAll: React.PropTypes.func,
  onLoadProjectUserAll: React.PropTypes.func,
  onLoadProjectUdfs: React.PropTypes.func
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
    onEditProject: (project, resolve, final) => dispatch(editProject(project, resolve, final)),
    onLoadProjectNameInputValue: (value, resolve, reject) => dispatch(loadProjectNameInputValue(value, resolve, reject)),
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
  projectNameExited: selectProjectNameExited(),
  namespaces: selectNamespaces(),
  users: selectUsers()
})

export default connect(mapStateToProps, mapDispatchToProps)(Project)
