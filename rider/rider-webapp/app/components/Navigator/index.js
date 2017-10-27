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

import UserPswForm from './UserPswForm'
import Menu from 'antd/lib/menu'
import Icon from 'antd/lib/icon'
import Tooltip from 'antd/lib/tooltip'
import Button from 'antd/lib/button'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
const MenuItem = Menu.Item
const SubMenu = Menu.SubMenu

import { logOut } from '../../containers/Login/action'

import { editroleTypeUserPsw } from '../../containers/User/action'
import { selectModalLoading } from '../../containers/User/selectors'

import { selectCurrentProject } from '../../containers/App/selectors'
import { setProject } from '../../containers/App/actions'

import request from '../../utils/request'

class Navigator extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      selectedKey: '',
      formVisible: false
    }
  }

  componentWillMount () {
    localStorage.getItem('loginRoleType')
    this.handleProjectState(this.props)
  }

  componentWillReceiveProps (props) {
    if (this.props.currentProject !== props.params.projectId) {
      this.handleProjectState(props)
    }
  }

  // MenuItem 高亮
  handleProjectState = (props) => {
    props.onSetProject(props.params.projectId || '')

    const routes = props.router.routes
    this.state.selectedKey = routes[routes.length - 1].name
  }

  navClick = (e) => {
    const { router } = this.props

    switch (e.key) {
      case 'projects':
        router.push('/projects')
        break
      case 'flows':
        router.push('/flows')
        break
      case 'streams':
        router.push('/streams')
        break
      case 'namespaces':
        router.push('/namespaces')
        break
      case 'instance':
        router.push('/instance')
        break
      case 'database':
        router.push('/database')
        break
      case 'users':
        router.push('/users')
        break
      case 'udf':
        router.push('/udf')
        break
      case 'riderInfo':
        router.push('/riderInfo')
        break
      default:
        break
    }
  }

  logout = () => {
    localStorage.removeItem('token')
    request.removeToken()
    this.props.router.push('/login')
    this.props.onLogOut()
  }

  logoChange = () => {
    this.props.router.push('/projects')
  }

  showEditPsw = () => {
    this.setState({
      formVisible: true
    })
  }

  hideForm = () => {
    this.setState({
      formVisible: false
    })
    this.userPswForm.resetFields()
  }

  onModalOk = () => {
    this.userPswForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const requestValue = {
          id: Number(localStorage.getItem('loginId')),
          oldPass: values.oldPassword,
          newPass: values.password
        }
        this.props.onEditroleTypeUserPsw(requestValue, () => {
          message.success('密码修改成功！', 3)
          this.hideForm()
        }, (result) => {
          if (result === 'Wrong password') {
            message.error('旧密码错误！', 3)
          }
        })
      }
    })
  }

  render () {
    const { selectedKey } = this.state

    // SubMenu 高亮
    let dataSystemSelectedClass = ''
    let projectSelectedClass = ''
    if (selectedKey === 'instance' || selectedKey === 'database') {
      dataSystemSelectedClass = 'ant-menu-item-selected'
    }
    // 出现二级菜单时，一级菜单对应高亮
    if (selectedKey === 'workbench' || selectedKey === 'performance') {
      projectSelectedClass = 'ant-menu-item-selected'
    }

    let node = ''
    if (localStorage.getItem('loginRoleType') === 'admin') {
      node = (
        <Menu
          className="ri-menu"
          mode="horizontal"
          theme="dark"
          selectedKeys={[selectedKey]}
          onClick={this.navClick}
        >
          <MenuItem key="projects" className={`ri-menu-item ${projectSelectedClass}`}>
            <Icon type="appstore-o" />Project
          </MenuItem>
          <MenuItem key="flows" className="ri-menu-item">
            <i className="iconfont icon-flow"></i>Flow
          </MenuItem>
          <MenuItem key="streams" className="ri-menu-item">
            <i className="iconfont icon-318stream-copy"></i>Stream
          </MenuItem>

          <SubMenu
            key="dataSystem"
            className={`ri-menu-item ${dataSystemSelectedClass}`}
            title={
              <span>
                <i className="iconfont icon-system-copy"></i>Namespace<Icon type="down" className="arrow" />
              </span>
            }>
            <MenuItem key="namespaces">
              <Icon type="menu-unfold" />Namespace
            </MenuItem>
            <MenuItem key="instance">
              <i className="iconfont icon-instanceobjgcroot"></i>Instance
            </MenuItem>
            <MenuItem key="database">
              <Icon type="database" />Database
            </MenuItem>
          </SubMenu>

          <MenuItem key="users" className="ri-menu-item">
            <Icon type="solution" />User
          </MenuItem>

          <MenuItem key="udf" className="ri-menu-item">
            <i className="iconfont icon-function" style={{ marginRight: '6px' }}></i>UDF
          </MenuItem>

          <MenuItem key="riderInfo" className="ri-menu-item">
            <i className="iconfont icon-infor"></i>Rider Info
          </MenuItem>
        </Menu>
      )
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      node = ''
    }

    const logNameShow = localStorage.getItem('loginName')

    return (
      <header className="ri-header">
        <nav>
          <div className="ri-logo-href" onClick={this.logoChange}><div className="ri-logo" ></div></div>
          {node}
          <div>
            <Tooltip title="修改密码">
              <a title={logNameShow} className="user-login" onClick={this.showEditPsw}>{logNameShow}</a>
            </Tooltip>
            <div
              className="ri-logout"
              onClick={this.logout}
            >
              <Icon type="logout" />
            </div>
          </div>

          <Modal
            title="修改密码"
            okText="保存"
            wrapClassName="ant-modal-small"
            visible={this.state.formVisible}
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
            <UserPswForm
              type={this.state.formType}
              ref={(f) => { this.userPswForm = f }}
            />
          </Modal>

        </nav>
      </header>
    )
  }
}

export function mapDispatchToProps (dispatch) {
  return {
    onSetProject: (projectId) => dispatch(setProject(projectId)),
    onLogOut: () => dispatch(logOut()),
    onEditroleTypeUserPsw: (pwdValues, resolve, reject) => dispatch(editroleTypeUserPsw(pwdValues, resolve, reject))
  }
}

const mapStateToProps = createStructuredSelector({
  currentProject: selectCurrentProject(),
  modalLoading: selectModalLoading()
})

Navigator.propTypes = {
  currentProject: React.PropTypes.string,
  modalLoading: React.PropTypes.bool,
  router: React.PropTypes.any,
  onLogOut: React.PropTypes.func,
  onEditroleTypeUserPsw: React.PropTypes.func
}

export default connect(mapStateToProps, mapDispatchToProps)(Navigator)
