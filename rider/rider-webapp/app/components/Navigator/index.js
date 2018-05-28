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

import { FormattedMessage } from 'react-intl'
import messages from './messages'

import UserPswForm from './UserPswForm'
import Menu from 'antd/lib/menu'
import Icon from 'antd/lib/icon'
import Tooltip from 'antd/lib/tooltip'
import Button from 'antd/lib/button'
import Modal from 'antd/lib/modal'
import message from 'antd/lib/message'
const MenuItem = Menu.Item
const SubMenu = Menu.SubMenu

import { logOut, setProject } from '../../containers/App/actions'
import { changeLocale } from '../../containers/LanguageProvider/actions'

import { editroleTypeUserPsw, loadUserDetail, editUser, loadNormalDetail, editNormal } from '../../containers/User/action'
import { selectModalLoading } from '../../containers/User/selectors'
import { selectLocale } from '../../containers/LanguageProvider/selectors'

import { selectCurrentProject } from '../../containers/App/selectors'

import request from '../../utils/request'

class Navigator extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      selectedKey: '',
      formVisible: false,
      paneWidthValue: -1,
      menuInlineShow: false // menu inline 是否展开
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

  componentDidMount () {
    // 自动获取浏览器窗口大小
    window.onresize = () => {
      const paneWidth = document.documentElement.clientWidth - 228 - 198 - 64

      this.setState({
        paneWidthValue: paneWidth,
        menuInlineShow: false
      })
    }
  }

  // MenuItem 高亮
  handleProjectState = (props) => {
    props.onSetProject(props.params.projectId || '')

    const routes = props.router.routes
    this.state.selectedKey = routes[routes.length - 1].name
  }

  navClickSubmenu = (e) => {
    const { router } = this.props
    router.push('/namespaces')
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
      case 'jobs':
        router.push('/jobs')
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

    this.setState({ menuInlineShow: false })
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

  showEditPsw = () => this.setState({ formVisible: true })

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

  toggleCollapsed = () => {
    this.setState({
      menuInlineShow: !this.state.menuInlineShow
    })
  }

  logoImgStyle (width) {
    let logoImgClass = ''
    if (width > -12 || width === -12 || width > 414 || width === 414) {
      logoImgClass = 'ri-logo-img'
    } else if ((width < -12 && width > -106) || (width < 414 && width > 320)) {
      logoImgClass = 'ri-logo-img-reduce'
    } else if (width < -106 || width === -106 || width < 320 || width === 320) {
      logoImgClass = 'ri-logo-img-reduce-reduce'
    }
    return logoImgClass
  }

  changelanguageRequestValue (result) {
    const requestValue = {
      id: result.id,
      email: result.email,
      password: result.password,
      name: result.name,
      roleType: result.roleType,
      preferredLanguage: result.preferredLanguage === 'chinese' ? 'english' : 'chinese',
      active: result.active,
      createTime: result.createTime,
      createBy: result.createBy,
      updateTime: result.updateTime,
      updateBy: result.updateBy
    }
    return requestValue
  }

  onChangeLanguageBtn = () => {
    const { locale } = this.props

    const userRoleType = localStorage.getItem('loginRoleType')
    const userId = localStorage.getItem('loginId')

    switch (userRoleType) {
      case 'admin':
        new Promise((resolve) => {
          // 获取最新的user信息
          this.props.onLoadUserDetail(userId, (result) => {
            resolve(result)
          })
        }).then((result) => {
          // 相当于更改用户信息
          this.props.onEditUser(this.changelanguageRequestValue(result), () => {})
        })
        break
      case 'user':
        new Promise((resolve) => {
          this.props.onLoadNormalDetail(userId, (result) => {
            resolve(result)
          })
        }).then((result) => {
          this.props.onEditNormal(this.changelanguageRequestValue(result), () => {})
        })
        break
    }

    const langText = locale === 'zh' ? 'en' : 'zh'
    this.props.onChangeLanguage(langText)
    localStorage.setItem('preferredLanguage', langText)
  }

  render () {
    const { selectedKey, paneWidthValue, menuInlineShow } = this.state

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

    const logNameShow = localStorage.getItem('loginName')

    const menuHtml = (
      <Menu
        className="ri-menu"
        mode="horizontal"
        theme="dark"
        selectedKeys={[selectedKey]}
        onClick={this.navClick}
      >
        <MenuItem key="projects" className={`ri-menu-item ${projectSelectedClass} ri-menu-item-icon`}>
          <Icon type="appstore-o" className="item-icon-extra" />Project
        </MenuItem>
        <MenuItem key="flows" className="ri-menu-item ri-menu-item-icon">
          <i className="iconfont icon-flow item-i-extra"></i>Flow
        </MenuItem>
        <MenuItem key="streams" className="ri-menu-item ri-menu-item-icon">
          <Icon type="barcode" className="item-icon-extra" />Stream
        </MenuItem>

        <MenuItem key="jobs" className="ri-menu-item ri-menu-item-icon">
          <i className="iconfont icon-job item-i-extra"></i>Job
        </MenuItem>

        <SubMenu
          onTitleClick={this.navClickSubmenu}
          key="dataSystem"
          className={`ri-menu-item ${dataSystemSelectedClass}`}
          title={
            <span>
              <i className="iconfont icon-system-copy item-i-extra"></i>Namespace<Icon type="down" className="arrow" />
            </span>
          }>
          <MenuItem key="namespaces">
            <Icon type="menu-unfold" className="item-icon-extra" />Namespace
          </MenuItem>
          <MenuItem key="instance">
            <i className="iconfont icon-instanceobjgcroot item-i-extra"></i>Instance
          </MenuItem>
          <MenuItem key="database">
            <Icon type="database" className="item-icon-extra" />Database
          </MenuItem>
        </SubMenu>

        <MenuItem key="users" className="ri-menu-item ri-menu-item-icon">
          <Icon type="solution" className="item-icon-extra" />User
        </MenuItem>
        <MenuItem key="udf" className="ri-menu-item ri-menu-item-icon">
          <Icon type="menu-fold" className="item-icon-extra" />UDF
        </MenuItem>

        <MenuItem key="riderInfo" className="ri-menu-item ri-menu-item-icon">
          <i className="iconfont icon-infor item-i-extra"></i>Rider Info
        </MenuItem>
      </Menu>
    )

    const menuInlineHtml = (
      <Menu
        className="ri-menu-inline"
        mode="inline"
        theme="dark"
        selectedKeys={[selectedKey]}
        onClick={this.navClick}
      >
        <MenuItem key="projects" className={`ri-menu-item ${projectSelectedClass} ri-menu-item-icon`}>
          <Icon type="appstore-o" className="item-icon-extra" />Project
        </MenuItem>
        <MenuItem key="flows" className="ri-menu-item ri-menu-item-icon">
          <i className="iconfont icon-flow item-i-extra"></i>Flow
        </MenuItem>
        <MenuItem key="streams" className="ri-menu-item ri-menu-item-icon">
          <Icon type="barcode" className="item-icon-extra" />Stream
        </MenuItem>

        <MenuItem key="jobs" className="ri-menu-item ri-menu-item-icon">
          <i className="iconfont icon-job item-i-extra"></i>Job
        </MenuItem>

        <SubMenu
          onTitleClick={this.navClickSubmenu}
          key="dataSystem"
          className={`ri-menu-item ${dataSystemSelectedClass}`}
          title={
            <span>
              <i className="iconfont icon-system-copy item-i-extra"></i>Namespace
            </span>
          }
        >
          <MenuItem key="namespaces" style={{ backgroundColor: '#424242' }}>
            <Icon type="menu-unfold" className="item-icon-extra" />Namespace
          </MenuItem>
          <MenuItem key="instance" style={{ backgroundColor: '#424242' }}>
            <i className="iconfont icon-instanceobjgcroot item-i-extra"></i>Instance
          </MenuItem>
          <MenuItem key="database" style={{ backgroundColor: '#424242' }}>
            <Icon type="database" className="item-icon-extra" />Database
          </MenuItem>
        </SubMenu>

        <MenuItem key="users" className="ri-menu-item ri-menu-item-icon">
          <Icon type="solution" className="item-icon-extra" />User
        </MenuItem>
        <MenuItem key="udf" className="ri-menu-item ri-menu-item-icon">
          <Icon type="menu-fold" className="item-icon-extra" />UDF
        </MenuItem>

        <MenuItem key="riderInfo" className="ri-menu-item ri-menu-item-icon">
          <i className="iconfont icon-infor item-i-extra"></i>Rider Info
        </MenuItem>
      </Menu>
    )

    const paneWidth = document.documentElement.clientWidth - 228 - 198

    let node = ''
    let logoImgClass = ''
    if (localStorage.getItem('loginRoleType') === 'admin') {
      if (paneWidthValue === -1) {
        if (paneWidth < 824) {
          node = (
            <span>
              <div className="nav--window-reduce" onClick={this.toggleCollapsed}>
                <i className="iconfont icon-1111111"></i>
              </div>
              {menuInlineShow ? menuInlineHtml : ''}
            </span>
          )
          logoImgClass = this.logoImgStyle(paneWidth)
        } else {
          node = menuHtml
          logoImgClass = 'ri-logo-img'
        }
      } else {
        if (paneWidthValue < 824) {
          node = (
            <span>
              <div className="nav--window-reduce" onClick={this.toggleCollapsed}>
                <i className="iconfont icon-1111111"></i>
              </div>
              {menuInlineShow ? menuInlineHtml : ''}
            </span>
          )
          logoImgClass = this.logoImgStyle(paneWidthValue)
        } else {
          node = menuHtml
          logoImgClass = 'ri-logo-img'
        }
      }
    } else if (localStorage.getItem('loginRoleType') === 'user') {
      node = ''
      if (paneWidthValue === -1) {
        logoImgClass = paneWidth < 824 ? this.logoImgStyle(paneWidth) : 'ri-logo-img'
      } else {
        logoImgClass = paneWidthValue < 824 ? this.logoImgStyle(paneWidthValue) : 'ri-logo-img'
      }
    }

    return (
      <header className="ri-header">
        <nav>
          <div className="ri-logo-href" onClick={this.logoChange}>
            <img src={require(`../../assets/images/logo.png`)} alt="Wormhole" className={logoImgClass} />
          </div>
          {node}

          <Button
            type="primary"
            size="small"
            className="language-change"
            onClick={this.onChangeLanguageBtn}
          >
            <FormattedMessage {...messages.navChangeLanguage} />
          </Button>
          <Tooltip title={<FormattedMessage {...messages.navChangePsw} />}>
            <a title={logNameShow} className="user-login" onClick={this.showEditPsw}>{logNameShow}</a>
          </Tooltip>
          <div
            className="ri-logout"
            onClick={this.logout}
          >
            <Icon type="logout" />
          </div>

          <Modal
            title={<FormattedMessage {...messages.navChangePsw} />}
            okText="保存"
            wrapClassName="db-form-style"
            visible={this.state.formVisible}
            onCancel={this.hideForm}
            footer={[
              <Button
                key="cancel"
                size="large"
                type="ghost"
                onClick={this.hideForm}
              >
                <FormattedMessage {...messages.navModalCancel} />
              </Button>,
              <Button
                key="submit"
                size="large"
                type="primary"
                loading={this.props.modalLoading}
                onClick={this.onModalOk}
              >
                <FormattedMessage {...messages.navModalSave} />
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
    onEditroleTypeUserPsw: (pwdValues, resolve, reject) => dispatch(editroleTypeUserPsw(pwdValues, resolve, reject)),
    onLoadUserDetail: (userId, resolve) => dispatch(loadUserDetail(userId, resolve)),
    onLoadNormalDetail: (userId, resolve) => dispatch(loadNormalDetail(userId, resolve)),
    onEditUser: (values, resolve) => dispatch(editUser(values, resolve)),
    onEditNormal: (values, resolve) => dispatch(editNormal(values, resolve)),
    onChangeLanguage: (type) => dispatch(changeLocale(type))
  }
}

const mapStateToProps = createStructuredSelector({
  currentProject: selectCurrentProject(),
  modalLoading: selectModalLoading(),
  locale: selectLocale()
})

Navigator.propTypes = {
  currentProject: PropTypes.string,
  modalLoading: PropTypes.bool,
  router: PropTypes.any,
  onLogOut: PropTypes.func,
  onEditroleTypeUserPsw: PropTypes.func,
  onLoadUserDetail: PropTypes.func,
  onLoadNormalDetail: PropTypes.func,
  onEditUser: PropTypes.func,
  onChangeLanguage: PropTypes.func,
  onEditNormal: PropTypes.func,
  locale: PropTypes.string
}

export default connect(mapStateToProps, mapDispatchToProps)(Navigator)
