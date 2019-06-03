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

import Menu from 'antd/lib/menu'
import Icon from 'antd/lib/icon'

const MenuItem = Menu.Item

import { loadSingleProject } from '../../containers/Project/action'
import { selectCurrentProject } from '../../containers/App/selectors'
import { setProject } from '../../containers/App/actions'

class SecondNavigator extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      selectedKey: '',
      singleProjectName: ''
    }
  }

  componentWillMount () {
    localStorage.getItem('loginRoleType')
    this.handleSecondNavState(this.props)
    this.props.onLoadSingleProject(this.props.params.projectId, (result) => {
      this.setState({
        singleProjectName: result.name
      })
    })
  }

  componentWillReceiveProps (props) {
    if (props.currentProject !== props.params.projectId) {
      this.handleSecondNavState(props)
    }
    this.secondNavLight()
  }

  // 默认高亮的二级菜单
  handleSecondNavState = (props) => {
    props.onSetProject(props.params.projectId || '')
    this.secondNavLight()
  }

  secondNavClick = (e) => {
    const { router } = this.props
    router.push(`/project/${this.props.params.projectId}/${e.key}`)
    this.secondNavLight()
  }

  // 二级菜单高亮
  secondNavLight () {
    const routes = this.props.router.routes
    this.state.selectedKey = routes[routes.length - 1].name
  }

  render () {
    const { selectedKey, singleProjectName } = this.state

    const secondNavSelectedClass = [
      selectedKey === 'workbench' ? 'ant-menu-item-selected' : '',
      selectedKey === 'performance' ? 'ant-menu-item-selected' : ''
    ]

    return (
      <div className="second-ri-header">
        <span className="project-name"><Icon type="right" /> {singleProjectName}</span>
        <Menu
          className="second-ri-menu"
          mode="horizontal"
          theme="dark"
          selectedKeys={[selectedKey]}
          onClick={this.secondNavClick}>

          <MenuItem key="workbench" className={`ri-menu-item ri-menu-item-extra ${secondNavSelectedClass[0]}`}>
            <Icon type="desktop" />Workbench
          </MenuItem>
          {/* <MenuItem key="performance" className={`ri-menu-item ri-menu-item-extra ${secondNavSelectedClass[1]}`}>
            <Icon type="bar-chart" />Performance
          </MenuItem> */}
        </Menu>
      </div>
    )
  }
}

export function mapDispatchToProps (dispatch) {
  return {
    onSetProject: (projectId) => dispatch(setProject(projectId)),
    onLoadSingleProject: (id, resolve) => dispatch(loadSingleProject(id, resolve))
  }
}

const mapStateToProps = createStructuredSelector({
  currentProject: selectCurrentProject()
})

SecondNavigator.propTypes = {
  router: PropTypes.any,
  params: PropTypes.any,
  onLoadSingleProject: PropTypes.func
}

export default connect(mapStateToProps, mapDispatchToProps)(SecondNavigator)
