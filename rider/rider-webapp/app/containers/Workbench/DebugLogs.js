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
import {connect} from 'react-redux'
import Button from 'antd/lib/button'

export class DebugLogs extends React.Component {

  constructor (props) {
    super(props)
    this.state = {
      logContent: '',
      socket: null
    }
  }

  componentDidMount () {
    const logPath = this.props.logPath
    const productionHost = window._RIDER_PRODUCTION_HOST.replace('http://', 'ws://')
    const socket = new WebSocket(`${productionHost}/debug/ws`)
    socket.onopen = () => {
      socket.send(`{"action": "read", "logPath": "${logPath}"}`)
    }
    socket.onmessage = (msg) => {
      let { logContent } = this.state
      logContent += msg['data']
      this.setState({
        logContent: logContent
      })
    }
    this.setState({
      socket: socket
    })
  }

  componentWillUnmount () {
    const { socket } = this.state
    socket.close()
  }

  render () {
    const {logContent} = this.state
    return (
      <div>
        <div className="logs-modal-style">
          <span className="logs-btn-style">
            <Button
              icon="reload"
              type="ghost"
              loading
              onClick={this.refreshLogs}
              className="logs-refresh-style refresh-button-style"
            >
              {'outputing'}
            </Button>
          </span>
        </div>

        <div className="logs-content">
          <pre>
            {logContent}
          </pre>
        </div>
      </div>
    )
  }

}

DebugLogs.propTypes = {
  logPath: PropTypes.string
}

export default connect()(DebugLogs)
