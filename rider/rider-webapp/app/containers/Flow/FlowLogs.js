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

import Form from 'antd/lib/form'
import Button from 'antd/lib/button'

export class FlowLogs extends React.Component {
  refreshLogs = () => {
    this.props.onInitRefreshLogs(this.props.logsProjectId, this.props.logsFlowId)
  }

  render = (text, record) => {
    const { logsContent, refreshLogLoading, refreshLogText } = this.props

    const logsContentFinal = logsContent.replace(/\n/g, '\n')

    return (
      <div>
        <div className="logs-modal-style">
          <span className="logs-btn-style">
            <Button
              icon="reload"
              type="ghost"
              loading={refreshLogLoading}
              onClick={this.refreshLogs}
              className="logs-refresh-style refresh-button-style"
            >
              {refreshLogText}
            </Button>
          </span>
        </div>

        <div className="logs-content">
          <pre>
            {logsContentFinal}
          </pre>
        </div>
      </div>
    )
  }
}

FlowLogs.propTypes = {
  logsContent: PropTypes.string,
  onInitRefreshLogs: PropTypes.func,
  logsProjectId: PropTypes.number,
  logsFlowId: PropTypes.number,
  refreshLogLoading: PropTypes.bool,
  refreshLogText: PropTypes.string
}

export default Form.create({wrappedComponentRef: true})(FlowLogs)
