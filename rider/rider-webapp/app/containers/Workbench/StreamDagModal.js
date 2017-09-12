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

import Form from 'antd/lib/form'

export class StreamDagModal extends React.Component {
  render () {
    return (
      <div>
        <div className="ant-modal-header">
          <div className="ant-modal-title" id="rcDialogTitle0">Stream DAG</div>
        </div>
      </div>
    )
  }
}

StreamDagModal.propTypes = {
  form: React.PropTypes.any
}

export default Form.create({wrappedComponentRef: true})(StreamDagModal)
