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

import Icon from 'antd/lib/icon'
import Form from 'antd/lib/form'
import Row from 'antd/lib/row'
import Col from 'antd/lib/col'
const FormItem = Form.Item

import { loadRiderInfos } from './action'
import { selectRiderInfos } from './selectors'

export class RiderInfo extends React.Component {
  componentWillMount () {
    this.props.onLoadRiderInfos()
  }

  render () {
    const { riderInfos } = this.props

    return (
      <div className={`ri-workbench-table ri-common-block`}>
        <Helmet title="Rider Info" />
        <h3 className="ri-common-block-title">
          <Icon type="bars" /> Rider Info <FormattedMessage {...messages.riderInfoTableList} />
        </h3>
        <Form className="rider-info-class">
          <Row>
            <Col span={2}></Col>
            <Col span={16}>
              <FormItem label="">
                <pre>"zookeeper":"{riderInfos.zookeeper}"</pre>
                <pre>"kafka":"{riderInfos.kafka}"</pre>
                <pre>"feedback_topic":"{riderInfos.feedback_topic}"</pre>
                <pre>"heartbeat_topic":"{riderInfos.heartbeat_topic}"</pre>
                <pre>"hdfslog_root_path":"{riderInfos.hdfslog_root_path}"</pre>
                <pre>"spark_submit_user":"{riderInfos.spark_submit_user}"</pre>
                <pre>"spark_app_tags":"{riderInfos.spark_app_tags}"</pre>
                <pre>"yarn_rm1_http_url":"{riderInfos.yarn_rm1_http_url}"</pre>
                <pre>"yarn_rm2_http_url":"{riderInfos.yarn_rm2_http_url}"</pre>
              </FormItem>
            </Col>
            <Col span={6}></Col>
          </Row>
        </Form>

      </div>
    )
  }
}

RiderInfo.propTypes = {
  onLoadRiderInfos: PropTypes.func,
  riderInfos: PropTypes.oneOfType([
    PropTypes.object,
    PropTypes.bool
  ])
}

export function mapDispatchToProps (dispatch) {
  return {
    onLoadRiderInfos: () => dispatch(loadRiderInfos())
  }
}

const mapStateToProps = createStructuredSelector({
  riderInfos: selectRiderInfos()
})

export default connect(mapStateToProps, mapDispatchToProps)(RiderInfo)

