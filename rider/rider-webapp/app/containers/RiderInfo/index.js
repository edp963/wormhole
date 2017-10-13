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

    // let infoMonitor = {}
    // let infoConsumer = {}
    // let infoZk = {}
    // let infoDb = {}
    // let infoSpark = {}
    // if (typeof (riderInfos) === 'object') {
    //   infoMonitor = riderInfos.monitor
    //   infoConsumer = riderInfos.consumer
    //   infoZk = riderInfos.zk
    //   infoDb = riderInfos.db
    //   infoSpark = riderInfos.spark
    // }

    // const itemStyle = {
    //   labelCol: { span: 11 },
    //   wrapperCol: { span: 12 }
    // }

    return (
      <div className={`ri-workbench-table ri-common-block`}>
        <Helmet title="Rider Info" />
        <h3 className="ri-common-block-title">
          <Icon type="bars" /> Rider Info 列表
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
          {/* <Row gutter={8}>
            <Col span={12}>
              <FormItem label="Monitor" {...itemStyle}>
                <pre>"grafana_host":"{infoMonitor.grafana_host}"</pre>
                <pre>"grafana_admin_user":"{infoMonitor.grafana_admin_user}"</pre>
                <pre>"grafana_viewer_user":"{infoMonitor.grafana_viewer_user}"</pre>
                <pre>"elasticsearch_host":"{infoMonitor.elasticsearch_host}"</pre>
              </FormItem>
            </Col>
            <Col span={12}>
              <FormItem label="ZK" {...itemStyle}>
                <pre>"url":"{infoZk.url}"</pre>
                <pre>"directive_root_path":"{infoZk.directive_root_path}"</pre>
                <pre>"directive_flow_path":"{infoZk.directive_flow_path}"</pre>
                <pre>"directive_topic_path":"{infoZk.directive_topic_path}"</pre>
              </FormItem>
            </Col>
          </Row>
          <Row gutter={8}>
            <Col span={12}>
              <FormItem label="Consumer" {...itemStyle}>
                <pre>"brokers":"{infoConsumer.brokers}"</pre>
                <pre>"topic":"{infoConsumer.topic}"</pre>
                <pre>"client_id":"{infoConsumer.client_id}"</pre>
                <pre>"group_id":"{infoConsumer.group_id}"</pre>
              </FormItem>
            </Col>
            <Col span={12}>
              <FormItem label="DB" {...itemStyle}>
                <pre>"url":"{infoDb.url}"</pre>
                <pre>"user":"{infoDb.user}"</pre>
                <pre>"thread_number":"{infoDb.thread_number}"</pre>
              </FormItem>
            </Col>
          </Row>
          <Row gutter={8}>
            <Col span={12}>
              <FormItem label="Spark" {...itemStyle}>
                <pre>"wormhole_default_topic":"{infoSpark.wormhole_default_topic}"</pre>
                <pre>"hdfs_root":"{infoSpark.hdfs_root}"</pre>
                <pre>"log4j_file_path":"{infoSpark.log4j_file_path}"</pre>
                <pre>"host":"{infoSpark.host}"</pre>
                <pre>"rest_api":"{infoSpark.rest_api}"</pre>
                <pre>"wormhole_start_shell":"{infoSpark.wormhole_start_shell}"</pre>
                <pre>"wormhole_jar_path":"{infoSpark.wormhole_jar_path}"</pre>
                <pre>"mode":"{infoSpark.mode}"</pre>
                <pre>"spark_home":"{infoSpark.spark_home}"</pre>
                <pre>"user":"{infoSpark.user}"</pre>
                <pre>"log_path":"{infoSpark.log_path}"</pre>
              </FormItem>
            </Col>
          </Row> */}
        </Form>

      </div>
    )
  }
}

RiderInfo.propTypes = {
  onLoadRiderInfos: React.PropTypes.func,
  riderInfos: React.PropTypes.oneOfType([
    React.PropTypes.object,
    React.PropTypes.bool
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

