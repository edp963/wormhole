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

import Source2LogTable from './Source2LogTable'
import Source2LogForm from './Source2LogForm'
import Source2SinkTable from './Source2SinkTable'
import Source2SinkForm from './Source2SinkForm'
import SinkWriteRrror from './SinkWriteRrror'
import Tabs from 'antd/lib/tabs'
import Icon from 'antd/lib/icon'
const TabPane = Tabs.TabPane

export class FlowsDetail extends React.Component {

  constructor (props) {
    super(props)
    this.state = {
      sourceLogData: [],
      sourceLogPageIndex: 1,
      sourceLogPageSize: 10,
      sourceLogTotal: 0,

      sourceSinkData: [],
      sourceSinkPageIndex: 1,
      sourceSinkPageSize: 10,
      sourceSinkTotal: 0,

      sinkWriteRrrorData: [],
      sinkWriteRrrorPageIndex: 1,
      sinkWriteRrrorPageSize: 10,
      sinkWriteRrrorTotal: 0,

      sinkTempValue: null
    }
  }

  /**
   * Modal数据显示
   */
  onLoadData = (id) => {
    this.Source2LogTablePagination(this.state.sourceLogPageIndex, this.state.sourceLogPageSize)
    this.Source2SinkTablePagination(this.state.sourceSinkPageIndex, this.state.sourceSinkPageSize)
    this.SinkWriteRrrorPagination(this.state.sinkWriteRrrorPageIndex, this.state.sinkWriteRrrorPageSize)

    this.props.onLoadSourceInput(id, 'source2sink', (result) => {
      // 当result.result不存在时，将flowId转换成对象
      let formValue = result.result
        ? result.result.find(s => s.taskType === 'source2sink')
        : { flowId: id }

      // source2SinkForm是否存在（ant design Tabs）
      if (this.source2SinkForm) {
        this.source2SinkForm.setFieldsValue(formValue)
      } else {
        this.setState({
          sinkTempValue: formValue
        })
      }
    })

    this.props.onLoadSourceInput(id, 'source2hdfs', (result) => {
      if (result.result) {
        const source2hdfsData = result.result.find(s => s.taskType === 'source2hdfs')
        this.source2LogForm.setFieldsValue(source2hdfsData)
      } else {
        // 当result.result不存在时，将flowId转换成对象
        const flowId = `${id}`
        const idObject = {flowId}
        this.source2LogForm.setFieldsValue(idObject)
      }
    })
  }

  /**
   * 点击Source2Sink保存
   */
  onSaveSinkOk = () => {
    this.source2SinkForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        // this.props.onSaveForm(this.props.flowIdGeted, 'source2sink', values, () => {
        //   message.success('保存成功！', 5)
        // })
      }
    })
  }

  /**
   * 点击Source2Log保存
   */
  onSaveLogOk = () => {
    this.source2LogForm.validateFieldsAndScroll((err, values) => {
      if (!err) {
        // this.props.onSaveForm(this.props.flowIdGeted, 'source2hdfs', values, () => {
        //   message.success('保存成功！', 5)
        // })
      }
    })
  }

  /**
   * 清除数据
   */
  onCancelCleanData = () => {
    this.source2LogForm.resetFields()
    this.source2SinkForm && this.source2SinkForm.resetFields()
  }

  /**
   * Source2LogTable组件触发分页回调函数
   * @param pageIndex
   * @param pageSize
   */
  Source2LogTablePagination = (pageIndex, pageSize) => {
    this.props.onLoadSourceLogDetail(this.props.flowIdGeted, pageIndex, pageSize, (sourceLogTotal, sourceLogData) => {
      this.setState({
        sourceLogTotal: sourceLogTotal,
        sourceLogData: sourceLogData
      })
    })
  }

  onSourceLogTableChange = (pageIndex, pageSize) => {
    this.setState({
      sourceLogPageIndex: pageIndex,
      sourceLogPageSize: pageSize
    })
    this.Source2LogTablePagination(pageIndex, pageSize)
  }

  /**
   * Source2SinkTable组件触发分页回调函数
   * @param pageIndex
   * @param pageSize
   */
  Source2SinkTablePagination = (pageIndex, pageSize) => {
    this.props.onLoadSourceSinkDetail(this.props.flowIdGeted, pageIndex, pageSize, (sourceSinkTotal, sourceSinkData) => {
      this.setState({
        sourceSinkTotal: sourceSinkTotal,
        sourceSinkData: sourceSinkData
      })
    })
  }

  onSourceSinkTableChange = (pageIndex, pageSize) => {
    this.setState({
      sourceSinkPageIndex: pageIndex,
      sourceSinkPageSize: pageSize
    })
    this.Source2SinkTablePagination(pageIndex, pageSize)
  }

  /**
   * SinkWriteRrror组件触发分页回调函数
   * @param pageIndex
   * @param pageSize
   */
  SinkWriteRrrorPagination = (pageIndex, pageSize) => {
    this.props.onLoadSinkWriteRrrorDetail(this.props.flowIdGeted, pageIndex, pageSize, (sinkWriteRrrorTotal, sinkWriteRrrorData) => {
      this.setState({
        sinkWriteRrrorTotal: sinkWriteRrrorTotal,
        sinkWriteRrrorData: sinkWriteRrrorData
      })
    })
  }

  onSinkWriteRrrorTableChange = (pageIndex, pageSize) => {
    this.setState({
      sinkWriteRrrorPageIndex: pageIndex,
      sinkWriteRrrorPageSize: pageSize
    })
    this.SinkWriteRrrorPagination(pageIndex, pageSize)
  }

  render () {
    const { sourceLogPageIndex, sourceLogPageSize, sourceLogTotal, sourceLogData, sourceSinkData, sourceSinkPageIndex, sourceSinkPageSize, sourceSinkTotal, sinkWriteRrrorData, sinkWriteRrrorPageIndex, sinkWriteRrrorPageSize, sinkWriteRrrorTotal } = this.state
    return (
      <div className="flow-title">
        <Tabs defaultActiveKey="Source2Log">
          <TabPane tab={<h4><Icon type="setting" />Source2Log</h4>} key="Source2Log" className="log-table-style">
            <Source2LogForm
              onSaveLogClick={this.onSaveLogOk}
              type={this.state.formType}
              ref={(f) => { this.source2LogForm = f }}
              onCheckOutForm={this.props.onCheckOutForm}
              initialFlowId={this.props.flowIdGeted} />
            <Source2LogTable
              data={sourceLogData}
              pageIndex={sourceLogPageIndex}
              pageSize={sourceLogPageSize}
              total={sourceLogTotal}
              onChange={this.onSourceLogTableChange}
            />
          </TabPane>
          <TabPane tab={<h4><Icon type="setting" />Source2Sink</h4>} key="Source2Sink" className="sink-table-style">
            <Source2SinkForm
              onSaveSinkClick={this.onSaveSinkOk}
              type={this.state.formType}
              initialValue={this.state.sinkTempValue}
              initialFlowId={this.props.flowIdGeted}
              ref={(f) => { this.source2SinkForm = f }}
              onCheckOutForm={this.props.onCheckOutForm} />
            <Source2SinkTable
              data={sourceSinkData}
              pageIndex={sourceSinkPageIndex}
              pageSize={sourceSinkPageSize}
              total={sourceSinkTotal}
              onChange={this.onSourceSinkTableChange}
            />
          </TabPane>
          <TabPane tab={<h4><Icon type="file-excel" />SinkWriteError</h4>} key="SinkWriteRrror" className="error-table-style">
            <SinkWriteRrror
              data={sinkWriteRrrorData}
              pageIndex={sinkWriteRrrorPageIndex}
              pageSize={sinkWriteRrrorPageSize}
              total={sinkWriteRrrorTotal}
              onChange={this.onSinkWriteRrrorTableChange}
            />
          </TabPane>
        </Tabs>
      </div>
    )
  }
}

FlowsDetail.propTypes = {
  // onSaveForm: React.PropTypes.func,
  onLoadSourceLogDetail: React.PropTypes.func,
  onLoadSourceSinkDetail: React.PropTypes.func,
  onLoadSinkWriteRrrorDetail: React.PropTypes.func,
  onLoadSourceInput: React.PropTypes.func,
  flowIdGeted: React.PropTypes.number,
  onCheckOutForm: React.PropTypes.func
}

export default FlowsDetail
