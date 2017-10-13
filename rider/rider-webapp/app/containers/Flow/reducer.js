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

import {
  LOAD_ADMIN_ALL_FLOWS,
  LOAD_ADMIN_ALL_FLOWS_SUCCESS,
  LOAD_USER_ALL_FLOWS,
  LOAD_USER_ALL_FLOWS_SUCCESS,
  LOAD_ADMIN_SINGLE_FLOW,
  LOAD_ADMIN_SINGLE_FLOW_SUCCESS,
  LOAD_SELECT_STREAM_KAFKA_TOPIC,
  LOAD_SELECT_STREAM_KAFKA_TOPIC_SUCCESS,
  LOAD_SOURCESINKTYPE_NAMESPACE,
  LOAD_SOURCESINKTYPE_NAMESPACE_SUCCESS,
  LOAD_SINKTYPE_NAMESPACE,
  LOAD_SINKTYPE_NAMESPACE_SUCCESS,
  LOAD_TRANSINKTYPE_NAMESPACE,
  LOAD_TRANSINKTYPE_NAMESPACE_SUCCESS,
  LOAD_SOURCETOSINK_EXIST,
  LOAD_SOURCETOSINK_EXIST_SUCCESS,
  LOAD_SOURCETOSINK_EXIST_ERROR,
  ADD_FLOWS,
  ADD_FLOWS_SUCCESS,
  OPERATE_USER_FLOW,
  OPERATE_USER_FLOW_SUCCESS,
  QUERY_FLOW,
  QUERY_FLOW_SUCCESS,
  OPERATE_FLOW_ERROR,
  CHUCKAWAY_FLOW,

  LOAD_FLOWS_ERROR,
  LOAD_SOURCELOG_DETAIL,
  LOAD_SOURCELOG_DETAIL_SUCCESS,
  LOAD_SOURCELOG_DETAIL_ERROR,
  LOAD_SOURCESINK_DETAIL,
  LOAD_SOURCESINK_DETAIL_SUCCESS,
  LOAD_SOURCESINK_DETAIL_ERROR,
  LOAD_SINKWRITERROR_DETAIL,
  LOAD_SINKWRITERROR_DETAIL_SUCCESS,
  LOAD_SINKWRITERROR_DETAIL_ERROR,
  EDIT_LOGFORM,
  EDIT_LOGFORM_SUCCESS,
  EDIT_LOGFORM_ERROR,
  SAVE_FORM,
  SAVE_FORM_SUCCESS,
  SAVE_FORM_ERROR,
  CHECKOUT_FORM,
  CHECKOUT_FORM_SUCCESS,
  CHECKOUT_FORM_ERROR,
  EDIT_FLOWS,
  EDIT_FLOWS_SUCCESS,
  LOAD_SOURCEINPUT,
  LOAD_SOURCEINPUT_SUCCESS,
  LOAD_SOURCEINPUT_ERROR,
  OPERATE_FLOWS,
  OPERATE_FLOWS_SUCCESS
} from './constants'
import { fromJS } from 'immutable'

const initialState = fromJS({
  flows: false,
  error: false,
  flowSubmitLoading: false,
  sourceToSinkExited: false
})

function flowReducer (state = initialState, { type, payload }) {
  const flows = state.get('flows')
  switch (type) {
    case LOAD_ADMIN_ALL_FLOWS:
      return state.set('error', false)
    case LOAD_ADMIN_ALL_FLOWS_SUCCESS:
      payload.resolve()
      return state.set('flows', payload.flows)
    case LOAD_USER_ALL_FLOWS:
      return state.set('error', false)
    case LOAD_USER_ALL_FLOWS_SUCCESS:
      payload.resolve()
      return state.set('flows', payload.flows)
    case LOAD_ADMIN_SINGLE_FLOW:
      return state.set('error', false)
    case LOAD_ADMIN_SINGLE_FLOW_SUCCESS:
      payload.resolve()
      return state.set('flows', payload.flow)
    case LOAD_SELECT_STREAM_KAFKA_TOPIC:
      return state.set('error', false)
    case LOAD_SELECT_STREAM_KAFKA_TOPIC_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_SOURCESINKTYPE_NAMESPACE:
      return state.set('error', false)
    case LOAD_SOURCESINKTYPE_NAMESPACE_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_SINKTYPE_NAMESPACE:
      return state.set('error', false)
    case LOAD_SINKTYPE_NAMESPACE_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_TRANSINKTYPE_NAMESPACE:
      return state.set('error', false)
    case LOAD_TRANSINKTYPE_NAMESPACE_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_SOURCETOSINK_EXIST:
      return state.set('sourceToSinkExited', false)
    case LOAD_SOURCETOSINK_EXIST_SUCCESS:
      payload.resolve()
      return state.set('sourceToSinkExited', false)
    case LOAD_SOURCETOSINK_EXIST_ERROR:
      payload.reject()
      return state.set('sourceToSinkExited', true)
    case ADD_FLOWS:
      return state.set('flowSubmitLoading', true)
    case ADD_FLOWS_SUCCESS:
      payload.resolve(payload.result)

      for (let i = 0; i < payload.result.length; i++) {
        flows.unshift(payload.result[i])
      }
      payload.final()
      return state
        .set('flows', flows.slice())
        .set('flowSubmitLoading', false)
    case OPERATE_USER_FLOW:
      return state.set('error', false)
    case OPERATE_USER_FLOW_SUCCESS:
      if (typeof (payload.result) === 'string') {
        payload.resolve(payload.result)
        return state.set('flows', flows.filter(g => payload.result.split(',').indexOf(`${g.id}`) < 0))
      } else {
        if (payload.result.length === undefined) {
          flows.splice(flows.indexOf(flows.find(g => g.id === payload.result.id)), 1, payload.result)
        } else {
          for (let i = 0; i < payload.result.length; i++) {
            flows.splice(flows.indexOf(flows.find(g => g.id === payload.result[i].id)), 1, payload.result[i])
          }
        }
        payload.resolve(payload.result)
        return state.set('flows', flows.slice())
      }
    case OPERATE_FLOW_ERROR:
      payload.reject(payload.message)
      return state
    case CHUCKAWAY_FLOW:
      return state.set('flows', false)

    case QUERY_FLOW:
      return state.set('error', false)
    case QUERY_FLOW_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_FLOWS_ERROR:
      return state.set('error', payload.error)
    case LOAD_SOURCELOG_DETAIL:
      return state.set('error', false)
    case LOAD_SOURCELOG_DETAIL_SUCCESS:
      payload.resolve(payload.total, payload.sourceLog)
      return state
    case LOAD_SOURCELOG_DETAIL_ERROR:
      return state.set('error', payload.error)
    case LOAD_SOURCESINK_DETAIL:
      return state.set('error', false)
    case LOAD_SOURCESINK_DETAIL_SUCCESS:
      payload.resolve(payload.total, payload.sourceSink)
      return state
    case LOAD_SOURCESINK_DETAIL_ERROR:
      return state.set('error', payload.error)
    case LOAD_SINKWRITERROR_DETAIL:
      return state.set('error', false)
    case LOAD_SINKWRITERROR_DETAIL_SUCCESS:
      payload.resolve(payload.total, payload.sinkWriteRrror)
      return state
    case LOAD_SINKWRITERROR_DETAIL_ERROR:
      return state.set('error', payload.error)
    case LOAD_SOURCEINPUT:
      return state.set('error', false)
    case LOAD_SOURCEINPUT_SUCCESS:
      payload.resolve(payload)
      return state
    case LOAD_SOURCEINPUT_ERROR:
      return state.set('error', payload.error)
    case EDIT_LOGFORM:
      return state.set('error', false)
    case EDIT_LOGFORM_SUCCESS:
      payload.resolve()
      flows.splice(flows.indexOf(flows.find(g => g.id === payload.result.id)), 1, payload.result)
      return state.set('flows', flows.slice())
    case EDIT_LOGFORM_ERROR:
      return state.set('error', payload.error)
    case SAVE_FORM:
      return state.set('error', false)
    case SAVE_FORM_SUCCESS:
      payload.resolve()
      flows.splice(flows.indexOf(flows.find(g => g.id === payload.result.payload.flowId)), 1, payload.result.payload)
      return state.set('flows', flows.slice())
    case SAVE_FORM_ERROR:
      return state.set('error', payload.error)
    case CHECKOUT_FORM:
      return state.set('error', false)
    case CHECKOUT_FORM_SUCCESS:
      payload.resolve()
      flows.splice(flows.indexOf(flows.find(g => g.id === payload.result.payload.flowId)), 1, payload.result.payload)
      return state.set('flows', flows.slice())
    case CHECKOUT_FORM_ERROR:
      return state.set('error', payload.error)
    case EDIT_FLOWS:
      return state.set('flowSubmitLoading', true)
    case EDIT_FLOWS_SUCCESS:
      payload.resolve()
      flows.splice(flows.indexOf(flows.find(p => p.id === payload.result.id)), 1, payload.result)
      // if (payload.result.header.code < 400) {
      //   payload.resolve()
      //   flows.splice(flows.indexOf(flows.find(p => p.id === payload.result.payload.id)), 1, payload.result.payload)
      // } else {
      //   payload.reject(payload.result)
      // }
      payload.final()
      return state
        .set('flows', flows.slice())
        .set('flowSubmitLoading', false)
    case OPERATE_FLOWS:
      return state.set('error', false)
    case OPERATE_FLOWS_SUCCESS:
      payload.resolve()
      let selectedOperateFlows = flows.filter(g => payload.flowIds.split(',').indexOf(`${g.id}`) >= 0)

      if (payload.operate !== 'backfill') {
        if (payload.operate === 'start') {
          selectedOperateFlows.forEach((element) => {
            if (element.status === 'failed' || element.status === 'stopped' || element.status === 'new') {
              element.status = 'starting'
            } else {
              console.log('start-todo')
            }
          })
        } else if (payload.operate === 'stop') {
          selectedOperateFlows.forEach((element) => {
            if (element.status === 'starting' || element.status === 'running') {
              element.status = 'stopping'
            } else {
              console.log('stop-todo')
            }
          })
        } else if (payload.operate === 'delete') {
          selectedOperateFlows.forEach((element) => {
            if (element.status === 'failed' || element.status === 'new' || element.status === 'stopped') {
              element.status = 'deleting'
            } else {
              console.log('delete-todo')
            }
          })
        }
      }

      return state.set('flows', flows.slice())
    default:
      return state
  }
}

export default flowReducer
