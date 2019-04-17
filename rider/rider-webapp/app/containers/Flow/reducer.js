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
  LOAD_FLOW_DETAIL,
  LOAD_FLOW_DETAIL_SUCCESS,
  LOAD_LOOKUP_SQL,
  LOAD_LOOKUP_SQL_SUCCESS,
  LOAD_LOOKUP_SQL_ERROR,

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
  STARTFLINK_FLOWS,
  STARTFLINK_FLOWS_SUCCESS,
  OPERATE_FLOWS_ERROR,
  LOAD_RECHARGE_HISTORY,
  LOAD_RECHARGE_HISTORY_SUCCESS,
  COMFIRM_RECHARGE,
  COMFIRM_RECHARGE_SUCCESS
} from './constants'
import { fromJS } from 'immutable'

const initialState = fromJS({
  flows: false,
  error: false,
  flowSubmitLoading: false,
  sourceToSinkExited: false,
  flowStartModalLoading: false,
  rechargeHistoryLoading: false,
  confirmRechargeLoading: false
})

function flowReducer (state = initialState, { type, payload }) {
  const flows = state.get('flows')
  switch (type) {
    case LOAD_ADMIN_ALL_FLOWS:
      return state.set('error', false)
    case LOAD_ADMIN_ALL_FLOWS_SUCCESS:
      return state.set('flows', payload.flows)
    case LOAD_USER_ALL_FLOWS:
      return state.set('error', false)
    case LOAD_USER_ALL_FLOWS_SUCCESS:
      return state.set('flows', payload.flows)
    case LOAD_ADMIN_SINGLE_FLOW:
      return state.set('error', false)
    case LOAD_ADMIN_SINGLE_FLOW_SUCCESS:
      return state.set('flows', payload.flow)
    case LOAD_SELECT_STREAM_KAFKA_TOPIC:
      return state.set('error', false)
    case LOAD_SELECT_STREAM_KAFKA_TOPIC_SUCCESS:
      return state
    case LOAD_SOURCESINKTYPE_NAMESPACE:
      return state.set('error', false)
    case LOAD_SOURCESINKTYPE_NAMESPACE_SUCCESS:
      return state
    case LOAD_SINKTYPE_NAMESPACE:
      return state.set('error', false)
    case LOAD_SINKTYPE_NAMESPACE_SUCCESS:
      return state
    case LOAD_TRANSINKTYPE_NAMESPACE:
      return state.set('error', false)
    case LOAD_TRANSINKTYPE_NAMESPACE_SUCCESS:
      return state
    case LOAD_SOURCETOSINK_EXIST:
      return state.set('sourceToSinkExited', false)
    case LOAD_SOURCETOSINK_EXIST_SUCCESS:
      return state.set('sourceToSinkExited', false)
    case LOAD_SOURCETOSINK_EXIST_ERROR:
      return state.set('sourceToSinkExited', true)
    case LOAD_LOOKUP_SQL:
      return state
    case LOAD_LOOKUP_SQL_SUCCESS:
      return state
    case LOAD_LOOKUP_SQL_ERROR:
      return state
    case ADD_FLOWS:
      return state.set('flowSubmitLoading', true)
    case ADD_FLOWS_SUCCESS:
      for (let i = 0; i < payload.result.length; i++) {
        flows.unshift(payload.result[i])
      }
      return state
        .set('flows', flows.slice())
        .set('flowSubmitLoading', false)
    case OPERATE_USER_FLOW:
      return state.set('error', false)
    case OPERATE_USER_FLOW_SUCCESS:
      if (typeof payload.result === 'string') {
        // 删除操作
        return state.set('flows', flows.filter(g => payload.result.split(',').indexOf(`${g.id}`) < 0))
      } else {
        if (payload.result.length) {
          // 批量操作
          for (let j = 0; j < flows.length; j++) {
            for (let i = 0; i < payload.result.length; i++) {
              flows[j] = flows[j].id === payload.result[i].id ? payload.result[i] : flows[j]
            }
          }
          return state.set('flows', flows.slice())
        } else {
          // 单行操作
          const flowsFinal = flows.map(t => t.id === payload.result.id ? payload.result : t)
          return state.set('flows', flowsFinal.slice())
        }
      }
    case OPERATE_FLOW_ERROR:
      return state.set('confirmRechargeLoading', false)
    case LOAD_FLOW_DETAIL:
      return state
    case LOAD_FLOW_DETAIL_SUCCESS:
      return state
    case CHUCKAWAY_FLOW:
      return state.set('flows', false)

    case QUERY_FLOW:
      return state.set('error', false)
    case QUERY_FLOW_SUCCESS:
      return state
    case LOAD_FLOWS_ERROR:
      return state.set('error', payload.error).set('rechargeHistoryLoading', false)
    case LOAD_SOURCELOG_DETAIL:
      return state.set('error', false)
    case LOAD_SOURCELOG_DETAIL_SUCCESS:
      return state
    case LOAD_SOURCELOG_DETAIL_ERROR:
      return state.set('error', payload.error)
    case LOAD_SOURCESINK_DETAIL:
      return state.set('error', false)
    case LOAD_SOURCESINK_DETAIL_SUCCESS:
      return state
    case LOAD_SOURCESINK_DETAIL_ERROR:
      return state.set('error', payload.error)
    case LOAD_SINKWRITERROR_DETAIL:
      return state.set('error', false)
    case LOAD_SINKWRITERROR_DETAIL_SUCCESS:
      return state
    case LOAD_SINKWRITERROR_DETAIL_ERROR:
      return state.set('error', payload.error)
    case LOAD_SOURCEINPUT:
      return state.set('error', false)
    case LOAD_SOURCEINPUT_SUCCESS:
      return state
    case LOAD_SOURCEINPUT_ERROR:
      return state.set('error', payload.error)
    case EDIT_LOGFORM:
      return state.set('error', false)
    case EDIT_LOGFORM_SUCCESS:
      const startIndexLogForm = flows.indexOf(flows.find(g => Object.is(g.id, payload.result.id)))
      flows.fill(payload.result, startIndexLogForm, startIndexLogForm + 1)
      return state.set('flows', flows.slice())
    case EDIT_LOGFORM_ERROR:
      return state.set('error', payload.error)
    case SAVE_FORM:
      return state.set('error', false)
    case SAVE_FORM_SUCCESS:
      const startIndexSave = flows.indexOf(flows.find(g => Object.is(g.id, payload.result.payload.flowId)))
      flows.fill(payload.result.payload, startIndexSave, startIndexSave + 1)
      return state.set('flows', flows.slice())
    case SAVE_FORM_ERROR:
      return state.set('error', payload.error)
    case CHECKOUT_FORM:
      return state.set('error', false)
    case CHECKOUT_FORM_SUCCESS:
      const startIndexCheckout = flows.indexOf(flows.find(g => Object.is(g.id, payload.result.payload.flowId)))
      flows.fill(payload.result.payload, startIndexCheckout, startIndexCheckout + 1)
      return state.set('flows', flows.slice())
    case CHECKOUT_FORM_ERROR:
      return state.set('error', payload.error)
    case EDIT_FLOWS:
      return state.set('flowSubmitLoading', true)
    case EDIT_FLOWS_SUCCESS:
      const startIndexEdit = flows.indexOf(flows.find(p => Object.is(p.id, payload.result.id)))
      flows.fill(payload.result, startIndexEdit, startIndexEdit + 1)
      return state
        .set('flows', flows.slice())
        .set('flowSubmitLoading', false)
    case STARTFLINK_FLOWS:
      return state.set('flowStartModalLoading', true)
    case STARTFLINK_FLOWS_SUCCESS:
      const startIndexStartOrRenew = flows.indexOf(flows.find(p => Object.is(p.id, payload.result.id)))
      flows[startIndexStartOrRenew].disableActions = payload.result.disableActions
      flows[startIndexStartOrRenew].startedTime = payload.result.startedTime
      flows[startIndexStartOrRenew].stoppedTime = payload.result.stoppedTime
      flows[startIndexStartOrRenew].status = payload.result.status
      // streams.fill(payload.result, startIndexStartOrRenew, startIndexStartOrRenew + 1)
      return state
        .set('flows', flows.slice())
        .set('flowStartModalLoading', false)
    case OPERATE_FLOWS_ERROR:
      return state.set('flowStartModalLoading', false).set('confirmRechargeLoading', false)
    case LOAD_RECHARGE_HISTORY:
      return state.set('rechargeHistoryLoading', true)
    case LOAD_RECHARGE_HISTORY_SUCCESS:
      return state.set('rechargeHistoryList', payload.list).set('rechargeHistoryLoading', false)
    case COMFIRM_RECHARGE:
      return state.set('confirmRechargeLoading', true)
    case COMFIRM_RECHARGE_SUCCESS:
      return state.set('confirmRechargeLoading', false)
    default:
      return state
  }
}

export default flowReducer
