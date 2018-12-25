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
  LOAD_USER_STREAMS,
  LOAD_USER_STREAMS_SUCCESS,
  LOAD_ADMIN_ALL_STREAMS,
  LOAD_ADMIN_ALL_STREAMS_SUCCESS,
  LOAD_ADMIN_SINGLE_STREAM,
  LOAD_ADMIN_SINGLE_STREAM_SUCCESS,
  LOAD_STREAM_DETAIL,
  LOAD_STREAM_DETAIL_SUCCESS,
  LOAD_STREAM_NAME_VALUE,
  LOAD_STREAM_NAME_VALUE_SUCCESS,
  LOAD_STREAM_NAME_VALUE_ERROR,
  LOAD_KAFKA,
  LOAD_KAFKA_SUCCESS,
  LOAD_STREAM_CONFIG_JVM,
  LOAD_STREAM_CONFIG_JVM_SUCCESS,
  LOAD_STREAM_CONFIG_SPARK,
  LOAD_STREAM_CONFIG_SPARK_SUCCESS,
  LOAD_LOGS_INFO,
  LOAD_LOGS_INFO_SUCCESS,
  LOAD_ADMIN_LOGS_INFO,
  LOAD_ADMIN_LOGS_INFO_SUCCESS,
  ADD_STREAMS,
  ADD_STREAMS_SUCCESS,
  EDIT_STREAM,
  EDIT_STREAM_SUCCESS,
  OPERATE_STREAMS,
  OPERATE_STREAMS_SUCCESS,
  DELETE_STREAMS,
  DELETE_STREAMS_SUCCESS,
  STARTORRENEW_STREAMS,
  STARTORRENEW_STREAMS_SUCCESS,
  OPERATE_STREAMS_ERROR,
  LOAD_LASTEST_OFFSET,
  LOAD_LASTEST_OFFSET_SUCCESS,
  JUMP_STREAM_TO_FLOW_FILTER
} from './constants'
import { fromJS } from 'immutable'

const initialState = fromJS({
  streams: false,
  streamSubmitLoading: false,
  streamNameExited: false,
  streamStartModalLoading: false,
  streamFilterId: ''
})

function streamReducer (state = initialState, { type, payload }) {
  const streams = state.get('streams')
  switch (type) {
    case LOAD_USER_STREAMS:
      return state
    case LOAD_USER_STREAMS_SUCCESS:
      return state.set('streams', payload.streams)
    case LOAD_ADMIN_ALL_STREAMS:
      return state.set('error', false)
    case LOAD_ADMIN_ALL_STREAMS_SUCCESS:
      return state.set('streams', payload.streams)
    case LOAD_ADMIN_SINGLE_STREAM:
      return state.set('error', false)
    case LOAD_ADMIN_SINGLE_STREAM_SUCCESS:
      return state.set('streams', payload.stream)
    case LOAD_STREAM_DETAIL:
      return state
    case LOAD_STREAM_DETAIL_SUCCESS:
      return state
    case LOAD_STREAM_NAME_VALUE:
      return state.set('streamNameExited', false)
    case LOAD_STREAM_NAME_VALUE_SUCCESS:
      return state.set('streamNameExited', false)
    case LOAD_STREAM_NAME_VALUE_ERROR:
      return state.set('streamNameExited', true)
    case LOAD_KAFKA:
      return state
    case LOAD_KAFKA_SUCCESS:
      return state
    case LOAD_STREAM_CONFIG_JVM:
      return state
    case LOAD_STREAM_CONFIG_JVM_SUCCESS:
      return state
    case LOAD_STREAM_CONFIG_SPARK:
      return state
    case LOAD_STREAM_CONFIG_SPARK_SUCCESS:
      return state
    case LOAD_LOGS_INFO:
      return state
    case LOAD_LOGS_INFO_SUCCESS:
      return state
    case LOAD_ADMIN_LOGS_INFO:
      return state
    case LOAD_ADMIN_LOGS_INFO_SUCCESS:
      return state
    case ADD_STREAMS:
      return state.set('streamSubmitLoading', true)
    case ADD_STREAMS_SUCCESS:
      streams.unshift(payload.result)
      return state
        .set('streams', streams.slice())
        .set('streamSubmitLoading', false)
    case EDIT_STREAM:
      return state
        .set('error', false)
        .set('streamSubmitLoading', true)
    case EDIT_STREAM_SUCCESS:
      const startIndexEdit = streams.indexOf(streams.find(p => Object.is(p.stream.id, payload.result.stream.id)))
      streams.fill(payload.result, startIndexEdit, startIndexEdit + 1)
      return state
        .set('streams', streams.slice())
        .set('streamSubmitLoading', false)
    case OPERATE_STREAMS:
      return state
    case OPERATE_STREAMS_SUCCESS:
      const startIndexOperate = streams.indexOf(streams.find(p => Object.is(p.stream.id, payload.result.stream.id)))
      streams.fill(payload.result, startIndexOperate, startIndexOperate + 1)
      return state.set('streams', streams.slice())
    case DELETE_STREAMS:
      return state
    case DELETE_STREAMS_SUCCESS:
      return state.set('streams', streams.filter(g => !Object.is(g.stream.id, payload.result)))
    case STARTORRENEW_STREAMS:
      return state.set('streamStartModalLoading', true)
    case STARTORRENEW_STREAMS_SUCCESS:
      const startIndexStartOrRenew = streams.indexOf(streams.find(p => Object.is(p.stream.id, payload.result.id)))
      streams[startIndexStartOrRenew].disableActions = payload.result.disableActions
      streams[startIndexStartOrRenew].stream.status = payload.result.status
      streams[startIndexStartOrRenew].stream.startedTime = payload.result.startedTime
      streams[startIndexStartOrRenew].stream.stoppedTime = payload.result.stoppedTime
      streams[startIndexStartOrRenew].stream.sparkAppid = payload.result.appId
      // streams.fill(payload.result, startIndexStartOrRenew, startIndexStartOrRenew + 1)
      return state
        .set('streams', streams.slice())
        .set('streamStartModalLoading', false)
    case OPERATE_STREAMS_ERROR:
      return state.set('streamStartModalLoading', false)
    case LOAD_LASTEST_OFFSET:
      return state
    case LOAD_LASTEST_OFFSET_SUCCESS:
      return state
    case JUMP_STREAM_TO_FLOW_FILTER:
      return state.set('streamFilterId', payload.streamFilterId)
    default:
      return state
  }
}

export default streamReducer
