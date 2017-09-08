/*-
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
  LOAD_OFFSET,
  LOAD_OFFSET_SUCCESS,
  CHUCKAWAY_TOPIC,
  LOAD_STREAM_NAME_VALUE,
  LOAD_STREAM_NAME_VALUE_SUCCESS,
  LOAD_KAFKA,
  LOAD_KAFKA_SUCCESS,
  LOAD_STREAM_CONFIG_JVM,
  LOAD_STREAM_CONFIG_JVM_SUCCESS,
  LOAD_TOPICS,
  LOAD_TOPICS_SUCCESS,
  EDIT_TOPICS,
  EDIT_TOPICS_SUCCESS,
  LOAD_LOGS_INFO,
  LOAD_LOGS_INFO_SUCCESS,
  LOAD_ADMIN_LOGS_INFO,
  LOAD_ADMIN_LOGS_INFO_SUCCESS,
  ADD_STREAMS,
  ADD_STREAMS_SUCCESS,
  LOAD_SINGLE_STREAM,
  LOAD_SINGLE_STREAM_SUCCESS,
  EDIT_STREAM,
  EDIT_STREAM_SUCCESS,
  OPERATE_STREAMS,
  OPERATE_STREAMS_SUCCESS,
  STARTORRENEW_STREAMS,
  STARTORRENEW_STREAMS_SUCCESS,
  OPERATE_STREAMS_ERROR
} from './constants'
import { fromJS } from 'immutable'

const initialState = fromJS({
  streams: false,
  streamSubmitLoading: false
})

function streamReducer (state = initialState, { type, payload }) {
  const streams = state.get('streams')
  switch (type) {
    case LOAD_USER_STREAMS:
      return state
    case LOAD_USER_STREAMS_SUCCESS:
      payload.resolve()
      return state.set('streams', payload.streams)
    case LOAD_ADMIN_ALL_STREAMS:
      return state.set('error', false)
    case LOAD_ADMIN_ALL_STREAMS_SUCCESS:
      payload.resolve()
      return state.set('streams', payload.streams)
    case LOAD_ADMIN_SINGLE_STREAM:
      return state.set('error', false)
    case LOAD_ADMIN_SINGLE_STREAM_SUCCESS:
      payload.resolve()
      return state.set('streams', payload.stream)
    case LOAD_OFFSET:
      return state
    case LOAD_OFFSET_SUCCESS:
      payload.resolve(payload.result)
      return state
    case CHUCKAWAY_TOPIC:
      // payload.resolve(payload.result)
      return state
    case LOAD_STREAM_NAME_VALUE:
      return state
    case LOAD_STREAM_NAME_VALUE_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_KAFKA:
      return state
    case LOAD_KAFKA_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_STREAM_CONFIG_JVM:
      return state
    case LOAD_STREAM_CONFIG_JVM_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_TOPICS:
      return state
    case LOAD_TOPICS_SUCCESS:
      payload.resolve(payload.result)
      return state
    case EDIT_TOPICS:
      return state.set('error', false)
    case EDIT_TOPICS_SUCCESS:
      payload.resolve(payload.result)
      // streams.splice(streams.indexOf(streams.find(p => p.streamTopic.id === payload.result.streamTopic.id)), 1, payload.result)
      return state.set('streams', streams.slice())
    case LOAD_LOGS_INFO:
      return state
    case LOAD_LOGS_INFO_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_ADMIN_LOGS_INFO:
      return state
    case LOAD_ADMIN_LOGS_INFO_SUCCESS:
      payload.resolve(payload.result)
      return state
    case ADD_STREAMS:
      return state.set('streamSubmitLoading', true)
    case ADD_STREAMS_SUCCESS:
      payload.resolve()
      streams.unshift(payload.result[0])
      return state
        .set('streams', streams.slice())
        .set('streamSubmitLoading', false)
    case LOAD_SINGLE_STREAM:
      return state
    case LOAD_SINGLE_STREAM_SUCCESS:
      payload.resolve(payload.result)
      return state
    case EDIT_STREAM:
      return state
        .set('error', false)
        .set('streamSubmitLoading', true)
    case EDIT_STREAM_SUCCESS:
      payload.resolve()
      streams.splice(streams.indexOf(streams.find(p => p.stream.id === payload.result[0].stream.id)), 1, payload.result[0])
      return state
        .set('streams', streams.slice())
        .set('streamSubmitLoading', false)
    case OPERATE_STREAMS:
      return state
    case OPERATE_STREAMS_SUCCESS:
      payload.resolve()
      streams.splice(streams.indexOf(streams.find(p => p.stream.id === payload.result.stream.id)), 1, payload.result)
      return state.set('streams', streams.slice())
    case STARTORRENEW_STREAMS:
      return state
    case STARTORRENEW_STREAMS_SUCCESS:
      payload.resolve()
      streams.splice(streams.indexOf(streams.find(p => p.stream.id === payload.result.stream.id)), 1, payload.result)
      return state.set('streams', streams.slice())
    case OPERATE_STREAMS_ERROR:
      payload.reject(payload.message)
      return state
    default:
      return state
  }
}

export default streamReducer
