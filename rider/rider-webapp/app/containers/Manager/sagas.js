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

import { takeLatest, takeEvery } from 'redux-saga'
import { call, fork, put } from 'redux-saga/effects'
import {
  LOAD_USER_STREAMS,
  LOAD_ADMIN_ALL_STREAMS,
  LOAD_ADMIN_SINGLE_STREAM,
  LOAD_STREAM_DETAIL,
  LOAD_OFFSET,
  LOAD_STREAM_NAME_VALUE,
  LOAD_KAFKA,
  LOAD_STREAM_CONFIG_JVM,
  LOAD_LOGS_INFO,
  LOAD_ADMIN_LOGS_INFO,
  ADD_STREAMS,
  EDIT_STREAM,
  OPERATE_STREAMS,
  DELETE_STREAMS,
  STARTORRENEW_STREAMS
} from './constants'

import {
  userStreamsLoaded,
  adminAllStreamsLoaded,
  adminSingleStreamLoaded,
  streamDetailLoaded,
  offsetLoaded,
  streamNameValueLoaded,
  streamNameValueErrorLoaded,
  kafkaLoaded,
  streamConfigJvmLoaded,
  logsInfoLoaded,
  adminLogsInfoLoaded,
  streamAdded,
  streamEdited,
  streamOperated,
  streamDeleted,
  streamStartOrRenewed,
  streamOperatedError
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'
import { notifySagasError } from '../../utils/util'

export function* getUserStreams ({ payload }) {
  try {
    const streams = yield call(request, `${api.projectStream}/${payload.projectId}/streams`)
    yield put(userStreamsLoaded(streams.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getUserStreams')
  }
}

export function* getUserStreamsWatcher () {
  yield fork(takeLatest, LOAD_USER_STREAMS, getUserStreams)
}

export function* getAdminAllStreams ({ payload }) {
  try {
    const streams = yield call(request, api.stream)
    yield put(adminAllStreamsLoaded(streams.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getAdminAllStreams')
  }
}

export function* getAdminAllFlowsWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_ALL_STREAMS, getAdminAllStreams)
}

export function* getAdminSingleStream ({ payload }) {
  try {
    const streams = yield call(request, `${api.projectAdminStream}/${payload.projectId}/streams`)
    yield put(adminSingleStreamLoaded(streams.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getAdminSingleStream')
  }
}

export function* getAdminSingleFlowWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_SINGLE_STREAM, getAdminSingleStream)
}

export function* getStreamDetail ({ payload }) {
  const apiFinal = payload.roleType === 'admin'
    ? `${api.projectAdminStream}`
    : `${api.projectStream}`
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${apiFinal}/${payload.projectId}/streams/${payload.streamId}`
    })
    yield put(streamDetailLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getStreamDetail')
  }
}

export function* getStreamDetailWatcher () {
  yield fork(takeLatest, LOAD_STREAM_DETAIL, getStreamDetail)
}

export function* getOffset ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.projectStream}/${payload.values.id}/streams/${payload.values.streamId}/topics/offsets/latest`
    })
    yield put(offsetLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getOffset')
  }
}

export function* getOffsetWatcher () {
  yield fork(takeLatest, LOAD_OFFSET, getOffset)
}

export function* getStreamNameValue ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.projectStream}/${payload.projectId}/streams?streamName=${payload.value}`
    })
    if (result.code === 409) {
      yield put(streamNameValueErrorLoaded(result.msg, payload.reject))
    } else {
      yield put(streamNameValueLoaded(result.payload, payload.resolve))
    }
  } catch (err) {
    notifySagasError(err, 'getStreamNameValue')
  }
}

export function* getStreamNameValueWatcher () {
  yield fork(takeLatest, LOAD_STREAM_NAME_VALUE, getStreamNameValue)
}

export function* getKafka ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.projectStream}/${payload.projectId}/instances?nsSys=kafka`
    })
    yield put(kafkaLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getKafka')
  }
}

export function* getKafkaWatcher () {
  yield fork(takeLatest, LOAD_KAFKA, getKafka)
}

export function* getStreamConfigJvm ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.projectStream}/streams/default/config`
    })
    yield put(streamConfigJvmLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getStreamConfigJvm')
  }
}

export function* getStreamConfigJvmWatcher () {
  yield fork(takeLatest, LOAD_STREAM_CONFIG_JVM, getStreamConfigJvm)
}

export function* getLogs ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.projectStream}/${payload.projectId}/streams/${payload.streamId}/logs`
    })
    yield put(logsInfoLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getLogs')
  }
}

export function* getLogsWatcher () {
  yield fork(takeLatest, LOAD_LOGS_INFO, getLogs)
}

export function* getAdminLogs ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.projectList}/${payload.projectId}/streams/${payload.streamId}/logs`
    })
    yield put(adminLogsInfoLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getAdminLogs')
  }
}

export function* getAdminLogsWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_LOGS_INFO, getAdminLogs)
}

export function* addStream ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: `${api.projectUserList}/${payload.projectId}/streams`,
      data: payload.stream
    })
    yield put(streamAdded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'addStream')
  }
}

export function* addStreamWathcer () {
  yield fork(takeEvery, ADD_STREAMS, addStream)
}

export function* editStream ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectStream}/${payload.stream.projectId}/streams`,
      data: payload.stream
    })
    yield put(streamEdited(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'editStream')
  }
}

export function* editStreamWathcer () {
  yield fork(takeEvery, EDIT_STREAM, editStream)
}

export function* operateStream ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectStream}/${payload.projectId}/streams/${payload.id}/${payload.action}`
    })
    if (result.code && result.code !== 200) {
      yield put(streamOperatedError(result.msg, payload.reject))
    } else if (result.header.code && result.header.code === 200) {
      yield put(streamOperated(result.payload, payload.resolve))
    }
  } catch (err) {
    notifySagasError(err, 'operateStream')
  }
}

export function* operateStreamWathcer () {
  yield fork(takeEvery, OPERATE_STREAMS, operateStream)
}

export function* deleteStream ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectStream}/${payload.projectId}/streams/${payload.id}/${payload.action}`
    })
    if (result.code && result.code !== 200) {
      yield put(streamOperatedError(result.msg, payload.reject))
    } else if (result.code && result.code === 200) {
      yield put(streamDeleted(payload.id, payload.resolve))
    }
  } catch (err) {
    notifySagasError(err, 'deleteStream')
  }
}

export function* deleteStreamWathcer () {
  yield fork(takeEvery, DELETE_STREAMS, deleteStream)
}

export function* startOrRenewStream ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectStream}/${payload.projectId}/streams/${payload.id}/${payload.action}`,
      data: payload.topicResult
    })
    if (result.code && result.code !== 200) {
      yield put(streamOperatedError(result.msg, payload.reject))
    } else if (result.header.code && result.header.code === 200) {
      yield put(streamStartOrRenewed(result.payload, payload.resolve))
    }
  } catch (err) {
    notifySagasError(err, 'startOrRenewStream')
  }
}

export function* startOrRenewStreamWathcer () {
  yield fork(takeEvery, STARTORRENEW_STREAMS, startOrRenewStream)
}

export default [
  getUserStreamsWatcher,
  getAdminAllFlowsWatcher,
  getAdminSingleFlowWatcher,
  getStreamDetailWatcher,
  getOffsetWatcher,
  getStreamNameValueWatcher,
  getKafkaWatcher,
  getStreamConfigJvmWatcher,
  getLogsWatcher,
  getAdminLogsWatcher,
  addStreamWathcer,
  editStreamWathcer,
  operateStreamWathcer,
  deleteStreamWathcer,
  startOrRenewStreamWathcer
]
