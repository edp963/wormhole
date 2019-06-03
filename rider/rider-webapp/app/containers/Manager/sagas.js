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
  LOAD_FLOW_LIST,
  SET_FLOW_PRIORITY,
  LOAD_ADMIN_ALL_STREAMS,
  LOAD_ADMIN_SINGLE_STREAM,
  LOAD_STREAM_DETAIL,
  LOAD_STREAM_NAME_VALUE,
  LOAD_KAFKA,
  LOAD_STREAM_CONFIG_JVM,
  LOAD_STREAM_CONFIG_SPARK,
  LOAD_LOGS_INFO,
  LOAD_ADMIN_LOGS_INFO,
  ADD_STREAMS,
  EDIT_STREAM,
  OPERATE_STREAMS,
  DELETE_STREAMS,
  STARTORRENEW_STREAMS,
  LOAD_LASTEST_OFFSET,
  POST_USER_TOPIC,
  DELETE_USER_TOPIC,
  LOAD_UDFS,
  LOAD_STREAM_CONFIGS,
  LOAD_YARN_UI
} from './constants'

import {
  userStreamsLoaded,
  flowListLoaded,
  flowListOfPrioritySubmited,
  adminAllStreamsLoaded,
  adminSingleStreamLoaded,
  streamDetailLoaded,
  streamNameValueLoaded,
  streamNameValueErrorLoaded,
  kafkaLoaded,
  streamConfigJvmLoaded,
  streamConfigSparkLoaded,
  logsInfoLoaded,
  adminLogsInfoLoaded,
  streamAdded,
  streamEdited,
  streamOperated,
  streamDeleted,
  streamStartOrRenewed,
  streamOperatedError,
  lastestOffsetLoaded,
  postUserTopicLoaded,
  deleteUserTopicLoaded
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'
import { notifySagasError } from '../../utils/util'
import { message } from 'antd'

export function* getUserStreams ({ payload }) {
  try {
    const streams = yield call(request, `${api.projectStream}/${payload.projectId}/streams`)
    yield put(userStreamsLoaded(streams.payload))
    payload.resolve()
  } catch (err) {
    notifySagasError(err, 'getUserStreams')
  }
}

export function* getUserStreamsWatcher () {
  yield fork(takeLatest, LOAD_USER_STREAMS, getUserStreams)
}

export function* getFlowList ({ payload }) {
  try {
    const flows = yield call(request, `${api.projectStream}/${payload.projectId}/streams/${payload.streamId}/flows/order`)
    yield put(flowListLoaded(flows.payload.flowPrioritySeq))
    payload.resolve()
  } catch (err) {
    notifySagasError(err, 'getFlowList')
  }
}

export function* getFlowListWatcher () {
  yield fork(takeLatest, LOAD_FLOW_LIST, getFlowList)
}

export function* setPriority ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectStream}/${payload.projectId}/streams/${payload.streamId}/flows/order`,
      data: payload.flows
    })
    if (result.header && result.header.code !== 200) {
      yield put(streamOperatedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header && result.header.code === 200) {
      yield put(flowListOfPrioritySubmited(result.payload))
      payload.resolve()
    }
  } catch (err) {
    notifySagasError(err, 'setPriority')
  }
}

export function* setPriorityWathcer () {
  yield fork(takeEvery, SET_FLOW_PRIORITY, setPriority)
}

export function* getAdminAllStreams ({ payload }) {
  try {
    const streams = yield call(request, api.stream)
    yield put(adminAllStreamsLoaded(streams.payload))
    payload.resolve()
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
    yield put(adminSingleStreamLoaded(streams.payload))
    payload.resolve()
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
    const result = yield call(request, `${apiFinal}/${payload.projectId}/streams/${payload.streamId}`)
    yield put(streamDetailLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getStreamDetail')
  }
}

export function* getStreamDetailWatcher () {
  yield fork(takeLatest, LOAD_STREAM_DETAIL, getStreamDetail)
}

export function* getStreamNameValue ({ payload }) {
  try {
    const result = yield call(request, `${api.projectStream}/${payload.projectId}/streams?streamName=${payload.value}`)
    if (result.header && result.header.code === 451) {
      yield put(streamNameValueErrorLoaded(result.payload))
      payload.reject(result.payload)
    } else {
      yield put(streamNameValueLoaded(result.payload))
      payload.resolve()
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
    const result = yield call(request, `${api.projectStream}/${payload.projectId}/instances?nsSys=kafka`)
    yield put(kafkaLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getKafka')
  }
}

export function* getKafkaWatcher () {
  yield fork(takeLatest, LOAD_KAFKA, getKafka)
}
// ======= 由下面api替代 ====
export function* getStreamConfigJvm ({ payload }) {
  try {
    const result = yield call(request, `${api.projectStream}/streams/default/config/jvm`)
    yield put(streamConfigJvmLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getStreamConfigJvm')
  }
}

export function* getStreamConfigJvmWatcher () {
  yield fork(takeLatest, LOAD_STREAM_CONFIG_JVM, getStreamConfigJvm)
}

export function* getStreamConfigSpark ({ payload }) {
  try {
    const result = yield call(request, `${api.projectStream}/streams/default/config/spark`)
    yield put(streamConfigSparkLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getStreamConfigSpark')
  }
}

export function* getStreamConfigSparkWatcher () {
  yield fork(takeLatest, LOAD_STREAM_CONFIG_SPARK, getStreamConfigSpark)
}
// ------------ 下面 替代 上面 ---
export function* getStreamDefaultconfigs ({ payload }) {
  try {
    const result = yield call(request, `${api.userStream}/streams/defaultconfigs?streamType=${payload.type}`)
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getStreamDefaultconfigs')
  }
}

export function* getStreamDefaultconfigsWatcher () {
  yield fork(takeLatest, LOAD_STREAM_CONFIGS, getStreamDefaultconfigs)
}
// ============================

export function* getLogs ({ payload }) {
  try {
    const result = yield call(request, `${api.projectStream}/${payload.projectId}/streams/${payload.streamId}/logs`)
    yield put(logsInfoLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getLogs')
  }
}

export function* getLogsWatcher () {
  yield fork(takeLatest, LOAD_LOGS_INFO, getLogs)
}

export function* getAdminLogs ({ payload }) {
  try {
    const result = yield call(request, `${api.projectList}/${payload.projectId}/streams/${payload.streamId}/logs`)
    yield put(adminLogsInfoLoaded(result.payload))
    payload.resolve(result.payload)
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
    yield put(streamAdded(result.payload))
    payload.resolve()
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
    yield put(streamEdited(result.payload))
    payload.resolve()
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
      yield put(streamOperatedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(streamOperated(result.payload))
      payload.resolve()
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
      yield put(streamOperatedError(result.msg))
      payload.reject(result.msg)
    } else if (result.code && result.code === 200) {
      yield put(streamDeleted(payload.id))
      payload.resolve()
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
      yield put(streamOperatedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(streamStartOrRenewed(result.payload))
      payload.resolve()
    } else {
      yield put(streamOperatedError(result.payload))
      payload.reject(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'startOrRenewStream')
  }
}

export function* startOrRenewStreamWathcer () {
  yield fork(takeEvery, STARTORRENEW_STREAMS, startOrRenewStream)
}

export function* getLastestOffset ({ payload }) {
  let req = null
  if (payload.type === 'get') {
    req = `${api.projectStream}/${payload.projectId}/streams/${payload.streamId}/topics`
  } else if (payload.type === 'post') {
    req = {
      method: 'post',
      url: `${api.projectStream}/${payload.projectId}/streams/${payload.streamId}/topics`,
      data: payload.topics
    }
  }
  try {
    const result = yield call(request, req)
    if (result.code && result.code === 200) {
      yield put(lastestOffsetLoaded(result.msg))
      payload.resolve(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(lastestOffsetLoaded(result.payload))
      payload.resolve(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'getLastestOffset')
  }
}

export function* getLastestOffsetWatcher () {
  yield fork(takeLatest, LOAD_LASTEST_OFFSET, getLastestOffset)
}

export function* addUserTopic ({payload}) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: `${api.projectUserList}/${payload.projectId}/streams/${payload.streamId}/topics/userdefined`,
      data: payload.topic
    })
    if (result.header.code && result.header.code === 200) {
      yield put(postUserTopicLoaded(result.payload))
      payload.resolve(result.payload)
    } else {
      payload.reject(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'addUserTopic')
  }
}

export function* addUserTopicWatcher () {
  yield fork(takeEvery, POST_USER_TOPIC, addUserTopic)
}

export function* removeUserTopic ({payload}) {
  try {
    const result = yield call(request, {
      method: 'delete',
      url: `${api.projectUserList}/${payload.projectId}/streams/${payload.streamId}/topics/userdefined/${payload.topicId}`
    })
    if (result.header.code && result.header.code === 200) {
      yield put(deleteUserTopicLoaded(result.payload))
      payload.resolve(result.payload)
    } else {
      payload.reject(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'removeUserTopic')
  }
}

export function* removeUserTopicWatcher () {
  yield fork(takeEvery, DELETE_USER_TOPIC, removeUserTopic)
}

export function* getUdfs ({payload}) {
  const apiFinal = payload.roleType === 'admin'
  ? `${api.projectAdminStream}`
  : `${api.projectStream}`
  try {
    const result = yield call(request, `${apiFinal}/${payload.projectId}/streams/${payload.streamId}/udfs`)
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getUdfs')
  }
}

export function* getUdfsWatcher () {
  yield fork(takeEvery, LOAD_UDFS, getUdfs)
}

export function* getYarnUi ({payload}) {
  const apiFinal = payload.roleType === 'admin'
  ? `${api.projectAdminStream}`
  : `${api.projectStream}`
  try {
    const result = yield call(request, `${apiFinal}/${payload.projectId}/streams/${payload.streamId}/yarnUi`)
    if (result.header.code === 200) {
      payload.resolve(result.payload)
    } else {
      message.warning(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'getYarnUi')
  }
}

export function* getYarnUiWatcher () {
  yield fork(takeEvery, LOAD_YARN_UI, getYarnUi)
}
export default [
  getUserStreamsWatcher,
  getFlowListWatcher,
  setPriorityWathcer,
  getAdminAllFlowsWatcher,
  getAdminSingleFlowWatcher,
  getStreamDetailWatcher,
  getStreamNameValueWatcher,
  getKafkaWatcher,
  getStreamConfigJvmWatcher,
  getStreamConfigSparkWatcher,
  getLogsWatcher,
  getAdminLogsWatcher,
  addStreamWathcer,
  editStreamWathcer,
  operateStreamWathcer,
  deleteStreamWathcer,
  startOrRenewStreamWathcer,
  getLastestOffsetWatcher,
  addUserTopicWatcher,
  removeUserTopicWatcher,
  getUdfsWatcher,
  getStreamDefaultconfigsWatcher,
  getYarnUiWatcher
]
