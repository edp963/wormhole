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

import { takeEvery, takeLatest } from 'redux-saga'
import { put, fork, call } from 'redux-saga/effects'
import {
  LOAD_SOURCE_NAMESPACES,
  LOAD_UDFS,
  LOAD_SINGLE_UDF,
  LOAD_LASTEST_OFFSET,
  START_DEBUG,
  STOP_DEBUG
} from './constants'
import {
  sourceNamespacesLoaded,
  getError,
  lastestOffsetLoaded,
  startDebugSuccess,
  startDebugError
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'
import { notifySagasError } from '../../utils/util'

export function* getSourceNamespaces () {
  try {
    const sourceNamespaces = yield call(request, api.sourceNamespace)
    yield put(sourceNamespacesLoaded(sourceNamespaces))
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getUdfs ({payload}) {
  const apiFinal = payload.roleType === 'admin'
    ? `${api.projectAdminStream}`
    : `${api.projectStream}`
  try {
    const result = yield call(request, `${apiFinal}/${payload.projectId}/${payload.type}/${payload.id}/udfs`)
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getUdfs')
  }
}

export function* getSingleUdf ({ payload }) {
  let urlTemp = ''
  if (payload.roleType === 'admin') {
    urlTemp = `${api.projectList}/${payload.projectId}/udfs`
  } else if (payload.roleType === 'user') {
    urlTemp = `${api.projectUserList}/${payload.projectId}/udfs/${payload.type || 'all'}`
  } else if (payload.roleType === 'adminSelect') {
    urlTemp = `${api.projectList}/${payload.projectId}/udfs?public=false`
  }

  try {
    const result = yield call(request, urlTemp)
    payload.resolve(result.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getLastestOffset ({ payload }) {
  let req = null
  if (payload.method === 'get') {
    if (payload.sourceNs) {
      req = `${api.projectStream}/${payload.projectId}/topics?sourceNs=${payload.sourceNs}`
    } else {
      req = `${api.projectStream}/${payload.projectId}/${payload.type}/${payload.id}/topics`
    }
  } else if (payload.method === 'post') {
    req = {
      method: 'post',
      url: `${api.projectStream}/${payload.projectId}/${payload.type}/${payload.id}/topics`,
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

export function* startDebug ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectStream}/${payload.projectId}/streams/${payload.id}/${payload.action}`,
      data: payload.topicResult
    })
    if (result.code && result.code !== 200) {
      yield put(startDebugError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(startDebugSuccess(result.payload))
      payload.resolve(result.payload)
    } else {
      yield put(startDebugError(result.payload))
      payload.reject(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'startDebug')
  }
}

export function* stopDebug ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectStream}/${payload.projectId}/streams/${payload.id}/stopDebug`,
      data: {logPath: payload.logPath}
    })
    if (result.code && result.code === 200) {
      payload.resolve(result.payload)
    } else if (result.header.code && result.header.code === 200) {
      payload.resolve(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'stopDebug')
  }
}

export function* getSourceNamespacesWatcher () {
  yield fork(takeLatest, LOAD_SOURCE_NAMESPACES, getSourceNamespaces)
}

export function* getUdfsWatcher () {
  yield fork(takeEvery, LOAD_UDFS, getUdfs)
}

export function* getSingleUdfWatcher () {
  yield fork(takeLatest, LOAD_SINGLE_UDF, getSingleUdf)
}

export function* getLastestOffsetWatcher () {
  yield fork(takeLatest, LOAD_LASTEST_OFFSET, getLastestOffset)
}

export function* startDebugWathcer () {
  yield fork(takeEvery, START_DEBUG, startDebug)
}

export function* stopDebugWathcer () {
  yield fork(takeEvery, STOP_DEBUG, stopDebug)
}

export default [
  getSourceNamespacesWatcher,
  getUdfsWatcher,
  getSingleUdfWatcher,
  getLastestOffsetWatcher,
  startDebugWathcer,
  stopDebugWathcer
]
