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

import { takeLatest, takeEvery, throttle } from 'redux-saga'
import { call, fork, put } from 'redux-saga/effects'

import {
  LOAD_INSTANCES,
  ADD_INSTANCE,
  LOAD_SINGLE_INSTANCE,
  EDIT_INSTANCE,
  DELETE_INSTANCE,
  CHECK_INSTANCE,
  CHECK_URL
} from './constants'
import {
  instancesLoaded,
  instanceAdded,
  instanceAddedError,
  singleInstanceLoaded,
  instanceEdited,
  instanceEditedError,
  instanceDeleted,
  instanceDeletedError,
  getError
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'

export function* getInstances ({ payload }) {
  try {
    const result = yield call(request, api.instance)
    yield put(instancesLoaded(result.payload))
    payload.resolve()
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getInstancesWatcher () {
  yield fork(takeLatest, LOAD_INSTANCES, getInstances)
}

export function* addInstance ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: api.instance,
      data: {
        connUrl: payload.instance.connectionUrl,
        desc: payload.instance.description === undefined ? '' : payload.instance.description,
        nsSys: payload.instance.instanceDataSystem,
        nsInstance: payload.instance.instance,
        connConfig: payload.instance.connConfig
      }
    })
    if (result.code && result.code === 400) {
      yield put(instanceAddedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(instanceAdded(result.payload))
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* addInstanceWatcher () {
  yield fork(takeEvery, ADD_INSTANCE, addInstance)
}

export function* getSingleInstance ({ payload }) {
  try {
    const result = yield call(request, `${api.instance}/${payload.instanceId}`)
    yield put(singleInstanceLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* singleInstanceWatcher () {
  yield fork(takeEvery, LOAD_SINGLE_INSTANCE, getSingleInstance)
}

export function* editInstance ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: api.instance,
      data: payload.value
    })

    if (result.code && result.code === 400) {
      yield put(instanceEditedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(instanceEdited(result.payload))
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* editInstanceWatcher () {
  yield fork(takeEvery, EDIT_INSTANCE, editInstance)
}

export function* deleteInstanceAction ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'delete',
      url: `${api.instance}/${payload.instanceId}`
    })
    if (result.code === 412) {
      yield put(instanceDeletedError(result.msg))
      payload.reject(result.msg)
    } else if (result.code === 200) {
      yield put(instanceDeleted(payload.instanceId))
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* deleteInstanceActionWatcher () {
  yield fork(takeEvery, DELETE_INSTANCE, deleteInstanceAction)
}

export function* checkInstance (action) {
  const { type, nsInstance, resolve, reject } = action.payload
  try {
    const asyncData = yield call(request, {
      method: 'get',
      url: `${api.instance}?type=${type}&nsInstance=${nsInstance}`
    })
    const msg = asyncData && asyncData.msg ? asyncData.msg : ''
    const code = asyncData && asyncData.code ? asyncData.code : ''
    if (code && code >= 400) {
      reject(msg)
    }
    if (code && code === 200) {
      resolve(msg)
    }
  } catch (err) {
    console.log(err)
  }
}

export function* checkInstanceWatcher () {
  yield throttle(1000, CHECK_INSTANCE, checkInstance)
}

export function* checkConnectUrl ({ payload }) {
  try {
    const result = yield call(request, `${api.instance}?type=${payload.value.type}&conn_url=${payload.value.conn_url}`)

    if (result.code && (result.code >= 400)) {
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* checkConnectUrlWatcher () {
  yield throttle(1000, CHECK_URL, checkConnectUrl)
}

export default [
  getInstancesWatcher,
  addInstanceWatcher,
  singleInstanceWatcher,
  editInstanceWatcher,
  checkConnectUrlWatcher,
  deleteInstanceActionWatcher,
  checkInstanceWatcher
]
