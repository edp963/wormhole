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
  LOAD_INSTANCES_INPUT_VALUE,
  LOAD_INSTANCES_EXIT,
  LOAD_SINGLE_INSTANCE,
  EDIT_INSTANCE
} from './constants'
import {
  instancesLoaded,
  instanceAdded,
  instanceInputValueLoaded,
  instanceInputValueErrorLoaded,
  instanceExitLoaded,
  instanceExitErrorLoaded,
  singleInstanceLoaded,
  instanceEdited,
  getError
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'

export function* getInstances ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: api.instance
    })
    yield put(instancesLoaded(result.payload, payload.resolve))
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
        nsInstance: payload.instance.instance
      }
    })
    yield put(instanceAdded(result.payload, payload.resolve))
  } catch (err) {
    yield put(getError(err))
  }
}

export function* addInstanceWatcher () {
  yield fork(takeEvery, ADD_INSTANCE, addInstance)
}

export function* getSingleInstance ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.instance}/${payload.instanceId}`
    })
    yield put(singleInstanceLoaded(result.payload, payload.resolve))
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
    yield put(instanceEdited(result.payload, payload.resolve))
  } catch (err) {
    yield put(getError(err))
  }
}

export function* editInstanceWatcher () {
  yield fork(takeEvery, EDIT_INSTANCE, editInstance)
}

export function* getInstanceInputValue ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.instance}?type=${payload.value.type}&conn_url=${payload.value.conn_url}`
    })

    if (result.code === 409 || result.code === 400) {
      yield put(instanceInputValueErrorLoaded(result.msg, payload.reject))
    } else {
      yield put(instanceInputValueLoaded(result.payload, payload.resolve))
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getInstanceInputValueWatcher () {
  yield throttle(800, LOAD_INSTANCES_INPUT_VALUE, getInstanceInputValue)
}

export function* getInstanceValExit ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.instance}?type=${payload.value.type}&nsInstance=${payload.value.nsInstance}`
    })
    if (result.code === 409) {
      yield put(instanceExitErrorLoaded(result.msg, payload.reject))
    } else {
      yield put(instanceExitLoaded(payload.result, payload.resolve))
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getInstanceValExitWatcher () {
  yield throttle(800, LOAD_INSTANCES_EXIT, getInstanceValExit)
}

export default [
  getInstancesWatcher,
  addInstanceWatcher,
  singleInstanceWatcher,
  editInstanceWatcher,
  getInstanceInputValueWatcher,
  getInstanceValExitWatcher
]
