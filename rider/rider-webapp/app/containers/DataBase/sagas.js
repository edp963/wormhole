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
  LOAD_DATABASES,
  ADD_DATABASE,
  LOAD_SINGLE_DATABASE,
  EDIT_DATABASE,
  LOAD_DATABASES_INSTANCE,
  DELETE_DB,
  CHECK_DATABASE
} from './constants'
import {
  databasesLoaded,
  databaseAdded,
  databaseAddedError,
  singleDatabaseLoaded,
  databaseEdited,
  databaseEditedError,
  databasesInstanceLoaded,
  dBDeleted,
  dBDeletedError,
  getError
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'

export function* getDatabases ({ payload }) {
  try {
    const result = yield call(request, api.database)
    yield put(databasesLoaded(result.payload))
    payload.resolve()
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getDatabasesWatcher () {
  yield fork(takeLatest, LOAD_DATABASES, getDatabases)
}

export function* addDatabase ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: api.database,
      data: payload.database
    })
    if (result.code && result.code === 400) {
      yield put(databaseAddedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(databaseAdded(result.payload))
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* addDatabaseWatcher () {
  yield fork(takeEvery, ADD_DATABASE, addDatabase)
}

export function* getSingleDatabase ({ payload }) {
  try {
    const result = yield call(request, `${api.database}/${payload.databaseId}`)
    yield put(singleDatabaseLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* singleDatabaseWatcher () {
  yield fork(takeEvery, LOAD_SINGLE_DATABASE, getSingleDatabase)
}

export function* editDatabase ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: api.database,
      data: payload.database
    })
    if (result.code && result.code === 400) {
      yield put(databaseEditedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(databaseEdited(result.payload))
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* editDatabaseWatcher () {
  yield fork(takeEvery, EDIT_DATABASE, editDatabase)
}

export function* getDatabaseInstance ({ payload }) {
  try {
    const result = yield call(request, `${api.instance}?type=${payload.value}`)
    yield put(databasesInstanceLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getDatabaseInstanceWatcher () {
  yield fork(takeLatest, LOAD_DATABASES_INSTANCE, getDatabaseInstance)
}

export function* deleteDBAction ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'delete',
      url: `${api.database}/${payload.databaseId}`
    })
    if (result.code === 412) {
      yield put(dBDeletedError(result.msg))
      payload.reject(result.msg)
    } else if (result.code === 200) {
      yield put(dBDeleted(payload.databaseId))
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* deleteDBActionWatcher () {
  yield fork(takeEvery, DELETE_DB, deleteDBAction)
}

export function* checkDatabase (action) {
  const { id, name, resolve, reject } = action.payload
  try {
    const asyncData = yield call(request, {
      method: 'get',
      url: `${api.database}?nsInstanceId=${id}&nsDatabaseName=${name}`
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

export function* checkDatabaseWatcher () {
  yield throttle(1000, CHECK_DATABASE, checkDatabase)
}

export default [
  getDatabasesWatcher,
  addDatabaseWatcher,
  singleDatabaseWatcher,
  editDatabaseWatcher,
  getDatabaseInstanceWatcher,
  deleteDBActionWatcher,
  checkDatabaseWatcher
]
