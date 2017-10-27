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
  LOAD_UDFS,
  LOAD_SINGLE_UDF,
  LOAD_PROJECT_UDFS,
  ADD_UDF,
  EDIT_UDF,
  DELETE_UDF
} from './constants'
import {
  udfsLoaded,
  singleUdfLoaded,
  projectUdfsLoaded,
  udfAdded,
  udfAddedError,
  udfEdited,
  udfEditedError,
  udfDeleted,
  getError
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'

export function* getUdfs ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: api.udf
    })
    yield put(udfsLoaded(result.payload, payload.resolve))
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getUdfsWatcher () {
  yield fork(takeLatest, LOAD_UDFS, getUdfs)
}

export function* getProjectUdfs ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.projectList}/udfs`
    })
    yield put(projectUdfsLoaded(result.payload, payload.resolve))
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getProjectUdfsWatcher () {
  yield fork(takeLatest, LOAD_PROJECT_UDFS, getProjectUdfs)
}

export function* getSingleUdf ({ payload }) {
  let urlTemp = ''
  if (payload.roleType === 'admin') {
    urlTemp = `${api.projectList}/${payload.projectId}/udfs`
  } else if (payload.roleType === 'user') {
    urlTemp = `${api.projectUserList}/${payload.projectId}/udfs`
  } else if (payload.roleType === 'adminSelect') {
    urlTemp = `${api.projectList}/${payload.projectId}/udfs?public=false`
  }

  try {
    const result = yield call(request, {
      method: 'get',
      url: urlTemp
    })
    yield put(singleUdfLoaded(result.payload, payload.resolve))
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getSingleUdfWatcher () {
  yield fork(takeLatest, LOAD_SINGLE_UDF, getSingleUdf)
}

export function* addUdf ({payload}) {
  const publicFinal = payload.values.public === 'true'
  try {
    const result = yield call(request, {
      method: 'post',
      url: api.udf,
      data: {
        functionName: payload.values.functionName,
        fullClassName: payload.values.fullName,
        jarName: payload.values.jarName,
        desc: payload.values.desc,
        public: publicFinal
      }
    })
    if (result.code && (result.code === 409 || result.code === 412)) {
      yield put(udfAddedError(result.msg, payload.reject))
    } else if (result.header.code && result.header.code === 200) {
      yield put(udfAdded(result.payload, payload.resolve))
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* addUdfWatcher () {
  yield fork(takeEvery, ADD_UDF, addUdf)
}

export function* editUdf ({payload}) {
  const publicFinal = payload.values.public === 'true'
  try {
    const result = yield call(request, {
      method: 'put',
      url: api.udf,
      data: {
        functionName: payload.values.functionName,
        fullClassName: payload.values.fullName,
        jarName: payload.values.jarName,
        desc: payload.values.desc,
        pubic: publicFinal,
        id: payload.values.id,
        createTime: payload.values.createTime,
        createBy: payload.values.createBy,
        updateTime: payload.values.updateTime,
        updateBy: payload.values.updateBy
      }
    })
    if (result.code && result.code === 412) {
      yield put(udfEditedError(result.msg, payload.reject))
    } else if (result.header.code && result.header.code === 200) {
      yield put(udfEdited(result.payload, payload.resolve))
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* editUdfWatcher () {
  yield fork(takeEvery, EDIT_UDF, editUdf)
}

export function* deleteUdf ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'delete',
      url: api.udf,
      data: payload.values
    })
    yield put(udfDeleted(result, payload.resolve))
  } catch (err) {
    yield put(getError(err))
  }
}

export function* deleteUdfWatcher () {
  yield fork(takeEvery, DELETE_UDF, deleteUdf)
}

export default [
  getUdfsWatcher,
  getSingleUdfWatcher,
  getProjectUdfsWatcher,
  addUdfWatcher,
  editUdfWatcher,
  deleteUdfWatcher
]
