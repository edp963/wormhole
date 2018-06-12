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
  LOAD_ADMIN_ALL_NAMESPACES,
  LOAD_USER_NAMESPACES,
  LOAD_SELECT_NAMESPACES,
  LOAD_NAMESPACE_DATABASE,
  LOAD_TABLE_NAME_EXIST,
  ADD_NAMESPACE,
  LOAD_SINGLE_NAMESPACE,
  EDIT_NAMESPACE,
  LOAD_PROJECT_NS_ALL,
  SET_SCHEMA,
  QUERY_SCHEMA_CONFIG,
  DELETE_NS
} from './constants'
import {
  adminAllNamespacesLoaded,
  userNamespacesLoaded,
  selectNamespacesLoaded,
  namespaceDatabaseLoaded,
  tableNameExistLoaded,
  tableNameExistErrorLoaded,
  namespaceAdded,
  singleNamespaceLoaded,
  namespaceEdited,
  projectNsAllLoaded,
  schemaSetted,
  schemaConfigQueried,
  nsDeleted,
  nsDeletedError,
  getError
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'

export function* getAdminAllNamespaces ({ payload }) {
  try {
    const namespaces = yield call(request, api.namespace)
    yield put(adminAllNamespacesLoaded(namespaces.payload))
    payload.resolve()
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getAdminAllNamespacesWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_ALL_NAMESPACES, getAdminAllNamespaces)
}

export function* getUserNamespaces ({ payload }) {
  try {
    const namespaces = yield call(request, `${api.projectUserList}/${payload.projectId}/namespaces`)
    yield put(userNamespacesLoaded(namespaces.payload))
    payload.resolve()
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getUserNamespacesWatcher () {
  yield fork(takeLatest, LOAD_USER_NAMESPACES, getUserNamespaces)
}

export function* getSelectNamespaces ({ payload }) {
  try {
    const namespaces = yield call(request, `${api.projectList}/${payload.projectId}/namespaces`)
    yield put(selectNamespacesLoaded(namespaces.payload))
    payload.resolve(namespaces.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getSelectNamespacesWatcher () {
  yield fork(takeLatest, LOAD_SELECT_NAMESPACES, getSelectNamespaces)
}

export function* getNamespaceDatabase ({ payload }) {
  try {
    const database = yield call(request, `${api.instance}/${payload.instanceId}/databases`)
    yield put(namespaceDatabaseLoaded(database.payload))
    payload.resolve(database.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getNamespaceDatabaseWatcher () {
  yield fork(takeLatest, LOAD_NAMESPACE_DATABASE, getNamespaceDatabase)
}

export function* getNsTableName ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.namespace}?instanceId=${payload.value.instanceId}&databaseId=${payload.value.databaseId}&tableNames=${payload.value.tableNames}`
    })
    if (result.code === 200) {
      yield put(tableNameExistLoaded(result.msg))
      payload.resolve()
    } else {
      yield put(tableNameExistErrorLoaded(result.msg))
      payload.reject(result.msg)
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getNsTableNameWatcher () {
  yield fork(throttle, 500, LOAD_TABLE_NAME_EXIST, getNsTableName)
}

export function* addNamespace ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: api.namespace,
      data: payload.value
    })
    yield put(namespaceAdded(result.payload))
    payload.resolve()
  } catch (err) {
    yield put(getError(err))
  }
}

export function* addNamespaceWatcher () {
  yield fork(takeEvery, ADD_NAMESPACE, addNamespace)
}

export function* getSingleNamespace ({ payload }) {
  try {
    const namespace = yield call(request, `${api.namespace}/${payload.namespaceId}`)
    yield put(singleNamespaceLoaded(namespace.payload))
    payload.resolve(namespace.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getSelectNamespaceWatcher () {
  yield fork(takeLatest, LOAD_SINGLE_NAMESPACE, getSingleNamespace)
}

export function* editNamespace ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: api.namespace,
      data: payload.value
    })
    yield put(namespaceEdited(result.payload))
    payload.resolve()
  } catch (err) {
    yield put(getError(err))
  }
}

export function* editNamespaceWatcher () {
  yield fork(takeEvery, EDIT_NAMESPACE, editNamespace)
}

export function* getProjectNsAll ({ payload }) {
  try {
    const result = yield call(request, `${api.projectList}/namespaces`)
    yield put(projectNsAllLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    yield put(getError())
  }
}

export function* getProjectNsAllWatcher () {
  yield fork(takeLatest, LOAD_PROJECT_NS_ALL, getProjectNsAll)
}

export function* setSchema ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.namespace}/${payload.namespaceId}/schema/${payload.type}`,
      data: payload.value
    })
    yield put(schemaSetted(result.payload))
    payload.resolve()
  } catch (err) {
    yield put(getError())
  }
}

export function* setSchemaWatcher () {
  yield fork(takeLatest, SET_SCHEMA, setSchema)
}

export function* querySchema ({ payload }) {
  let requestURL = ''
  if (localStorage.getItem('loginRoleType') === 'admin') {
    requestURL = api.namespace
  } else if (localStorage.getItem('loginRoleType') === 'user') {
    requestURL = `${api.projectUserList}/${payload.ids.projectId}/namespaces`
  }

  try {
    const result = yield call(request, `${requestURL}/${payload.ids.namespaceId}/schema/${payload.type}`)
    yield put(schemaConfigQueried(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    yield put(getError())
  }
}

export function* querySchemaWatcher () {
  yield fork(takeLatest, QUERY_SCHEMA_CONFIG, querySchema)
}

export function* deleteNsAction ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'delete',
      url: `${api.namespace}/${payload.namespaceId}`
    })
    if (result.code === 412) {
      yield put(nsDeletedError(result.msg))
      payload.reject(result.msg)
    } else if (result.code === 200) {
      yield put(nsDeleted(payload.namespaceId))
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* deleteNsActionWatcher () {
  yield fork(takeEvery, DELETE_NS, deleteNsAction)
}

export default [
  getAdminAllNamespacesWatcher,
  getUserNamespacesWatcher,
  getSelectNamespacesWatcher,
  getNamespaceDatabaseWatcher,
  getNsTableNameWatcher,
  addNamespaceWatcher,
  getSelectNamespaceWatcher,
  editNamespaceWatcher,
  getProjectNsAllWatcher,
  setSchemaWatcher,
  querySchemaWatcher,
  deleteNsActionWatcher
]
