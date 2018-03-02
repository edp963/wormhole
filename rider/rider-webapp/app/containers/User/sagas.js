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
  LOAD_ADMIN_ALL_USERS,
  LOAD_USER_USERS,
  LOAD_NORMAL,
  LOAD_SELECT_USERS,
  ADD_USER,
  EDIT_USER,
  LOAD_EMAIL_INPUT_VALUE,
  EDIT_ROLETYPE_USERPSW,
  EDIT_NORMAL,
  LOAD_USER_DETAIL,
  DELETE_USER,

  LOAD_PROJECT_USER_ALL
} from './constants'
import {
  adminAllUsersLoaded,
  userUsersLoaded,
  normalDetailLoaded,
  selectUsersLoaded,
  userAdded,
  userEdited,
  normalEdited,
  emailInputValueLoaded,
  emailInputValueErrorLoaded,
  roleTypeUserPswEdited,
  roleTypeUserPswErrorEdited,
  userDetailLoaded,
  userDeleted,
  userDeletedError,

  projectUserAllLoaded,
  getError
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'

export function* getAdminAllUsers ({ payload }) {
  try {
    const users = yield call(request, api.user)
    yield put(adminAllUsersLoaded(users.payload))
    payload.resolve()
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getAdminAllUsersWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_ALL_USERS, getAdminAllUsers)
}

export function* getUserUsers ({ payload }) {
  try {
    const users = yield call(request, `${api.projectUserList}/${payload.projectId}/users`)
    yield put(userUsersLoaded(users.payload))
    payload.resolve()
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getUserUsersWatcher () {
  yield fork(takeLatest, LOAD_USER_USERS, getUserUsers)
}

export function* getNormal ({ payload }) {
  try {
    const users = yield call(request, `${api.userNormal}/${payload.userId}`)
    yield put(normalDetailLoaded(users.payload))
    payload.resolve(users.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getNormalWatcher () {
  yield fork(takeLatest, LOAD_NORMAL, getNormal)
}

export function* getSelectUsers ({ payload }) {
  try {
    const users = yield call(request, `${api.projectList}/${payload.projectId}/users`)
    yield put(selectUsersLoaded(users.payload))
    payload.resolve(users.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getSelectUsersWatcher () {
  yield fork(takeLatest, LOAD_SELECT_USERS, getSelectUsers)
}

export function* addUser ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: api.user,
      data: payload.user
    })
    yield put(userAdded(result.payload))
    payload.resolve()
  } catch (err) {
    yield put(getError(err))
  }
}

export function* addUserWatcher () {
  yield fork(takeEvery, ADD_USER, addUser)
}

export function* editUser ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: api.user,
      data: payload.user
    })
    yield put(userEdited(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* editUserWatcher () {
  yield fork(takeEvery, EDIT_USER, editUser)
}

export function* editNormal ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.userNormal}/${payload.value.id}`,
      data: payload.value
    })
    yield put(normalEdited(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* editNormalWatcher () {
  yield fork(takeEvery, EDIT_NORMAL, editNormal)
}

export function* getEmailInputValue ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.user}?email=${payload.value}`,
      data: payload.value
    })
    if (result.code === 409) {
      yield put(emailInputValueErrorLoaded(result.msg))
      payload.reject()
    } else {
      yield put(emailInputValueLoaded(result.msg))
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getEmailInputValueWatcher () {
  yield fork(throttle, 500, LOAD_EMAIL_INPUT_VALUE, getEmailInputValue)
}

export function* editroleTypeUserPsw ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: api.changepwd,
      data: payload.pwdValues
    })
    if (result.code !== 200) {
      yield put(roleTypeUserPswErrorEdited(result.msg))
      payload.reject(result.msg)
    } else {
      yield put(roleTypeUserPswEdited(result.msg))
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* editroleTypeUserPswWatcher () {
  yield fork(takeEvery, EDIT_ROLETYPE_USERPSW, editroleTypeUserPsw)
}

export function* getProjectUserAll ({ payload }) {
  try {
    const users = yield call(request, `${api.projectList}/users`)
    yield put(projectUserAllLoaded(users.payload))
    payload.resolve(users.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getProjectUserAllWatcher () {
  yield fork(takeLatest, LOAD_PROJECT_USER_ALL, getProjectUserAll)
}

export function* queryUser ({payload}) {
  try {
    const result = yield call(request, `${api.user}/${payload.userId}`)
    yield put(userDetailLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    yield put(getError(err))
  }
}

export function* queryUserWatcher () {
  yield fork(takeLatest, LOAD_USER_DETAIL, queryUser)
}

export function* deleteUserAction ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'delete',
      url: `${api.user}/${payload.userId}`
    })
    if (result.code === 412) {
      yield put(userDeletedError(result.msg))
      payload.reject(result.msg)
    } else if (result.code === 200) {
      yield put(userDeleted(payload.userId))
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* deleteNsActionWatcher () {
  yield fork(takeEvery, DELETE_USER, deleteUserAction)
}

export default [
  getAdminAllUsersWatcher,
  getUserUsersWatcher,
  getNormalWatcher,
  getSelectUsersWatcher,
  addUserWatcher,
  editUserWatcher,
  editNormalWatcher,
  getEmailInputValueWatcher,
  editroleTypeUserPswWatcher,
  queryUserWatcher,
  deleteNsActionWatcher,
  getProjectUserAllWatcher
]
