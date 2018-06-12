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
  LOAD_PROJECTS,
  LOAD_USER_PROJECTS,
  LOAD_SINGLE_PROJECT,
  ADD_PROJECT,
  EDIT_PROJECT,
  LOAD_PROJECT_NAME_VALUE,
  DELETE_SINGLE_PROJECT
} from './constants'
import {
  projectsLoaded,
  userProjectsLoaded,
  singleProjectLoaded,
  projectAdded,
  projectEdited,
  projectEditedError,
  projectNameInputValueLoaded,
  projectNameInputValueErrorLoaded,
  singleProjectDeleted,
  singleProjectDeletedError,
  getError
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'

export function* getProjects ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: api.projectList,
      params: payload
    })
    yield put(projectsLoaded(result.payload))
  } catch (err) {
    yield put(getError())
  }
}

export function* getProjectsWatcher () {
  yield fork(takeLatest, LOAD_PROJECTS, getProjects)
}

export function* getUserProjects () {
  try {
    const result = yield call(request, api.projectUserList)
    yield put(userProjectsLoaded(result.payload))
  } catch (err) {
    yield put(getError())
  }
}

export function* getUserProjectsWatcher () {
  yield fork(takeLatest, LOAD_USER_PROJECTS, getUserProjects)
}

export function* getSingleProject ({ payload }) {
  let requestUrl = ''
  if (localStorage.getItem('loginRoleType') === 'admin') {
    requestUrl = `${api.projectList}`
  } else if (localStorage.getItem('loginRoleType') === 'user') {
    requestUrl = `${api.projectUserList}`
  }
  try {
    const result = yield call(request, `${requestUrl}/${payload.projectId}`)
    yield put(singleProjectLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    yield put(getError())
  }
}

export function* getSingleProjectWatcher () {
  yield fork(takeLatest, LOAD_SINGLE_PROJECT, getSingleProject)
}

export function* addProject ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: api.projectList,
      data: payload.project
    })
    yield put(projectAdded(result.payload))
    payload.resolve()
    payload.final()
  } catch (err) {
    yield put(getError(payload.final))
  }
}

export function* addProjectWatcher () {
  yield fork(takeEvery, ADD_PROJECT, addProject)
}

export function* editProject ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: api.projectList,
      data: payload.project
    })
    if (result.header.code === 200) {
      yield put(projectEdited(result.payload))
      payload.resolve(result.payload)
    }
    if (result.header.code === 406) {
      yield put(projectEditedError(result))
      payload.reject(result)
    }
  } catch (err) {
    yield put(getError(payload.final))
  }
}

export function* editProjectWatcher () {
  yield fork(takeEvery, EDIT_PROJECT, editProject)
}

export function* getProjectNameInputValue ({ payload }) {
  try {
    const result = yield call(request, `${api.projectList}?name=${payload.value}`)
    if (result.code === 409) {
      yield put(projectNameInputValueErrorLoaded(result.msg))
      payload.reject(result.msg)
    } else {
      yield put(projectNameInputValueLoaded(result.payload))
      payload.resolve()
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* getProjectNameInputValueWatcher () {
  yield fork(throttle, 500, LOAD_PROJECT_NAME_VALUE, getProjectNameInputValue)
}

export function* deleteSinglePro ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'delete',
      url: `${api.projectList}/${payload.projectId}`
    })
    if (result.code === 200) {
      yield put(singleProjectDeleted(payload.projectId))
      payload.resolve()
    } else if (result.code === 412) {
      yield put(singleProjectDeletedError(result.msg))
      payload.reject(result.msg)
    }
  } catch (err) {
    yield put(getError(err))
  }
}

export function* deleteSingleProWatcher () {
  yield fork(takeEvery, DELETE_SINGLE_PROJECT, deleteSinglePro)
}

export default [
  getProjectsWatcher,
  getUserProjectsWatcher,
  getSingleProjectWatcher,
  addProjectWatcher,
  editProjectWatcher,
  getProjectNameInputValueWatcher,
  deleteSingleProWatcher
]
