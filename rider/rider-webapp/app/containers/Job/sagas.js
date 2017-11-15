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

import {takeLatest, takeEvery
  // throttle
} from 'redux-saga'
import { call, fork, put } from 'redux-saga/effects'
import {
  LOAD_ADMIN_ALL_JOBS,
  LOAD_USER_ALL_JOBS,
  LOAD_ADMIN_SINGLE_JOB,
  LOAD_ADMIN_JOB_LOGS,
  LOAD_USER_JOB_LOGS,
  OPERATE_JOB
} from './constants'

import {
  adminAllJobsLoaded,
  userAllJobsLoaded,
  adminSingleJobLoaded,
  adminJobLogsLoaded,
  userJobLogsLoaded,
  jobOperated,
  jobOperatedError
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'
import { notifySagasError } from '../../utils/util'

export function* getAdminAllJobs ({ payload }) {
  try {
    const result = yield call(request, api.job)
    yield put(adminAllJobsLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getAdminAllJobs')
  }
}

export function* getAdminAllJobsWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_ALL_JOBS, getAdminAllJobs)
}

export function* getUserAllJobs ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/jobs`)
    yield put(userAllJobsLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getUserAllJobs')
  }
}

export function* getUserAllJobsWatcher () {
  yield fork(takeLatest, LOAD_USER_ALL_JOBS, getUserAllJobs)
}

export function* getAdminSingleJob ({ payload }) {
  try {
    const result = yield call(request, `${api.projectList}/${payload.projectId}/jobs`)
    yield put(adminSingleJobLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getAdminSingleJob')
  }
}

export function* getAdminSingleJobWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_SINGLE_JOB, getAdminSingleJob)
}

export function* getAdminJobLogs ({ payload }) {
  try {
    const result = yield call(request, `${api.job}/${payload.jobId}/logs`)
    yield put(adminJobLogsLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getAdminJobLogs')
  }
}

export function* getAdminJobLogsWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_JOB_LOGS, getAdminJobLogs)
}

export function* getUserJobLogs ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/jobs/${payload.jobId}/logs`)
    yield put(userJobLogsLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getUserJobLogs')
  }
}

export function* getUserJobLogsWatcher () {
  yield fork(takeLatest, LOAD_USER_JOB_LOGS, getUserJobLogs)
}

export function* operateUserJob ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectUserList}/${payload.values.projectId}/jobs/${Number(payload.values.jobId)}/${payload.values.action}`
    })
    if (result.code && result.code !== 200) {
      yield put(jobOperatedError(result.msg, payload.reject))
    } else if (result.header.code && result.header.code === 200) {
      yield put(jobOperated(payload.values.jobId, payload.resolve))
    }
  } catch (err) {
    notifySagasError(err, 'operateUserJob')
  }
}

export function* operateUserJobWatcher () {
  yield fork(takeEvery, OPERATE_JOB, operateUserJob)
}

export default [
  getAdminAllJobsWatcher,
  getUserAllJobsWatcher,
  getAdminSingleJobWatcher,
  getAdminJobLogsWatcher,
  getUserJobLogsWatcher,
  operateUserJobWatcher
]
