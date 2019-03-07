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

import {takeLatest, takeEvery, throttle
} from 'redux-saga'
import { call, fork, put } from 'redux-saga/effects'
import {
  LOAD_ADMIN_ALL_JOBS,
  LOAD_USER_ALL_JOBS,
  LOAD_ADMIN_SINGLE_JOB,
  LOAD_ADMIN_JOB_LOGS,
  LOAD_USER_JOB_LOGS,
  OPERATE_JOB,
  LOAD_JOB_NAME,
  LOAD_JOB_SOURCENS,
  LOAD_JOB_SINKNS,
  LOAD_JOB_SOURCETOSINK_EXIST,
  ADD_JOB,
  QUERY_JOB,
  EDIT_JOB,
  LOAD_JOB_DETAIL,
  LOAD_BACKFILL_TOPIC,
  GET_SOURCE_NS_VERSION
} from './constants'

import {
  adminAllJobsLoaded,
  userAllJobsLoaded,
  adminSingleJobLoaded,
  adminJobLogsLoaded,
  userJobLogsLoaded,
  jobOperated,
  jobOperatedError,
  jobNameLoaded,
  jobNameLoadedError,
  jobSourceNsLoaded,
  jobSourceNsLoadedError,
  jobSinkNsLoaded,
  jobSinkNsLoadedError,
  jobSourceToSinkExistLoaded,
  jobSourceToSinkExistErrorLoaded,
  jobAdded,
  jobQueryed,
  jobEdited,
  jobDetailLoaded,
  jobBackfillTopicLoaded,
  jobBackfillTopicError
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'
import { notifySagasError } from '../../utils/util'

export function* getAdminAllJobs ({ payload }) {
  try {
    const result = yield call(request, api.job)
    yield put(adminAllJobsLoaded(result.payload))
    payload.resolve()
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
    yield put(userAllJobsLoaded(result.payload))
    payload.resolve()
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
    yield put(adminSingleJobLoaded(result.payload))
    payload.resolve()
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
    yield put(adminJobLogsLoaded(result.payload))
    payload.resolve(result.payload)
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
    yield put(userJobLogsLoaded(result.payload))
    payload.resolve(result.payload)
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
    if (payload.values.action === 'delete') {
      if (result.code && result.code !== 200) {
        yield put(jobOperatedError(result.msg))
        payload.reject(result.msg)
      } else if (result.code && result.code === 200) {
        yield put(jobOperated(Number(payload.values.jobId), payload.resolve))
        payload.resolve(Number(payload.values.jobId))
      }
    } else {
      if (result.code && result.code !== 200) {
        yield put(jobOperatedError(result.msg))
        payload.reject(result.msg)
      } else if (result.header.code && result.header.code === 200) {
        yield put(jobOperated(result.payload, payload.resolve))
        payload.resolve(result.payload)
      }
    }
  } catch (err) {
    notifySagasError(err, 'operateUserJob')
  }
}

export function* operateUserJobWatcher () {
  yield fork(takeEvery, OPERATE_JOB, operateUserJob)
}

export function* loadJobNameValue ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/jobs?jobName=${payload.value}`)
    if (result.code && result.code === 409) {
      yield put(jobNameLoadedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(jobNameLoaded(result.payload))
      payload.resolve()
    }
  } catch (err) {
    notifySagasError(err, 'loadJobNameValue')
  }
}

export function* loadJobNameValueWatcher () {
  yield fork(takeEvery, LOAD_JOB_NAME, loadJobNameValue)
}

export function* loadJobSourceNsValue ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/namespaces?${payload.type}=${payload.value}`)
    if (result.code && result.code !== 200) {
      yield put(jobSourceNsLoadedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(jobSourceNsLoaded(result.payload))
      payload.resolve(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'loadJobSourceNsValue')
  }
}

export function* loadJobSourceNsValueWatcher () {
  yield fork(takeEvery, LOAD_JOB_SOURCENS, loadJobSourceNsValue)
}

export function* loadJobBackfillTopicValue ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/namespaces/${payload.namespaceId}/topic`)
    if (result.code && result.code !== 200) {
      yield put(jobBackfillTopicError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(jobBackfillTopicLoaded(result.payload))
      payload.resolve(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'loadJobBackfillTopicValue')
  }
}

export function* loadJobBackfillTopicValueWatcher () {
  yield fork(takeEvery, LOAD_BACKFILL_TOPIC, loadJobBackfillTopicValue)
}

export function* loadJobSinkNsValue ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/namespaces?${payload.type}=${payload.value}`)
    if (result.code && result.code !== 200) {
      yield put(jobSinkNsLoadedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(jobSinkNsLoaded(result.payload))
      payload.resolve(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'loadJobSinkNsValue')
  }
}

export function* loadJobSinkNsValueWatcher () {
  yield fork(takeEvery, LOAD_JOB_SINKNS, loadJobSinkNsValue)
}

export function* getJobSourceToSink ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/jobs?sourceNs=${payload.sourceNs}&sinkNs=${payload.sinkNs}`)
    if (result.code === 200) {
      yield put(jobSourceToSinkExistLoaded(result.msg))
      payload.resolve()
    } else {
      yield put(jobSourceToSinkExistErrorLoaded(result.msg))
      payload.reject()
    }
  } catch (err) {
    notifySagasError(err, 'getJobSourceToSink')
  }
}

export function* getJobSourceToSinkWatcher () {
  yield fork(throttle, 500, LOAD_JOB_SOURCETOSINK_EXIST, getJobSourceToSink)
}

export function* addJob ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: `${api.projectUserList}/${payload.projectId}/jobs`,
      data: payload.values
    })
    yield put(jobAdded(result.payload))
    payload.resolve()
    payload.final()
  } catch (err) {
    notifySagasError(err, 'addJob')
  }
}

export function* addJobWatcher () {
  yield fork(takeEvery, ADD_JOB, addJob)
}

export function* queryJob ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.values.projectId}/jobs/${payload.values.jobId}`)
    yield put(jobQueryed(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'queryJob')
  }
}

export function* queryJobWatcher () {
  yield fork(takeEvery, QUERY_JOB, queryJob)
}

export function* editJob ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectUserList}/${payload.values.projectId}/jobs`,
      data: payload.values
    })
    yield put(jobEdited(result.payload))
    payload.resolve()
    payload.final()
  } catch (err) {
    notifySagasError(err, 'editJob')
  }
}

export function* editJobWatcher () {
  yield fork(takeEvery, EDIT_JOB, editJob)
}

export function* queryJobDetail ({ payload }) {
  const apiFinal = payload.value.roleType === 'admin'
    ? `${api.job}/${payload.value.jobId}`
    : `${api.projectUserList}/${payload.value.projectId}/jobs/${payload.value.jobId}`

  try {
    const result = yield call(request, `${apiFinal}`)
    yield put(jobDetailLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'queryJobDetail')
  }
}

export function* queryJobDetailWatcher () {
  yield fork(takeEvery, LOAD_JOB_DETAIL, queryJobDetail)
}

export function* getSourceNsVersion ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/jobs/dataversions?namespace=${payload.namespace}`)
    payload.resolve(result.payload)
  } catch (err) {
    payload.resolve('')
    notifySagasError(err, 'getSourceNsVersion')
  }
}
export function* getSourceNsVersionWatcher () {
  yield fork(takeLatest, GET_SOURCE_NS_VERSION, getSourceNsVersion)
}

export default [
  getAdminAllJobsWatcher,
  getUserAllJobsWatcher,
  getAdminSingleJobWatcher,
  getAdminJobLogsWatcher,
  getUserJobLogsWatcher,
  operateUserJobWatcher,
  loadJobNameValueWatcher,
  loadJobSourceNsValueWatcher,
  loadJobSinkNsValueWatcher,
  getJobSourceToSinkWatcher,
  addJobWatcher,
  queryJobWatcher,
  editJobWatcher,
  queryJobDetailWatcher,
  loadJobBackfillTopicValueWatcher,
  getSourceNsVersionWatcher
]
