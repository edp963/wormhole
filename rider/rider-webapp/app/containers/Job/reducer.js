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

import {
  LOAD_ADMIN_ALL_JOBS,
  LOAD_ADMIN_ALL_JOBS_SUCCESS,
  LOAD_USER_ALL_JOBS,
  LOAD_USER_ALL_JOBS_SUCCESS,
  LOAD_ADMIN_SINGLE_JOB,
  LOAD_ADMIN_SINGLE_JOB_SUCCESS,
  LOAD_ADMIN_JOB_LOGS,
  LOAD_ADMIN_JOB_LOGS_SUCCESS,
  LOAD_USER_JOB_LOGS,
  LOAD_USER_JOB_LOGS_SUCCESS,
  OPERATE_JOB,
  OPERATE_JOB_SUCCESS,
  OPERATE_JOB_ERROR,
  LOAD_JOB_NAME,
  LOAD_JOB_NAME_SUCCESS,
  LOAD_JOB_NAME_ERROR,
  LOAD_JOB_SOURCENS,
  LOAD_JOB_SOURCENS_SUCCESS,
  LOAD_JOB_SOURCENS_ERROR,
  LOAD_JOB_SINKNS,
  LOAD_JOB_SINKNS_SUCCESS,
  LOAD_JOB_SINKNS_ERROR,
  LOAD_JOB_SOURCETOSINK_EXIST,
  LOAD_JOB_SOURCETOSINK_EXIST_SUCCESS,
  LOAD_JOB_SOURCETOSINK_EXIST_ERROR,
  ADD_JOB,
  ADD_JOB_SUCCESS,
  QUERY_JOB,
  QUERY_JOB_SUCCESS,
  EDIT_JOB,
  EDIT_JOB_SUCCESS,
  LOAD_JOB_DETAIL,
  LOAD_JOB_DETAIL_SUCCESS
} from './constants'
import { fromJS } from 'immutable'

const initialState = fromJS({
  jobs: false,
  error: false,
  flowSubmitLoading: false,
  jobNameExited: false,
  jobSourceToSinkExited: false
})

function jobReducer (state = initialState, { type, payload }) {
  const jobs = state.get('jobs')

  switch (type) {
    case LOAD_ADMIN_ALL_JOBS:
      return state.set('error', false)
    case LOAD_ADMIN_ALL_JOBS_SUCCESS:
      return state.set('jobs', payload.jobs)
    case LOAD_USER_ALL_JOBS:
      return state.set('error', false)
    case LOAD_USER_ALL_JOBS_SUCCESS:
      return state.set('jobs', payload.jobs)
    case LOAD_ADMIN_SINGLE_JOB:
      return state.set('error', false)
    case LOAD_ADMIN_SINGLE_JOB_SUCCESS:
      return state.set('jobs', payload.job)
    case LOAD_ADMIN_JOB_LOGS:
      return state
    case LOAD_ADMIN_JOB_LOGS_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_USER_JOB_LOGS:
      return state
    case LOAD_USER_JOB_LOGS_SUCCESS:
      payload.resolve(payload.result)
      return state
    case OPERATE_JOB:
      return state.set('error', false)
    case OPERATE_JOB_SUCCESS:
      if (typeof (payload.result) === 'number') {
        payload.resolve()
        return state.set('jobs', jobs.filter(g => g.job.id !== payload.result))
      } else {
        payload.resolve(payload.result)
        jobs.splice(jobs.indexOf(jobs.find(g => g.job.id === payload.result.job.id)), 1, payload.result)
        return state.set('jobs', jobs.slice())
      }
    case OPERATE_JOB_ERROR:
      payload.reject(payload.message)
      return state
    case LOAD_JOB_NAME:
      return state.set('jobNameExited', false)
    case LOAD_JOB_NAME_SUCCESS:
      payload.resolve()
      return state.set('jobNameExited', false)
    case LOAD_JOB_NAME_ERROR:
      payload.reject()
      return state.set('jobNameExited', true)
    case LOAD_JOB_SOURCENS:
      return state
    case LOAD_JOB_SOURCENS_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_JOB_SOURCENS_ERROR:
      payload.reject(payload.result)
      return state
    case LOAD_JOB_SINKNS:
      return state
    case LOAD_JOB_SINKNS_SUCCESS:
      payload.resolve(payload.result)
      return state
    case LOAD_JOB_SINKNS_ERROR:
      payload.reject(payload.result)
      return state
    case LOAD_JOB_SOURCETOSINK_EXIST:
      return state.set('jobSourceToSinkExited', false)
    case LOAD_JOB_SOURCETOSINK_EXIST_SUCCESS:
      payload.resolve()
      return state.set('jobSourceToSinkExited', false)
    case LOAD_JOB_SOURCETOSINK_EXIST_ERROR:
      payload.reject()
      return state.set('jobSourceToSinkExited', true)
    case ADD_JOB:
      return state.set('jobSubmitLoading', true)
    case ADD_JOB_SUCCESS:
      payload.resolve()
      jobs.unshift(payload.result)
      payload.final()
      return state
        .set('jobs', jobs.slice())
        .set('jobSubmitLoading', false)
    case QUERY_JOB:
      return state.set('error', false)
    case QUERY_JOB_SUCCESS:
      payload.resolve(payload.result)
      return state
    case EDIT_JOB:
      return state.set('jobSubmitLoading', true)
    case EDIT_JOB_SUCCESS:
      payload.resolve()
      jobs.splice(jobs.indexOf(jobs.find(p => p.job.id === payload.result.job.id)), 1, payload.result)
      payload.final()
      return state
        .set('jobs', jobs.slice())
        .set('jobSubmitLoading', false)
    case LOAD_JOB_DETAIL:
      return state.set('error', false)
    case LOAD_JOB_DETAIL_SUCCESS:
      payload.resolve(payload.result)
      return state
    default:
      return state
  }
}

export default jobReducer
