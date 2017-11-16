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
  CHUCKAWAY_JOB
} from './constants'
import { fromJS } from 'immutable'

const initialState = fromJS({
  jobs: false,
  error: false,
  flowSubmitLoading: false
})

function jobReducer (state = initialState, { type, payload }) {
  const jobs = state.get('jobs')

  switch (type) {
    case LOAD_ADMIN_ALL_JOBS:
      return state.set('error', false)
    case LOAD_ADMIN_ALL_JOBS_SUCCESS:
      payload.resolve()
      return state.set('jobs', payload.jobs)
    case LOAD_USER_ALL_JOBS:
      return state.set('error', false)
    case LOAD_USER_ALL_JOBS_SUCCESS:
      payload.resolve()
      return state.set('jobs', payload.jobs)
    case LOAD_ADMIN_SINGLE_JOB:
      return state.set('error', false)
    case LOAD_ADMIN_SINGLE_JOB_SUCCESS:
      payload.resolve()
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
      console.log('reqqq', payload.result)
      // if (typeof (payload.result) === 'string') {
      //   payload.resolve(payload.result)
      //   return state.set('jobs', jobs.filter(g => payload.result.split(',').indexOf(`${g.id}`) < 0))
      // } else {
      //   if (payload.result.length === undefined) {
      //     jobs.splice(jobs.indexOf(jobs.find(g => g.id === payload.result.id)), 1, payload.result)
      //   } else {
      //     for (let i = 0; i < payload.result.length; i++) {
      //       jobs.splice(jobs.indexOf(jobs.find(g => g.id === payload.result[i].id)), 1, payload.result[i])
      //     }
      //   }
      //   payload.resolve(payload.result)
      //   return state.set('jobs', jobs.slice())
      // }
      payload.resolve(payload.result)
      jobs.splice(jobs.indexOf(jobs.find(g => g.id === payload.result.job.id)), 1, payload.result)
      return state.set('jobs', jobs.slice())
    case OPERATE_JOB_ERROR:
      payload.reject(payload.message)
      return state
    case CHUCKAWAY_JOB:
      return state.set('jobs', false)

    default:
      return state
  }
}

export default jobReducer
