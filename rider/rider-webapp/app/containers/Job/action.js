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

export function loadAdminAllJobs (resolve) {
  return {
    type: LOAD_ADMIN_ALL_JOBS,
    payload: {
      resolve
    }
  }
}

export function adminAllJobsLoaded (jobs, resolve) {
  return {
    type: LOAD_ADMIN_ALL_JOBS_SUCCESS,
    payload: {
      jobs,
      resolve
    }
  }
}

export function loadUserAllJobs (projectId, resolve) {
  return {
    type: LOAD_USER_ALL_JOBS,
    payload: {
      projectId,
      resolve
    }
  }
}

export function userAllJobsLoaded (jobs, resolve) {
  return {
    type: LOAD_USER_ALL_JOBS_SUCCESS,
    payload: {
      jobs,
      resolve
    }
  }
}

export function loadAdminSingleJob (projectId, resolve) {
  return {
    type: LOAD_ADMIN_SINGLE_JOB,
    payload: {
      projectId,
      resolve
    }
  }
}

export function adminSingleJobLoaded (job, resolve) {
  return {
    type: LOAD_ADMIN_SINGLE_JOB_SUCCESS,
    payload: {
      job,
      resolve
    }
  }
}

export function loadAdminJobLogs (projectId, jobId, resolve) {
  return {
    type: LOAD_ADMIN_JOB_LOGS,
    payload: {
      projectId,
      jobId,
      resolve
    }
  }
}

export function adminJobLogsLoaded (result, resolve) {
  return {
    type: LOAD_ADMIN_JOB_LOGS_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function loadUserJobLogs (projectId, jobId, resolve) {
  return {
    type: LOAD_USER_JOB_LOGS,
    payload: {
      projectId,
      jobId,
      resolve
    }
  }
}

export function userJobLogsLoaded (result, resolve) {
  return {
    type: LOAD_USER_JOB_LOGS_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function operateJob (values, resolve, reject) {
  return {
    type: OPERATE_JOB,
    payload: {
      values,
      resolve,
      reject
    }
  }
}

export function jobOperated (result, resolve) {
  return {
    type: OPERATE_JOB_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function jobOperatedError (message, reject) {
  return {
    type: OPERATE_JOB_ERROR,
    payload: {
      message,
      reject
    }
  }
}

export function chuckAwayJob () {
  return {
    type: CHUCKAWAY_JOB
  }
}

