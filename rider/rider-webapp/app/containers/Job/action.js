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
  EDIT_JOB,
  EDIT_JOB_SUCCESS,
  QUERY_JOB,
  QUERY_JOB_SUCCESS,
  LOAD_JOB_DETAIL,
  LOAD_JOB_DETAIL_SUCCESS,
  LOAD_BACKFILL_TOPIC,
  LOAD_BACKFILL_TOPIC_SUCCUSS,
  LOAD_BACKFILL_TOPIC_ERROR,
  GET_SOURCE_NS_VERSION
} from './constants'

export function loadAdminAllJobs (resolve) {
  return {
    type: LOAD_ADMIN_ALL_JOBS,
    payload: {
      resolve
    }
  }
}

export function adminAllJobsLoaded (jobs) {
  return {
    type: LOAD_ADMIN_ALL_JOBS_SUCCESS,
    payload: {
      jobs
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

export function userAllJobsLoaded (jobs) {
  return {
    type: LOAD_USER_ALL_JOBS_SUCCESS,
    payload: {
      jobs
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

export function adminSingleJobLoaded (job) {
  return {
    type: LOAD_ADMIN_SINGLE_JOB_SUCCESS,
    payload: {
      job
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

export function adminJobLogsLoaded (result) {
  return {
    type: LOAD_ADMIN_JOB_LOGS_SUCCESS,
    payload: {
      result
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

export function userJobLogsLoaded (result) {
  return {
    type: LOAD_USER_JOB_LOGS_SUCCESS,
    payload: {
      result
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

export function jobOperated (result) {
  return {
    type: OPERATE_JOB_SUCCESS,
    payload: {
      result
    }
  }
}

export function jobOperatedError (message) {
  return {
    type: OPERATE_JOB_ERROR,
    payload: {
      message
    }
  }
}

export function loadJobName (projectId, value, resolve, reject) {
  return {
    type: LOAD_JOB_NAME,
    payload: {
      projectId,
      value,
      resolve,
      reject
    }
  }
}

export function jobNameLoaded (result) {
  return {
    type: LOAD_JOB_NAME_SUCCESS,
    payload: {
      result
    }
  }
}

export function jobNameLoadedError (message) {
  return {
    type: LOAD_JOB_NAME_ERROR,
    payload: {
      message
    }
  }
}

export function loadJobSourceNs (projectId, value, type, resolve, reject) {
  return {
    type: LOAD_JOB_SOURCENS,
    payload: {
      projectId,
      value,
      type,
      resolve,
      reject
    }
  }
}

export function jobSourceNsLoaded (result) {
  return {
    type: LOAD_JOB_SOURCENS_SUCCESS,
    payload: {
      result
    }
  }
}

export function jobSourceNsLoadedError (result) {
  return {
    type: LOAD_JOB_SOURCENS_ERROR,
    payload: {
      result
    }
  }
}

export function loadJobSinkNs (projectId, value, type, resolve, reject) {
  return {
    type: LOAD_JOB_SINKNS,
    payload: {
      projectId,
      value,
      type,
      resolve,
      reject
    }
  }
}

export function jobSinkNsLoaded (result) {
  return {
    type: LOAD_JOB_SINKNS_SUCCESS,
    payload: {
      result
    }
  }
}

export function jobSinkNsLoadedError (result) {
  return {
    type: LOAD_JOB_SINKNS_ERROR,
    payload: {
      result
    }
  }
}

export function loadJobSourceToSinkExist (projectId, sourceNs, sinkNs, resolve, reject) {
  return {
    type: LOAD_JOB_SOURCETOSINK_EXIST,
    payload: {
      projectId,
      sourceNs,
      sinkNs,
      resolve,
      reject
    }
  }
}

export function jobSourceToSinkExistLoaded (result) {
  return {
    type: LOAD_JOB_SOURCETOSINK_EXIST_SUCCESS,
    payload: {
      result
    }
  }
}

export function jobSourceToSinkExistErrorLoaded (result) {
  return {
    type: LOAD_JOB_SOURCETOSINK_EXIST_ERROR,
    payload: {
      result
    }
  }
}

export function addJob (projectId, values, resolve, final) {
  return {
    type: ADD_JOB,
    payload: {
      projectId,
      values,
      resolve,
      final
    }
  }
}

export function jobAdded (result) {
  return {
    type: ADD_JOB_SUCCESS,
    payload: {
      result
    }
  }
}
export function queryJob (values, resolve) {
  return {
    type: QUERY_JOB,
    payload: {
      values,
      resolve
    }
  }
}

export function jobQueryed (result) {
  return {
    type: QUERY_JOB_SUCCESS,
    payload: {
      result
    }
  }
}

export function editJob (values, resolve, final) {
  return {
    type: EDIT_JOB,
    payload: {
      values,
      resolve,
      final
    }
  }
}

export function jobEdited (result) {
  return {
    type: EDIT_JOB_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadJobDetail (value, resolve) {
  return {
    type: LOAD_JOB_DETAIL,
    payload: {
      value,
      resolve
    }
  }
}

export function jobDetailLoaded (result) {
  return {
    type: LOAD_JOB_DETAIL_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadJobBackfillTopic (projectId, namespaceId, resolve) {
  return {
    type: LOAD_BACKFILL_TOPIC,
    payload: {
      projectId,
      namespaceId,
      resolve
    }
  }
}

export function jobBackfillTopicLoaded (result) {
  return {
    type: LOAD_BACKFILL_TOPIC_SUCCUSS,
    payload: {
      result
    }
  }
}
export function jobBackfillTopicError (result) {
  return {
    type: LOAD_BACKFILL_TOPIC_ERROR,
    payload: {
      result
    }
  }
}

export function getSourceNsVersion (projectId, namespace, resolve) {
  return {
    type: GET_SOURCE_NS_VERSION,
    payload: {
      projectId,
      namespace,
      resolve
    }
  }
}
