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
  LOAD_SOURCE_NAMESPACES,
  LOAD_SOURCE_NAMESPACES_SUCCESS,
  GET_ERROR,
  CHANGE_TABS,
  LOAD_UDFS,
  LOAD_SINGLE_UDF,
  LOAD_LASTEST_OFFSET,
  LOAD_LASTEST_OFFSET_SUCCESS,
  START_DEBUG,
  START_DEBUG_SUCCESS,
  START_DEBUG_ERROR,
  STOP_DEBUG
} from './constants'

export function loadSourceNamaspaces () {
  return {
    type: LOAD_SOURCE_NAMESPACES
  }
}

export function sourceNamespacesLoaded (sourceNamespaces) {
  return {
    type: LOAD_SOURCE_NAMESPACES_SUCCESS,
    payload: {
      sourceNamespaces
    }
  }
}

export function getError (error) {
  return {
    type: GET_ERROR,
    payload: {
      error
    }
  }
}
export function changeTabs (key) {
  return {
    type: CHANGE_TABS,
    payload: {
      key
    }
  }
}

export function loadUdfs (projectId, type, id, roleType, resolve) {
  return {
    type: LOAD_UDFS,
    payload: {
      projectId,
      type,
      id,
      roleType,
      resolve
    }
  }
}

export function loadSingleUdf (projectId, roleType, resolve, type) {
  return {
    type: LOAD_SINGLE_UDF,
    payload: {
      projectId,
      roleType,
      resolve,
      type
    }
  }
}

export function loadLastestOffset (projectId, type, id, sourceNs, resolve, method = 'get', topics = []) {
  return {
    type: LOAD_LASTEST_OFFSET,
    payload: {
      projectId,
      type,
      id,
      sourceNs,
      method,
      topics,
      resolve
    }
  }
}

export function lastestOffsetLoaded (result) {
  return {
    type: LOAD_LASTEST_OFFSET_SUCCESS,
    payload: {
      result
    }
  }
}

export function startDebug (projectId, id, topicResult, action, resolve, reject) {
  return {
    type: START_DEBUG,
    payload: {
      projectId,
      id,
      topicResult,
      action,
      resolve,
      reject
    }
  }
}

export function startDebugSuccess (result) {
  return {
    type: START_DEBUG_SUCCESS,
    payload: {
      result
    }
  }
}

export function startDebugError (message) {
  return {
    type: START_DEBUG_ERROR,
    payload: {
      message
    }
  }
}

export function stopDebug (projectId, id, logPath, resolve) {
  return {
    type: STOP_DEBUG,
    payload: {
      projectId,
      id,
      logPath,
      resolve
    }
  }
}

