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
  LOAD_UDFS,
  LOAD_UDFS_SUCCESS,
  LOAD_SINGLE_UDF,
  LOAD_SINGLE_UDF_SUCCESS,
  LOAD_PROJECT_UDFS,
  LOAD_PROJECT_UDFS_SUCCESS,
  ADD_UDF,
  ADD_UDF_SUCCESS,
  ADD_UDF_ERROR,
  EDIT_UDF,
  EDIT_UDF_SUCCESS,
  EDIT_UDF_ERROR,
  DELETE_UDF,
  DELETE_UDF_SUCCESS,
  DELETE_UDF_ERROR,
  LOAD_UDF_DETAIL,
  LOAD_UDF_DETAIL_SUCCESS,
  GET_ERROR
} from './constants'

export function loadUdfs (resolve) {
  return {
    type: LOAD_UDFS,
    payload: {
      resolve
    }
  }
}

export function udfsLoaded (udfs) {
  return {
    type: LOAD_UDFS_SUCCESS,
    payload: {
      udfs
    }
  }
}

export function loadProjectUdfs (resolve) {
  return {
    type: LOAD_PROJECT_UDFS,
    payload: {
      resolve
    }
  }
}

export function projectUdfsLoaded (udfs) {
  return {
    type: LOAD_PROJECT_UDFS_SUCCESS,
    payload: {
      udfs
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

export function singleUdfLoaded (udf) {
  return {
    type: LOAD_SINGLE_UDF_SUCCESS,
    payload: {
      udf
    }
  }
}

export function addUdf (values, resolve, reject) {
  return {
    type: ADD_UDF,
    payload: {
      values,
      resolve,
      reject
    }
  }
}

export function udfAdded (udf) {
  return {
    type: ADD_UDF_SUCCESS,
    payload: {
      udf
    }
  }
}

export function udfAddedError (result) {
  return {
    type: ADD_UDF_ERROR,
    payload: {
      result
    }
  }
}

export function loadUdfDetail (udfId, resolve) {
  return {
    type: LOAD_UDF_DETAIL,
    payload: {
      udfId,
      resolve
    }
  }
}

export function udfDetailLoaded (result) {
  return {
    type: LOAD_UDF_DETAIL_SUCCESS,
    payload: {
      result
    }
  }
}

export function editUdf (values, resolve, reject) {
  return {
    type: EDIT_UDF,
    payload: {
      values,
      resolve,
      reject
    }
  }
}

export function udfEdited (udf) {
  return {
    type: EDIT_UDF_SUCCESS,
    payload: {
      udf
    }
  }
}

export function udfEditedError (result) {
  return {
    type: EDIT_UDF_ERROR,
    payload: {
      result
    }
  }
}

export function deleteUdf (udfId, resolve, reject) {
  return {
    type: DELETE_UDF,
    payload: {
      udfId,
      resolve,
      reject
    }
  }
}

export function udfDeleted (result) {
  return {
    type: DELETE_UDF_SUCCESS,
    payload: {
      result
    }
  }
}

export function udfDeletedError (result) {
  return {
    type: DELETE_UDF_ERROR,
    payload: {
      result
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

