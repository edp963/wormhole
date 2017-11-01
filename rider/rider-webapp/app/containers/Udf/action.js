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

export function udfsLoaded (udfs, resolve) {
  return {
    type: LOAD_UDFS_SUCCESS,
    payload: {
      udfs,
      resolve
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

export function projectUdfsLoaded (udfs, resolve) {
  return {
    type: LOAD_PROJECT_UDFS_SUCCESS,
    payload: {
      udfs,
      resolve
    }
  }
}

export function loadSingleUdf (projectId, roleType, resolve) {
  return {
    type: LOAD_SINGLE_UDF,
    payload: {
      projectId,
      roleType,
      resolve
    }
  }
}

export function singleUdfLoaded (udf, resolve) {
  return {
    type: LOAD_SINGLE_UDF_SUCCESS,
    payload: {
      udf,
      resolve
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

export function udfAdded (udf, resolve) {
  return {
    type: ADD_UDF_SUCCESS,
    payload: {
      udf,
      resolve
    }
  }
}

export function udfAddedError (result, reject) {
  return {
    type: ADD_UDF_ERROR,
    payload: {
      result,
      reject
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

export function udfEdited (udf, resolve) {
  return {
    type: EDIT_UDF_SUCCESS,
    payload: {
      udf,
      resolve
    }
  }
}

export function udfEditedError (result, reject) {
  return {
    type: EDIT_UDF_ERROR,
    payload: {
      result,
      reject
    }
  }
}

export function deleteUdf (values, resolve) {
  return {
    type: DELETE_UDF,
    payload: {
      values,
      resolve
    }
  }
}

export function udfDeleted (result, resolve) {
  return {
    type: DELETE_UDF_SUCCESS,
    payload: {
      result,
      resolve
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

