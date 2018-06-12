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
  LOAD_INSTANCES,
  LOAD_INSTANCES_SUCCESS,
  ADD_INSTANCE,
  ADD_INSTANCE_SUCCESS,
  ADD_INSTANCE_ERROR,
  LOAD_SINGLE_INSTANCE,
  LOAD_SINGLE_INSTANCE_SUCCESS,
  EDIT_INSTANCE,
  EDIT_INSTANCE_SUCCESS,
  EDIT_INSTANCE_ERROR,
  DELETE_INSTANCE,
  DELETE_INSTANCE_SUCCESS,
  DELETE_INSTANCE_ERROR,
  GET_ERROR,
  CHECK_INSTANCE,
  CHECK_URL
} from './constants'

export function loadInstances (resolve) {
  return {
    type: LOAD_INSTANCES,
    payload: {
      resolve
    }
  }
}

export function instancesLoaded (instances) {
  return {
    type: LOAD_INSTANCES_SUCCESS,
    payload: {
      instances
    }
  }
}

export function addInstance (instance, resolve, reject) {
  return {
    type: ADD_INSTANCE,
    payload: {
      instance,
      resolve,
      reject
    }
  }
}

export function instanceAdded (result) {
  return {
    type: ADD_INSTANCE_SUCCESS,
    payload: {
      result
    }
  }
}

export function instanceAddedError (result) {
  return {
    type: ADD_INSTANCE_ERROR,
    payload: {
      result
    }
  }
}

export function loadSingleInstance (instanceId, resolve) {
  return {
    type: LOAD_SINGLE_INSTANCE,
    payload: {
      instanceId,
      resolve
    }
  }
}

export function singleInstanceLoaded (result) {
  return {
    type: LOAD_SINGLE_INSTANCE_SUCCESS,
    payload: {
      result
    }
  }
}

export function editInstance (value, resolve, reject) {
  return {
    type: EDIT_INSTANCE,
    payload: {
      value,
      resolve,
      reject
    }
  }
}

export function instanceEdited (result) {
  return {
    type: EDIT_INSTANCE_SUCCESS,
    payload: {
      result
    }
  }
}

export function instanceEditedError (result) {
  return {
    type: EDIT_INSTANCE_ERROR,
    payload: {
      result
    }
  }
}

export function deleteInstace (instanceId, resolve, reject) {
  return {
    type: DELETE_INSTANCE,
    payload: {
      instanceId,
      resolve,
      reject
    }
  }
}

export function instanceDeleted (result) {
  return {
    type: DELETE_INSTANCE_SUCCESS,
    payload: {
      result
    }
  }
}

export function instanceDeletedError (result) {
  return {
    type: DELETE_INSTANCE_ERROR,
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

export function checkInstance (type, nsInstance, resolve, reject) {
  return {
    type: CHECK_INSTANCE,
    payload: {
      type,
      nsInstance,
      resolve,
      reject
    }
  }
}

export function checkConnectionUrl (type, url, resolve, reject) {
  return {
    type: CHECK_URL,
    payload: {
      type,
      url,
      resolve,
      reject
    }
  }
}

export function checkUrl (value, resolve, reject) {
  return {
    type: CHECK_URL,
    payload: {
      value,
      resolve,
      reject
    }
  }
}

