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
  LOAD_DATABASES,
  LOAD_DATABASES_SUCCESS,
  ADD_DATABASE,
  ADD_DATABASE_SUCCESS,
  ADD_DATABASE_ERROR,
  EDIT_DATABASE,
  EDIT_DATABASE_SUCCESS,
  EDIT_DATABASE_ERROR,
  LOAD_DATABASES_INSTANCE,
  LOAD_DATABASES_INSTANCE_SUCCESS,
  LOAD_SINGLE_DATABASE,
  LOAD_SINGLE_DATABASE_SUCCESS,
  DELETE_DB,
  DELETE_DB_SUCCESS,
  DELETE_DB_ERROR,
  GET_ERROR,
  CHECK_DATABASE
} from './constants'

export function loadDatabases (resolve) {
  return {
    type: LOAD_DATABASES,
    payload: {
      resolve
    }
  }
}

export function databasesLoaded (databases) {
  return {
    type: LOAD_DATABASES_SUCCESS,
    payload: {
      databases
    }
  }
}

export function addDatabase (database, resolve, reject) {
  return {
    type: ADD_DATABASE,
    payload: {
      database,
      resolve,
      reject
    }
  }
}

export function databaseAdded (result) {
  return {
    type: ADD_DATABASE_SUCCESS,
    payload: {
      result
    }
  }
}

export function databaseAddedError (result) {
  return {
    type: ADD_DATABASE_ERROR,
    payload: {
      result
    }
  }
}

export function editDatabase (database, resolve, reject) {
  return {
    type: EDIT_DATABASE,
    payload: {
      database,
      resolve,
      reject
    }
  }
}

export function databaseEdited (result) {
  return {
    type: EDIT_DATABASE_SUCCESS,
    payload: {
      result
    }
  }
}

export function databaseEditedError (result) {
  return {
    type: EDIT_DATABASE_ERROR,
    payload: {
      result
    }
  }
}

export function loadDatabasesInstance (value, resolve) {
  return {
    type: LOAD_DATABASES_INSTANCE,
    payload: {
      value,
      resolve
    }
  }
}

export function databasesInstanceLoaded (result) {
  return {
    type: LOAD_DATABASES_INSTANCE_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadSingleDatabase (databaseId, resolve) {
  return {
    type: LOAD_SINGLE_DATABASE,
    payload: {
      databaseId,
      resolve
    }
  }
}

export function singleDatabaseLoaded (result) {
  return {
    type: LOAD_SINGLE_DATABASE_SUCCESS,
    payload: {
      result
    }
  }
}

export function deleteDB (databaseId, resolve, reject) {
  return {
    type: DELETE_DB,
    payload: {
      databaseId,
      resolve,
      reject
    }
  }
}

export function dBDeleted (result) {
  return {
    type: DELETE_DB_SUCCESS,
    payload: {
      result
    }
  }
}

export function dBDeletedError (result) {
  return {
    type: DELETE_DB_ERROR,
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

export function checkDatabaseName (id, name, resolve, reject) {
  return {
    type: CHECK_DATABASE,
    payload: {
      id,
      name,
      resolve,
      reject
    }
  }
}

