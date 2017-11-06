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

import {fromJS} from 'immutable'

import {
  LOAD_DATABASES,
  LOAD_DATABASES_SUCCESS,
  ADD_DATABASE,
  ADD_DATABASE_SUCCESS,
  ADD_DATABASE_ERROR,
  LOAD_SINGLE_DATABASE,
  LOAD_SINGLE_DATABASE_SUCCESS,
  EDIT_DATABASE,
  EDIT_DATABASE_SUCCESS,
  EDIT_DATABASE_ERROR,
  LOAD_DATABASES_INSTANCE,
  LOAD_DATABASES_INSTANCE_SUCCESS,
  LOAD_NAME_EXIST,
  LOAD_NAME_EXIST_SUCCESS,
  LOAD_NAME_EXIST_ERROR,
  GET_ERROR
} from './constants'

const initialState = fromJS({
  databases: false,
  error: false,
  modalLoading: false,
  databaseNameExited: false,
  dbUrlValue: false
})

export function databaseReducer (state = initialState, { type, payload }) {
  const databases = state.get('databases')

  switch (type) {
    case LOAD_DATABASES:
      return state.set('error', false)
    case LOAD_DATABASES_SUCCESS:
      payload.resolve()
      return state.set('databases', payload.databases)
    case ADD_DATABASE:
      return state.set('modalLoading', true)
    case ADD_DATABASE_SUCCESS:
      payload.resolve()
      databases.unshift(payload.result)
      return state
        .set('databases', databases.slice())
        .set('modalLoading', false)
    case ADD_DATABASE_ERROR:
      payload.reject(payload.result)
      return state.set('modalLoading', false)
    case LOAD_SINGLE_DATABASE:
      return state.set('error', false)
    case LOAD_SINGLE_DATABASE_SUCCESS:
      payload.resolve(payload.result)
      return state
    case EDIT_DATABASE:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case EDIT_DATABASE_SUCCESS:
      payload.resolve()
      databases.splice(databases.indexOf(databases.find(p => p.id === payload.result.id)), 1, payload.result)
      return state
        .set('databases', databases.slice())
        .set('modalLoading', false)
    case EDIT_DATABASE_ERROR:
      payload.reject(payload.result)
      return state.set('modalLoading', false)
    case LOAD_DATABASES_INSTANCE:
      return state
    case LOAD_DATABASES_INSTANCE_SUCCESS:
      payload.resolve(payload.result)
      return state.set('dbUrlValue', payload.result)
    case LOAD_NAME_EXIST:
      return state.set('databaseNameExited', false)
    case LOAD_NAME_EXIST_SUCCESS:
      payload.resolve()
      return state.set('databaseNameExited', false)
    case LOAD_NAME_EXIST_ERROR:
      payload.reject()
      return state.set('databaseNameExited', true)
    case GET_ERROR:
      return state.set('error', payload.error)
    default:
      return state
  }
}

export default databaseReducer
