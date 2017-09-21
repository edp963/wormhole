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
  LOAD_ADMIN_ALL_NAMESPACES,
  LOAD_ADMIN_ALL_NAMESPACES_SUCCESS,
  LOAD_USER_NAMESPACES,
  LOAD_USER_NAMESPACES_SUCCESS,
  LOAD_SELECT_NAMESPACES,
  LOAD_SELECT_NAMESPACES_SUCCESS,
  LOAD_NAMESPACE_DATABASE,
  LOAD_NAMESPACE_DATABASE_SUCCESS,
  LOAD_TABLE_NAME_EXIST,
  LOAD_TABLE_NAME_EXIST_SUCCESS,
  LOAD_TABLE_NAME_EXIST_ERROR,
  ADD_NAMESPACE,
  ADD_NAMESPACE_SUCCESS,
  LOAD_SINGLE_NAMESPACE,
  LOAD_SINGLE_NAMESPACE_SUCCESS,
  EDIT_NAMESPACE,
  EDIT_NAMESPACE_SUCCESS,

  LOAD_PROJECT_NS_ALL,
  LOAD_PROJECT_NS_ALL_SUCCESS,
  GET_ERROR
} from './constants'

const initialState = fromJS({
  namespaces: false,
  error: false,
  modalLoading: false,
  tableNameExited: false
})

export function namespaceReducer (state = initialState, { type, payload }) {
  const namespaces = state.get('namespaces')
  switch (type) {
    case LOAD_ADMIN_ALL_NAMESPACES:
      return state.set('error', false)
    case LOAD_ADMIN_ALL_NAMESPACES_SUCCESS:
      payload.resolve()
      return state.set('namespaces', payload.namespaces)
    case LOAD_USER_NAMESPACES:
      return state.set('error', false)
    case LOAD_USER_NAMESPACES_SUCCESS:
      payload.resolve()
      return state.set('namespaces', payload.namespaces)
    case LOAD_SELECT_NAMESPACES:
      return state
    case LOAD_SELECT_NAMESPACES_SUCCESS:
      payload.resolve(payload.namespaces)
      return state.set('namespaces', payload.namespaces)
    case LOAD_NAMESPACE_DATABASE:
      return state
    case LOAD_NAMESPACE_DATABASE_SUCCESS:
      payload.resolve(payload.database)
      return state
    case LOAD_TABLE_NAME_EXIST:
      return state.set('tableNameExited', false)
    case LOAD_TABLE_NAME_EXIST_SUCCESS:
      payload.resolve()
      return state.set('tableNameExited', false)
    case LOAD_TABLE_NAME_EXIST_ERROR:
      payload.reject()
      return state.set('tableNameExited', true)
    case ADD_NAMESPACE:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case ADD_NAMESPACE_SUCCESS:
      payload.resolve()
      for (let i = 0; i < payload.result.length; i++) {
        namespaces.unshift(payload.result[i])
      }
      return state
        .set('namespaces', namespaces.slice())
        .set('modalLoading', false)
    case LOAD_SINGLE_NAMESPACE:
      return state
    case LOAD_SINGLE_NAMESPACE_SUCCESS:
      payload.resolve(payload.result)
      return state
    case EDIT_NAMESPACE:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case EDIT_NAMESPACE_SUCCESS:
      payload.resolve()
      namespaces.splice(namespaces.indexOf(namespaces.find(p => p.id === payload.result.id)), 1, payload.result)
      return state
        .set('namespaces', namespaces.slice())
        .set('modalLoading', false)

    case LOAD_PROJECT_NS_ALL:
      return state.set('error', false)
    case LOAD_PROJECT_NS_ALL_SUCCESS:
      payload.resolve(payload.result)
      return state.set('namespaces', payload.result)
    case GET_ERROR:
      return state.set('error', payload.error)
    default:
      return state
  }
}

export default namespaceReducer
