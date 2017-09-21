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
  LOAD_ADMIN_ALL_USERS,
  LOAD_ADMIN_ALL_USERS_SUCCESS,
  LOAD_USER_USERS,
  LOAD_USER_USERS_SUCCESS,
  LOAD_SELECT_USERS,
  LOAD_SELECT_USERS_SUCCESS,
  ADD_USER,
  ADD_USER_SUCCESS,
  EDIT_USER,
  EDIT_USER_SUCCESS,
  LOAD_EMAIL_INPUT_VALUE,
  LOAD_EMAIL_INPUT_VALUE_SUCCESS,
  LOAD_EMAIL_INPUT_VALUE_ERROR,
  EDIT_ROLETYPE_USERPSW,
  EDIT_ROLETYPE_USERPSW_SUCCESS,
  EDIT_ROLETYPE_USERPSW_ERROR,

  LOAD_PROJECT_USER_ALL,
  LOAD_PROJECT_USER_ALL_SUCCESS,
  GET_ERROR
} from './constants'

const initialState = fromJS({
  users: false,
  error: false,
  modalLoading: false,
  emailExited: false
})

export function userReducer (state = initialState, { type, payload }) {
  const users = state.get('users')
  switch (type) {
    case LOAD_ADMIN_ALL_USERS:
      return state.set('error', false)
    case LOAD_ADMIN_ALL_USERS_SUCCESS:
      payload.resolve()
      return state.set('users', payload.users)
    case LOAD_USER_USERS:
      return state.set('error', false)
    case LOAD_USER_USERS_SUCCESS:
      payload.resolve()
      return state.set('users', payload.users)
    case LOAD_SELECT_USERS:
      return state.set('error', false)
    case LOAD_SELECT_USERS_SUCCESS:
      payload.resolve(payload.users)
      return state.set('users', payload.users)
    case ADD_USER:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case ADD_USER_SUCCESS:
      payload.resolve()
      users.unshift(payload.result)
      return state
        .set('users', users.slice())
        .set('modalLoading', false)
    case EDIT_USER:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case EDIT_USER_SUCCESS:
      payload.resolve()
      users.splice(users.indexOf(users.find(p => p.id === payload.result.id)), 1, payload.result)
      return state
        .set('users', users.slice())
        .set('modalLoading', false)
    case LOAD_EMAIL_INPUT_VALUE:
      return state.set('emailExited', false)
    case LOAD_EMAIL_INPUT_VALUE_SUCCESS:
      payload.resolve()
      return state.set('emailExited', false)
    case LOAD_EMAIL_INPUT_VALUE_ERROR:
      payload.reject()
      return state.set('emailExited', true)
    case EDIT_ROLETYPE_USERPSW:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case EDIT_ROLETYPE_USERPSW_SUCCESS:
      payload.resolve()
      return state.set('modalLoading', false)
    case EDIT_ROLETYPE_USERPSW_ERROR:
      payload.reject(payload.result)
      return state.set('modalLoading', false)

    case LOAD_PROJECT_USER_ALL:
      return state.set('error', false)
    case LOAD_PROJECT_USER_ALL_SUCCESS:
      payload.resolve(payload.result)
      return state.set('users', payload.result)
    case GET_ERROR:
      return state.set('error', payload.error)
    default:
      return state
  }
}

export default userReducer
