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
  LOAD_NORMAL,
  LOAD_NORMAL_SUCCESS,
  LOAD_SELECT_USERS,
  LOAD_SELECT_USERS_SUCCESS,
  ADD_USER,
  ADD_USER_SUCCESS,
  EDIT_USER,
  EDIT_USER_SUCCESS,
  EDIT_NORMAL,
  EDIT_NORMAL_SUCCESS,
  LOAD_EMAIL_INPUT_VALUE,
  LOAD_EMAIL_INPUT_VALUE_SUCCESS,
  LOAD_EMAIL_INPUT_VALUE_ERROR,
  EDIT_ROLETYPE_USERPSW,
  EDIT_ROLETYPE_USERPSW_SUCCESS,
  EDIT_ROLETYPE_USERPSW_ERROR,
  LOAD_USER_DETAIL,
  LOAD_USER_DETAIL_SUCCESS,
  DELETE_USER,
  DELETE_USER_SUCCESS,
  DELETE_USER_ERROR,

  LOAD_PROJECT_USER_ALL,
  LOAD_PROJECT_USER_ALL_SUCCESS,
  GET_ERROR
} from './constants'
import { DEFAULT_LOCALE } from '../App/constants'

const initialState = fromJS({
  users: false,
  error: false,
  modalLoading: false,
  emailExited: false,
  locale: DEFAULT_LOCALE
})

export function userReducer (state = initialState, { type, payload }) {
  const users = state.get('users')
  switch (type) {
    case LOAD_ADMIN_ALL_USERS:
      return state.set('error', false)
    case LOAD_ADMIN_ALL_USERS_SUCCESS:
      return state.set('users', payload.users)
    case LOAD_USER_USERS:
      return state.set('error', false)
    case LOAD_USER_USERS_SUCCESS:
      return state.set('users', payload.users)
    case LOAD_SELECT_USERS:
      return state.set('error', false)
    case LOAD_NORMAL:
      return state.set('users', payload.users)
    case LOAD_NORMAL_SUCCESS:
      return state.set('error', false)
    case LOAD_SELECT_USERS_SUCCESS:
      return state.set('users', payload.users)
    case ADD_USER:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case ADD_USER_SUCCESS:
      users.unshift(payload.result)
      return state
        .set('users', users.slice())
        .set('modalLoading', false)
    case EDIT_USER:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case EDIT_USER_SUCCESS:
      const startIndex = users.indexOf(users.find(p => Object.is(p.id, payload.result.id)))
      users.fill(payload.result, startIndex, startIndex + 1)
      return state
        .set('users', users.slice())
        .set('modalLoading', false)
        .set('locale', payload.result.preferredLanguage)
    case EDIT_NORMAL:
      return state
    case EDIT_NORMAL_SUCCESS:
      return state
        .set('locale', payload.result.preferredLanguage)
    case LOAD_EMAIL_INPUT_VALUE:
      return state.set('emailExited', false)
    case LOAD_EMAIL_INPUT_VALUE_SUCCESS:
      return state.set('emailExited', false)
    case LOAD_EMAIL_INPUT_VALUE_ERROR:
      return state.set('emailExited', true)
    case EDIT_ROLETYPE_USERPSW:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case EDIT_ROLETYPE_USERPSW_SUCCESS:
      return state.set('modalLoading', false)
    case EDIT_ROLETYPE_USERPSW_ERROR:
      return state.set('modalLoading', false)
    case LOAD_USER_DETAIL:
      return state
    case LOAD_USER_DETAIL_SUCCESS:
      return state

    case LOAD_PROJECT_USER_ALL:
      return state.set('error', false)
    case LOAD_PROJECT_USER_ALL_SUCCESS:
      return state.set('users', payload.result)
    case DELETE_USER:
      return state
    case DELETE_USER_SUCCESS:
      return state.set('users', users.filter(g => !Object.is(g.id, payload.result)))
    case DELETE_USER_ERROR:
      return state
    case GET_ERROR:
      return state.set('error', payload.error)
    default:
      return state
  }
}

export default userReducer
