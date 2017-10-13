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
  LOAD_PROJECTS,
  LOAD_PROJECTS_SUCCESS,
  LOAD_USER_PROJECTS,
  LOAD_USER_PROJECTS_SUCCESS,
  LOAD_SINGLE_PROJECT,
  LOAD_SINGLE_PROJECT_SUCCESS,
  ADD_PROJECT,
  ADD_PROJECT_SUCCESS,
  EDIT_PROJECT,
  EDIT_PROJECT_SUCCESS,
  LOAD_PROJECT_NAME_VALUE,
  LOAD_PROJECT_NAME_VALUE_SUCCESS,
  LOAD_PROJECT_NAME_VALUE_ERROR,
  DELETE_SINGLE_PROJECT,
  DELETE_SINGLE_PROJECT_SUCCESS,
  DELETE_SINGLE_PROJECT_ERROR,
  GET_ERROR
} from './constants'

const initialState = fromJS({
  projects: false,
  error: false,
  modalLoading: false,
  projectNameExited: false
})

export function projectReducer (state = initialState, { type, payload }) {
  const projects = state.get('projects')
  switch (type) {
    case LOAD_PROJECTS:
      return state
    case LOAD_PROJECTS_SUCCESS:
      return state.set('projects', payload.projects)
    case LOAD_USER_PROJECTS:
      return state
    case LOAD_USER_PROJECTS_SUCCESS:
      return state.set('projects', payload.projects)
    case LOAD_SINGLE_PROJECT:
      return state
    case LOAD_SINGLE_PROJECT_SUCCESS:
      payload.resolve(payload.result)
      return state
    case ADD_PROJECT:
      return state.set('modalLoading', true)
    case ADD_PROJECT_SUCCESS:
      payload.resolve()
      payload.final()
      projects.unshift(payload.result)
      return state
        .set('projects', projects.slice())
        .set('modalLoading', false)
    case EDIT_PROJECT:
      return state.set('modalLoading', true)
    case EDIT_PROJECT_SUCCESS:
      payload.resolve()
      payload.final()
      projects.splice(projects.indexOf(projects.find(p => p.id === payload.result.id)), 1, payload.result)
      return state
        .set('projects', projects.slice())
        .set('modalLoading', false)
    case LOAD_PROJECT_NAME_VALUE:
      return state.set('projectNameExited', false)
    case LOAD_PROJECT_NAME_VALUE_SUCCESS:
      payload.resolve()
      return state.set('projectNameExited', false)
    case LOAD_PROJECT_NAME_VALUE_ERROR:
      payload.reject()
      return state.set('projectNameExited', true)
    case DELETE_SINGLE_PROJECT:
      return state
    case DELETE_SINGLE_PROJECT_SUCCESS:
      console.log('1', payload)
      payload.resolve()
      return state.set('projects', projects.filter(g => g.id !== payload.result))
    case DELETE_SINGLE_PROJECT_ERROR:
      payload.reject(payload.result)
      return state
    case GET_ERROR:
      payload.final && payload.final()
      return state
    default:
      return state
  }
}

export default projectReducer
