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
  GET_ERROR
} from './constants'

const initialState = fromJS({
  instances: false,
  error: false,
  modalLoading: false
})

export function instanceReducer (state = initialState, { type, payload }) {
  const instances = state.get('instances')

  switch (type) {
    case LOAD_INSTANCES:
      return state.set('error', false)
    case LOAD_INSTANCES_SUCCESS:
      return state.set('instances', payload.instances)
    case ADD_INSTANCE:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case ADD_INSTANCE_SUCCESS:
      instances.unshift(payload.result)
      return state
        .set('instances', instances.slice())
        .set('modalLoading', false)
    case ADD_INSTANCE_ERROR:
      return state.set('modalLoading', false)
    case LOAD_SINGLE_INSTANCE:
      return state.set('error', false)
    case LOAD_SINGLE_INSTANCE_SUCCESS:
      return state
    case EDIT_INSTANCE:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case EDIT_INSTANCE_SUCCESS:
      const startIndex = instances.indexOf(instances.find(p => Object.is(p.id, payload.result.id)))
      instances.fill(payload.result, startIndex, startIndex + 1)
      return state
        .set('instances', instances.slice())
        .set('modalLoading', false)
    case EDIT_INSTANCE_ERROR:
      return state.set('modalLoading', false)
    case DELETE_INSTANCE:
      return state
    case DELETE_INSTANCE_SUCCESS:
      return state.set('instances', instances.filter(g => !Object.is(g.id, payload.result)))
    case DELETE_INSTANCE_ERROR:
      return state
    case GET_ERROR:
      return state.set('error', payload.error)
    default:
      return state
  }
}

export default instanceReducer
