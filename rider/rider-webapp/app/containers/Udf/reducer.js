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

const initialState = fromJS({
  udfs: false,
  error: false,
  modalLoading: false
})

export function udfReducer (state = initialState, { type, payload }) {
  const udfs = state.get('udfs')

  switch (type) {
    case LOAD_UDFS:
      return state.set('error', false)
    case LOAD_UDFS_SUCCESS:
      payload.resolve(payload.udfs)
      return state.set('udfs', payload.udfs)
    case LOAD_PROJECT_UDFS:
      return state.set('error', false)
    case LOAD_PROJECT_UDFS_SUCCESS:
      payload.resolve(payload.udfs)
      return state.set('udfs', payload.udfs)
    case LOAD_SINGLE_UDF:
      return state.set('error', false)
    case LOAD_SINGLE_UDF_SUCCESS:
      payload.resolve(payload.udf)
      return state.set('udfs', payload.udf)
    case ADD_UDF:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case ADD_UDF_SUCCESS:
      payload.resolve()
      udfs.unshift(payload.udf)
      return state
        .set('udfs', udfs.slice())
        .set('modalLoading', false)
    case ADD_UDF_ERROR:
      payload.reject(payload.result)
      return state.set('modalLoading', false)
    case EDIT_UDF:
      return state
        .set('error', false)
        .set('modalLoading', true)
    case EDIT_UDF_SUCCESS:
      payload.resolve()
      udfs.splice(udfs.indexOf(udfs.find(p => p.id === payload.udf.id)), 1, payload.udf)
      return state
        .set('udfs', udfs.slice())
        .set('modalLoading', false)
    case EDIT_UDF_ERROR:
      payload.reject(payload.result)
      return state.set('modalLoading', false)
    case DELETE_UDF:
      return state
    case DELETE_UDF_SUCCESS:
      payload.resolve()
      return state.set('udfs', udfs.filter(g => g.udfs.id !== payload.result))
    case GET_ERROR:
      return state.set('error', payload.error)
    default:
      return state
  }
}

export default udfReducer
