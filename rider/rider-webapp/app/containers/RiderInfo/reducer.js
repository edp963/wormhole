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
  LOAD_RIDERINFOS,
  LOAD_RIDERINFOS_SUCCESS
} from './constants'
import { fromJS } from 'immutable'

const initialState = fromJS({
  riderInfos: false
})

function riderInfoReducer (state = initialState, { type, payload }) {
  switch (type) {
    case LOAD_RIDERINFOS:
      return state
    case LOAD_RIDERINFOS_SUCCESS:
      return state.set('riderInfos', payload.riderInfos)
    default:
      return state
  }
}

export default riderInfoReducer
