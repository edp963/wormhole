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

import { fromJS } from 'immutable'
import { LOGIN, LOGIN_SUCCESS, LOGIN_FAILURE, LOG_PSW_ERROR } from './constants'

const initialState = fromJS({
  error: false
})

export function loginReducer (state = initialState, { type, payload }) {
  switch (type) {
    case LOGIN:
      return state.set('error', false)
    case LOGIN_SUCCESS:
      // trigger LOCATION_CHANGE action, should async
      // 存储数据到 localStorage 对象里
      localStorage.setItem('loginCreateBy', payload.result.createBy)
      localStorage.setItem('loginCreateTime', payload.result.createTime)
      localStorage.setItem('loginEmail', payload.result.email)
      localStorage.setItem('loginId', payload.result.id)
      localStorage.setItem('loginName', payload.result.name)
      localStorage.setItem('loginPassword', payload.result.password)
      localStorage.setItem('loginRoleType', payload.result.roleType)
      localStorage.setItem('loginUpdateBy', payload.result.updateBy)
      localStorage.setItem('loginUpdateTime', payload.result.updateTime)

      setTimeout(() => {
        payload.resolve()
      }, 10)

      return state
    case LOGIN_FAILURE:
      payload.reject()
      return state
    case LOG_PSW_ERROR:
      payload.reject(payload.message)
      return state
    default:
      return state
  }
}

export default loginReducer
