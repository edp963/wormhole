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

import {
  SET_PROJECT,
  LOGIN,
  LOGIN_SUCCESS,
  LOGIN_FAILURE,
  LOG_PSW_ERROR,
  DEFAULT_LOCALE,
  SET_ROLETYPE
} from './constants'

const initialState = fromJS({
  currentProject: '',
  error: false,
  locale: DEFAULT_LOCALE,
  roleType: ''
})

function appReducer (state = initialState, { type, payload }) {
  switch (type) {
    case SET_PROJECT:
      return state.set('currentProject', payload.projectId)
    case LOGIN:
      return state.set('error', false)
    case LOGIN_SUCCESS:
      // trigger LOCATION_CHANGE action, should async
      // 存储数据到 localStorage 对象里
      const lanType = payload.result.preferredLanguage === 'chinese' ? 'zh' : 'en'
      localStorage.setItem('loginCreateBy', payload.result.createBy)
      localStorage.setItem('loginCreateTime', payload.result.createTime)
      localStorage.setItem('loginEmail', payload.result.email)
      localStorage.setItem('loginId', payload.result.id)
      localStorage.setItem('loginName', payload.result.name)
      localStorage.setItem('loginPassword', payload.result.password)
      localStorage.setItem('loginRoleType', payload.result.roleType)
      localStorage.setItem('loginUpdateBy', payload.result.updateBy)
      localStorage.setItem('loginUpdateTime', payload.result.updateTime)
      localStorage.setItem('preferredLanguage', lanType)
      localStorage.setItem('loginActive', payload.result.active)

      return state
        .set('locale', lanType)
        .set('roleType', payload.result.roleType)
    case LOGIN_FAILURE:
      return state
    case LOG_PSW_ERROR:
      return state
    case SET_ROLETYPE:
      return state.set('roleType', payload.type)
    default:
      return state
  }
}

export default appReducer
