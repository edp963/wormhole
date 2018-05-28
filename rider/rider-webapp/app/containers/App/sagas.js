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

import { takeLatest } from 'redux-saga'
import { put, fork, call } from 'redux-saga/effects'
import { LOGIN } from './constants'
import { logged, logError, logPswError } from './actions'

import request from '../../utils/request'
import api from '../../utils/api'

export function* login ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: api.login,
      data: payload.logoInfo
    })
    if (result.code && result.code !== 200) {
      yield put(logPswError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(logged(result.payload))
      payload.resolve(result.payload)
    }
  } catch (err) {
    yield put(logError(err))
    payload.reject()
  }
}

export function* loginWatcher () {
  yield fork(takeLatest, LOGIN, login)
}

export default [
  loginWatcher
]
