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
import { call, fork, put } from 'redux-saga/effects'
import {
  LOAD_PERFORMANCES
} from './constants'

import {
  performancesLoaded
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'
import { notifySagasError } from '../../utils/util'

export function* getPerformances ({ payload }) {
  let requestUrl = ''
  if (payload.roleType === 'admin') {
    requestUrl = `${api.projectList}/${payload.projectId}/monitors`
  } else if (payload.roleType === 'user') {
    requestUrl = `${api.projectUserList}/${payload.projectId}/monitors`
  }

  try {
    const performances = yield call(request, requestUrl)
    yield put(performancesLoaded(performances.payload))
  } catch (err) {
    notifySagasError(err, 'getPerformances')
  }
}

export function* getPerformancesWatcher () {
  yield fork(takeLatest, LOAD_PERFORMANCES, getPerformances)
}

export default [
  getPerformancesWatcher
]
