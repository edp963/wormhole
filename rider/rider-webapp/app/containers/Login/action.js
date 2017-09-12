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

import { LOGIN, LOGIN_SUCCESS, LOGIN_FAILURE, LOG_OUT, LOG_OUT_SUCCESS, LOG_PSW_ERROR } from './constants'

export function login (logoInfo, resolve, reject) {
  return {
    type: LOGIN,
    payload: {
      logoInfo,
      resolve,
      reject
    }
  }
}

export function logged (result, resolve, reject) {
  return {
    type: LOGIN_SUCCESS,
    payload: {
      result,
      resolve,
      reject
    }
  }
}

export function logError () {
  return {
    type: LOGIN_FAILURE
  }
}

export function logOut () {
  return {
    type: LOG_OUT
  }
}

export function outLogged () {
  return {
    type: LOG_OUT_SUCCESS
  }
}

export function logPswError (message, reject) {
  return {
    type: LOG_PSW_ERROR,
    payload: {
      message,
      reject
    }
  }
}
