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

import axios from 'axios'
import message from 'antd/lib/message'
import notifyError from 'util'

axios.defaults.validateStatus = function (status) {
  return status < 600
}

axios.interceptors.response.use(function (response) {
  if (response) {
    switch (response.status) {
      case 401:
        delete axios.defaults.headers.common['Authorization']
        localStorage.removeItem('token')
        location.hash = '#/login'
        break
      default:
        break
    }
  }
  return response
}, function (error) {
  return Promise.reject(error)
})
/**
 * Parses the JSON returned by a network request
 *
 * @param  {object} response A response from a network request
 *
 * @return {object}          The parsed JSON from the request
 */
function parseData (response) {
  return response.data
}

/**
 * Checks if a network request came back fine, and throws an error if not
 *
 * @param  {object} response   A response from a network request
 *
 * @return {object|undefined} Returns either the response, or throws an error
 */
function checkStatus (response) {
  const languageText = localStorage.getItem('preferredLanguage')
  const textRepeat = languageText === 'en'
    ? 'ServerException! Please try again later!'
    : '服务器异常，请稍后重试！'
  const text401 = languageText === 'en'
    ? 'Not login or session expired. Please login again!'
    : '未登录或会话过期，请重新登录！'
  const text403 = languageText === 'en'
    ? 'User Type Error!'
    : '用户类型错误！'
  switch (response.data.code) {
    case 401:
      message.error(text401, 3)
      delete axios.defaults.headers.common['Authorization']
      localStorage.removeItem('token')
      break
    case 403:
      message.error(text403, 3)
      break
    case 451:
      message.error(textRepeat, 3)
      break
    case 500:
      message.error(textRepeat, 3)
      break
    case 503:
      message.error(textRepeat, 3)
      break
    case 504:
      message.error(textRepeat, 3)
      break
    default:
      break
  }
  return response
}

function refreshToken (response) {
  if (response.data.header) {
    axios.defaults.headers.common['Authorization'] = `Bearer ${response.data.header.token}`
    localStorage.setItem('token', response.data.header.token)
  }
  return response
}

/**
 * Requests a URL, returning a promise
 *
 * @param  {object} [options] The options we want to pass to "fetch"
 *
 * @return {object}           The response data
 */
export default function request (options) {
  const languageText = localStorage.getItem('preferredLanguage')
  return axios.request(options)
    .then(checkStatus)
    .then(refreshToken)
    .then(parseData)
    .catch((error) => {
      // console.dir(error)
      notifyError(error, languageText === 'en' ? 'Request Error!' : '请求错误')
    })
}

request.setToken = (token) => {
  axios.defaults.headers.common['Authorization'] = `Bearer ${token}`
}

request.removeToken = () => {
  delete axios.defaults.headers.common['Authorization']
}
