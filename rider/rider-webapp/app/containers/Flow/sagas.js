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

import { takeLatest, takeEvery, throttle } from 'redux-saga'
import { call, fork, put } from 'redux-saga/effects'
import {
  LOAD_ADMIN_ALL_FLOWS,
  LOAD_USER_ALL_FLOWS,
  LOAD_ADMIN_SINGLE_FLOW,
  LOAD_SELECT_STREAM_KAFKA_TOPIC,
  LOAD_SOURCESINKTYPE_NAMESPACE,
  LOAD_SINKTYPE_NAMESPACE,
  LOAD_TRANSINKTYPE_NAMESPACE,
  LOAD_SOURCETOSINK_EXIST,
  ADD_FLOWS,
  OPERATE_USER_FLOW,

  LOAD_SOURCELOG_DETAIL,
  LOAD_SOURCESINK_DETAIL,
  LOAD_SINKWRITERROR_DETAIL,
  LOAD_SOURCEINPUT,
  EDIT_LOGFORM,
  SAVE_FORM,
  CHECKOUT_FORM,
  EDIT_FLOWS,
  OPERATE_FLOWS,
  QUERY_FLOW
} from './constants'

import {
  adminAllFlowsLoaded,
  userAllFlowsLoaded,
  adminSingleFlowLoaded,
  selectStreamKafkaTopicLoaded,
  sourceSinkTypeNamespaceLoaded,
  sinkTypeNamespaceLoaded,
  tranSinkTypeNamespaceLoaded,
  sourceToSinkExistLoaded,
  sourceToSinkExistErrorLoaded,
  flowAdded,
  userFlowOperated,
  operateFlowError,

  flowsLoadingError,
  sourceLogLoadedDetail,
  sourceSinkDetailLoaded,
  sinkWriteRrrorDetailLoaded,
  sourceInputLoaded,
  sourceLogDetailLoadingError,
  sourceSinkDetailLoadingError,
  sinkWriteRrrorDetailLoadingError,
  sourceInputLoadingError,
  logFormEdited,
  logFormEditingError,
  formSaved,
  formSavingError,
  formCheckOuted,
  formCheckOutingError,
  flowEdited,
  flowOperated,
  flowQueryed
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'
import { notifySagasError } from '../../utils/util'

export function* getAdminAllFlows ({ payload }) {
  try {
    const flows = yield call(request, api.flow)
    yield put(adminAllFlowsLoaded(flows.payload, payload.resolve))
  } catch (err) {
    yield put(flowsLoadingError(err))
  }
}

export function* getAdminAllFlowsWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_ALL_FLOWS, getAdminAllFlows)
}

export function* getUserAllFlows ({ payload }) {
  try {
    const flows = yield call(request, `${api.projectUserList}/${payload.projectId}/flows`)
    yield put(userAllFlowsLoaded(flows.payload, payload.resolve))
  } catch (err) {
    yield put(flowsLoadingError(err))
  }
}

export function* getUserAllFlowsWatcher () {
  yield fork(takeLatest, LOAD_USER_ALL_FLOWS, getUserAllFlows)
}

export function* getAdminSingleFlow ({ payload }) {
  try {
    const flow = yield call(request, `${api.projectList}/${payload.projectId}/flows`)
    yield put(adminSingleFlowLoaded(flow.payload, payload.resolve))
  } catch (err) {
    yield put(flowsLoadingError(err))
  }
}

export function* getAdminSingleFlowWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_SINGLE_FLOW, getAdminSingleFlow)
}

export function* getSelectStreamKafkaTopic ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/streams?streamType=${payload.value}`)
    yield put(selectStreamKafkaTopicLoaded(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'getSelectStreamKafkaTopic')
  }
}

export function* getSelectStreamKafkaTopicWatcher () {
  yield fork(takeLatest, LOAD_SELECT_STREAM_KAFKA_TOPIC, getSelectStreamKafkaTopic)
}

export function* getSourceTypeNamespace ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/streams/${payload.streamId}/namespaces?${payload.type}=${payload.value}`)
    if (result.code) {
      return
    } else if (result.header.code || result.header.code === 200) {
      yield put(sourceSinkTypeNamespaceLoaded(result.payload, payload.resolve))
    }
  } catch (err) {
    notifySagasError(err, 'getSourceTypeNamespace')
  }
}

export function* getSourceTypeNamespaceWatcher () {
  yield fork(takeLatest, LOAD_SOURCESINKTYPE_NAMESPACE, getSourceTypeNamespace)
}

export function* getSinkTypeNamespace ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/streams/${payload.streamId}/namespaces?${payload.type}=${payload.value}`)
    if (result.code) {
      return
    } else if (result.header.code || result.header.code === 200) {
      yield put(sinkTypeNamespaceLoaded(result.payload, payload.resolve))
    }
  } catch (err) {
    notifySagasError(err, 'getSinkTypeNamespace')
  }
}

export function* getSinkTypeNamespaceWatcher () {
  yield fork(takeLatest, LOAD_SINKTYPE_NAMESPACE, getSinkTypeNamespace)
}

export function* getTranSinkTypeNamespace ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/streams/${payload.streamId}/namespaces?${payload.type}=${payload.value}`)
    if (result.code) {
      return
    } else if (result.header.code || result.header.code === 200) {
      yield put(tranSinkTypeNamespaceLoaded(result.payload, payload.resolve))
    }
  } catch (err) {
    notifySagasError(err, 'getTranSinkTypeNamespace')
  }
}

export function* getTranSinkTypeNamespaceWatcher () {
  yield fork(takeLatest, LOAD_TRANSINKTYPE_NAMESPACE, getTranSinkTypeNamespace)
}

export function* getSourceToSink ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.projectUserList}/${payload.projectId}/flows?sourceNs=${payload.sourceNs}&sinkNs=${payload.sinkNs}`
    })
    if (result.code === 200) {
      yield put(sourceToSinkExistLoaded(result.msg, payload.resolve))
    } else {
      yield put(sourceToSinkExistErrorLoaded(result.msg, payload.reject))
    }
  } catch (err) {
    notifySagasError(err, 'getSourceToSink')
  }
}

export function* getSourceToSinkWatcher () {
  yield fork(throttle, 500, LOAD_SOURCETOSINK_EXIST, getSourceToSink)
}

export function* addFlow ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: `${api.projectUserList}/${payload.values.projectId}/streams/${payload.values.streamId}/flows`,
      data: payload.values
    })
    yield put(flowAdded(result.payload, payload.resolve, payload.final))
  } catch (err) {
    notifySagasError(err, 'addFlow')
  }
}

export function* addFlowWatcher () {
  yield fork(takeEvery, ADD_FLOWS, addFlow)
}

export function* operateUserFlow ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectUserList}/${payload.values.projectId}/actions`,
      data: {
        action: payload.values.action,
        flowIds: payload.values.flowIds
      }
    })
    if (result.code && result.code !== 200) {
      yield put(operateFlowError(result.msg, payload.reject))
    } else if (result.header.code && result.header.code === 200) {
      if (payload.values.action === 'delete') {
        yield put(userFlowOperated(payload.values.flowIds, payload.resolve))
      } else {
        const temp = payload.values.flowIds.split(',')
        if (temp.length === 1) {
          yield put(userFlowOperated(result.payload[0], payload.resolve))
        } else if (temp.length > 1) {
          yield put(userFlowOperated(result.payload, payload.resolve))
        }
      }
    }
  } catch (err) {
    notifySagasError(err, 'operateUserFlow')
  }
}

export function* operateUserFlowWatcher () {
  yield fork(takeEvery, OPERATE_USER_FLOW, operateUserFlow)
}

export function* queryForm ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.projectUserList}/${payload.values.projectId}/streams/${payload.values.streamId}/flows/${payload.values.id}`
    })
    yield put(flowQueryed(result.payload, payload.resolve))
  } catch (err) {
    notifySagasError(err, 'queryForm')
  }
}

export function* queryFormWatcher () {
  yield fork(takeEvery, QUERY_FLOW, queryForm)
}

export function* getSourceLogDetail ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.flowHdfscompare}/${payload.id}`,
      params: {
        pageIndex: payload.pageIndex,
        pageSize: payload.pageSize
      }
    })
    yield put(sourceLogLoadedDetail(result.paginate.total, result.payload, payload.resolve))
  } catch (err) {
    yield put(sourceLogDetailLoadingError(err))
  }
}

export function* getSourceLogDetailWatcher () {
  yield fork(takeLatest, LOAD_SOURCELOG_DETAIL, getSourceLogDetail)
}

export function* getSourceSinkDetail ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.flowFlowcompare}/${payload.id}`,
      params: {
        pageIndex: payload.pageIndex,
        pageSize: payload.pageSize
      }
    })
    yield put(sourceSinkDetailLoaded(result.paginate.total, result.payload, payload.resolve))
  } catch (err) {
    yield put(sourceSinkDetailLoadingError(err))
  }
}

export function* getSourceSinkDetailWatcher () {
  yield fork(takeLatest, LOAD_SOURCESINK_DETAIL, getSourceSinkDetail)
}

export function* getSinkWriteRrrorDetail ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.flowSinkwriteerror}/${payload.id}`,
      params: {
        pageIndex: payload.pageIndex,
        pageSize: payload.pageSize
      }
    })
    yield put(sinkWriteRrrorDetailLoaded(result.paginate.total, result.payload, payload.resolve))
  } catch (err) {
    yield put(sinkWriteRrrorDetailLoadingError(err))
  }
}

export function* getSinkWriteRrrorDetailWatcher () {
  yield fork(takeLatest, LOAD_SINKWRITERROR_DETAIL, getSinkWriteRrrorDetail)
}

export function* getSourceInput ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.flowTask}/${payload.flowId}`,
      params: {
        taskType: payload.taskType
      }
    })
    yield put(sourceInputLoaded(result.payload, payload.resolve))
  } catch (err) {
    yield put(sourceInputLoadingError(err))
  }
}

export function* getSourceInputWatcher () {
  yield fork(takeEvery, LOAD_SOURCEINPUT, getSourceInput)
}

export function* editLogForm ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.flow}/${payload.flow.id}`,
      data: payload.flow
    })
    yield put(logFormEdited(result, payload.resolve))
  } catch (err) {
    yield put(logFormEditingError(err))
  }
}

export function* editLogFormWatcher () {
  yield fork(takeEvery, EDIT_LOGFORM, editLogForm)
}

export function* saveForm ({ payload }) {
  payload.value.flowId = parseInt(payload.value.flowId)
  if (payload.value.taskId === undefined) {
    payload.value.taskId = 0
  }

  if (payload.value.createTsColumn === undefined) {
    payload.value.createTsColumn = ''
  }

  if (payload.value.updateTsColumn === undefined) {
    payload.value.updateTsColumn = ''
  }

  if (payload.value.connectUrl === undefined) {
    payload.value.connectUrl = ''
  }

  try {
    const result = yield call(request, {
      method: 'post',
      url: `${api.flowTask}/${payload.flowId}`,
      data: Object.assign({}, payload.value, {
        taskType: payload.taskType,
        startTime: '',
        endTime: ''
      })
    })
    yield put(formSaved(result.payload, payload.resolve))
  } catch (err) {
    yield put(formSavingError(err))
  }
}

export function* saveFormWatcher () {
  yield fork(takeEvery, SAVE_FORM, saveForm)
}

export function* checkOutForm ({ payload }) {
  payload.value.flowId = parseInt(payload.value.flowId)
  if (payload.value.taskId === undefined) {
    payload.value.taskId = 0
  }

  if (payload.value.createTsColumn === undefined) {
    payload.value.createTsColumn = ''
  }

  if (payload.value.updateTsColumn === undefined) {
    payload.value.updateTsColumn = ''
  }

  if (payload.value.connectUrl === undefined) {
    payload.value.connectUrl = ''
  }

  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.flowTask}/${payload.flowId}`,
      data: Object.assign({}, payload.value, {
        taskType: payload.taskType,
        startTime: payload.startDate,
        endTime: payload.endDate
      })
    })
    yield put(formCheckOuted(result.payload, payload.resolve))
  } catch (err) {
    yield put(formCheckOutingError(err))
  }
}

export function* checkOutFormWatcher () {
  yield fork(takeEvery, CHECKOUT_FORM, checkOutForm)
}

export function* editFlow ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectUserList}/${payload.values.projectId}/streams/${payload.values.streamId}/flows`,
      data: payload.values
    })
    yield put(flowEdited(result.payload, payload.resolve, payload.final))
  } catch (err) {
    notifySagasError(err, 'editFlow')
  }
}

export function* editFlowWatcher () {
  yield fork(takeEvery, EDIT_FLOWS, editFlow)
}

export function* operateFlow ({ payload }) {
  try {
    yield call(request, {
      method: 'post',
      url: `${api.flowService}/${payload.projectId}`,
      data: {
        endTime: payload.endDate,
        startTime: payload.startDate,
        flowIds: payload.flowIds,
        operate: payload.operate
      }
    })
    yield put(flowOperated(payload.projectId, payload.flowIds, payload.operate, payload.resolve, payload.reject))
  } catch (err) {
    notifySagasError(err, 'operateFlow')
  }
}

export function* operateFlowWatcher () {
  yield fork(takeEvery, OPERATE_FLOWS, operateFlow)
}

export default [
  getAdminAllFlowsWatcher,
  getUserAllFlowsWatcher,
  getAdminSingleFlowWatcher,
  getSelectStreamKafkaTopicWatcher,
  getSourceTypeNamespaceWatcher,
  getSinkTypeNamespaceWatcher,
  getTranSinkTypeNamespaceWatcher,
  getSourceToSinkWatcher,
  addFlowWatcher,
  operateUserFlowWatcher,
  queryFormWatcher,

  getSourceLogDetailWatcher,
  getSourceSinkDetailWatcher,
  getSinkWriteRrrorDetailWatcher,
  getSourceInputWatcher,
  editLogFormWatcher,
  saveFormWatcher,
  checkOutFormWatcher,
  editFlowWatcher,
  operateFlowWatcher,
  queryFormWatcher
]
