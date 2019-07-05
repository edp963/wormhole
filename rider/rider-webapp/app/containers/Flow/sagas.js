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
  LOAD_FLOW_DETAIL,
  LOAD_LOOKUP_SQL,

  LOAD_SOURCELOG_DETAIL,
  LOAD_SOURCESINK_DETAIL,
  LOAD_SINKWRITERROR_DETAIL,
  LOAD_SOURCEINPUT,
  EDIT_LOGFORM,
  SAVE_FORM,
  CHECKOUT_FORM,
  EDIT_FLOWS,
  QUERY_FLOW,
  STARTFLINK_FLOWS,
  LOAD_LASTEST_OFFSET,
  POST_USER_TOPIC,
  DELETE_USER_TOPIC,
  LOAD_UDFS,
  STOPFLINK_FLOWS,
  LOAD_ADMIN_LOGS_INFO,
  LOAD_LOGS_INFO,
  LOAD_DRIFT_LIST,
  POST_DRIFT,
  VERIFY_DRIFT,
  LOAD_FLOW_PERFORMANCE,
  LOAD_RECHARGE_HISTORY,
  COMFIRM_RECHARGE,
  LOAD_FLOW_ERROR_LIST
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
  flowDetailLoad,
  lookupSqlExisted,
  lookupSqlExistedError,

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
  flowQueryed,
  flowOperatedError,
  flinkFlowStartSucc,
  lastestOffsetLoaded,
  postUserTopicLoaded,
  deleteUserTopicLoaded,
  adminLogsInfoLoaded,
  logsInfoLoaded,
  rechargeHistoryLoaded,
  confirmReChangeLoaded
} from './action'

import request from '../../utils/request'
import api from '../../utils/api'
import { notifySagasError } from '../../utils/util'

export function* getAdminAllFlows ({ payload }) {
  try {
    const flows = yield call(request, api.flow)
    yield put(adminAllFlowsLoaded(flows.payload))
    payload.resolve()
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
    yield put(userAllFlowsLoaded(flows.payload))
    payload.resolve()
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
    yield put(adminSingleFlowLoaded(flow.payload))
    payload.resolve()
  } catch (err) {
    yield put(flowsLoadingError(err))
  }
}

export function* getAdminSingleFlowWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_SINGLE_FLOW, getAdminSingleFlow)
}

export function* getSelectStreamKafkaTopic ({ payload }) {
  try {
    let type = ''
    let value = ''
    if (payload.streamType === 'flink') {
      type = 'streamType'
      value = payload.streamType
    } else {
      type = 'functionType'
      value = payload.functionType
    }
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/streams?${type}=${value}`)
    yield put(selectStreamKafkaTopicLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getSelectStreamKafkaTopic')
  }
}

export function* getSelectStreamKafkaTopicWatcher () {
  yield fork(takeLatest, LOAD_SELECT_STREAM_KAFKA_TOPIC, getSelectStreamKafkaTopic)
}

export function* getTypeNamespace ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/streams/${payload.streamId}/namespaces?${payload.type}=${payload.value}`)
    if (result.code) {
      return
    } else if (result.header.code || result.header.code === 200) {
      yield put(sourceSinkTypeNamespaceLoaded(result.payload))
      payload.resolve(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'getTypeNamespace')
  }
}

export function* getTypeNamespaceWatcher () {
  yield fork(takeLatest, LOAD_SOURCESINKTYPE_NAMESPACE, getTypeNamespace)
}

export function* getSinkTypeNamespace ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/streams/${payload.streamId}/namespaces?${payload.type}=${payload.value}`)
    if (result.code) {
      return
    } else if (result.header.code || result.header.code === 200) {
      yield put(sinkTypeNamespaceLoaded(result.payload))
      payload.resolve(result.payload)
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
      yield put(tranSinkTypeNamespaceLoaded(result.payload))
      payload.resolve(result.payload)
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
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/flows?sourceNs=${payload.sourceNs}&sinkNs=${payload.sinkNs}`)
    if (result.code === 200) {
      yield put(sourceToSinkExistLoaded(result.msg))
      payload.resolve()
    } else {
      yield put(sourceToSinkExistErrorLoaded(result.msg))
      payload.reject()
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
    yield put(flowAdded(result.payload))
    payload.resolve(result.payload)
    payload.final()
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
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      if (payload.values.action === 'delete') {
        yield put(userFlowOperated(payload.values.flowIds))
        payload.resolve(payload.values.flowIds)
      } else {
        const temp = payload.values.flowIds.split(',')
        if (temp.length === 1) {
          yield put(userFlowOperated(result.payload[0]))
          payload.resolve(result.payload[0])
        } else if (temp.length > 1) {
          yield put(userFlowOperated(result.payload))
          payload.resolve(result.payload)
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
    const result = yield call(request, `${api.projectUserList}/${payload.values.projectId}/streams/${payload.values.streamId}/flows/${payload.values.id}`)
    yield put(flowQueryed(result.payload))
    payload.resolve(result.payload)
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
    yield put(sourceLogLoadedDetail(result.paginate.total, result.payload))
    payload.resolve(result.paginate.total, result.payload)
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
    yield put(sourceSinkDetailLoaded(result.paginate.total, result.payload))
    payload.resolve(result.paginate.total, result.payload)
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
    yield put(sinkWriteRrrorDetailLoaded(result.paginate.total, result.payload))
    payload.resolve(result.paginate.total, result.payload)
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
    yield put(sourceInputLoaded(result.payload))
    payload.resolve(result.payload)
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
    yield put(logFormEdited(result))
    payload.resolve()
  } catch (err) {
    yield put(logFormEditingError(err))
  }
}

export function* editLogFormWatcher () {
  yield fork(takeEvery, EDIT_LOGFORM, editLogForm)
}

export function* saveForm ({ payload }) {
  let { taskId, createTsColumn, updateTsColumn, connectUrl } = payload.value
  payload.value.flowId = parseInt(payload.value.flowId)
  if (!taskId) {
    taskId = 0
  }

  if (!createTsColumn) {
    createTsColumn = ''
  }

  if (!updateTsColumn) {
    updateTsColumn = ''
  }

  if (!connectUrl) {
    connectUrl = ''
  }

  try {
    const result = yield call(request, {
      method: 'post',
      url: `${api.flowTask}/${payload.flowId}`,
      data: Object.assign(payload.value, {
        taskType: payload.taskType,
        startTime: '',
        endTime: ''
      })
    })
    yield put(formSaved(result.payload))
    payload.resolve()
  } catch (err) {
    yield put(formSavingError(err))
  }
}

export function* saveFormWatcher () {
  yield fork(takeEvery, SAVE_FORM, saveForm)
}

export function* checkOutForm ({ payload }) {
  payload.value.flowId = parseInt(payload.value.flowId)
  let { taskId, createTsColumn, updateTsColumn, connectUrl } = payload.value
  if (!taskId) {
    taskId = 0
  }

  if (!createTsColumn) {
    createTsColumn = ''
  }

  if (!updateTsColumn) {
    updateTsColumn = ''
  }

  if (!connectUrl) {
    connectUrl = ''
  }

  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.flowTask}/${payload.flowId}`,
      data: Object.assign(payload.value, {
        taskType: payload.taskType,
        startTime: payload.startDate,
        endTime: payload.endDate
      })
    })
    yield put(formCheckOuted(result.payload))
    payload.resolve()
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
    yield put(flowEdited(result.payload))
    payload.resolve()
    payload.final()
  } catch (err) {
    notifySagasError(err, 'editFlow')
  }
}

export function* editFlowWatcher () {
  yield fork(takeEvery, EDIT_FLOWS, editFlow)
}

export function* queryFlow ({ payload }) {
  const apiFinal = payload.value.roleType === 'admin'
    ? `${api.projectList}/${payload.value.projectId}/flows/${payload.value.flowId}`
    : `${api.projectUserList}/${payload.value.projectId}/streams/${payload.value.streamId}/flows/${payload.value.flowId}`
  try {
    const result = yield call(request, apiFinal)
    yield put(flowDetailLoad(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'queryFlow')
  }
}

export function* queryFlowWatcher () {
  yield fork(takeEvery, LOAD_FLOW_DETAIL, queryFlow)
}

export function* queryLookupSql ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectUserList}/${payload.values.projectId}/streams/${payload.values.streamId}/flows/sqls/lookup`,
      data: payload.values.sql
    })
    if (result.code === 406) {
      yield put(lookupSqlExistedError(result.msg))
      payload.reject(result.msg)
    }
    if (result.code === 200) {
      yield put(lookupSqlExisted(result.payload))
      payload.resolve()
    }
  } catch (err) {
    notifySagasError(err, 'queryLookupSql')
  }
}

export function* queryLookupSqlWatcher () {
  yield fork(takeEvery, LOAD_LOOKUP_SQL, queryLookupSql)
}

export function* startFlinkFlow ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectStream}/${payload.projectId}/flinkstreams/flows/${payload.id}/${payload.action}`,
      data: payload.topicResult
    })
    if (result.code && result.code !== 200) {
      yield put(flowOperatedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(flinkFlowStartSucc(result.payload))
      payload.resolve()
    } else {
      yield put(flowOperatedError(result.payload))
      payload.reject(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'startFlinkFlow')
  }
}

export function* startFlinkFlowWathcer () {
  yield fork(takeEvery, STARTFLINK_FLOWS, startFlinkFlow)
}

export function* stopFlinkFlow ({payload}) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectStream}/${payload.projectId}/flinkstreams/flows/${payload.id}/stop`,
      data: null
    })
    if (result.code && result.code !== 200) {
      yield put(flowOperatedError(result.msg))
      payload.reject(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(flinkFlowStartSucc(result.payload))
      payload.resolve()
    } else {
      yield put(flowOperatedError(result.payload))
      payload.reject(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'stopFlinkFlow')
  }
}

export function* stopFlinkFlowWathcer () {
  yield fork(takeEvery, STOPFLINK_FLOWS, stopFlinkFlow)
}

export function* getLastestOffset ({ payload }) {
  let req = null
  if (payload.type === 'get') {
    req = `${api.projectStream}/${payload.projectId}/flows/${payload.streamId}/topics`
  } else if (payload.type === 'post') {
    req = {
      method: 'post',
      url: `${api.projectStream}/${payload.projectId}/flows/${payload.streamId}/topics`,
      data: payload.topics
    }
  }
  try {
    const result = yield call(request, req)
    if (result.code && result.code === 200) {
      yield put(lastestOffsetLoaded(result.msg))
      payload.resolve(result.msg)
    } else if (result.header.code && result.header.code === 200) {
      yield put(lastestOffsetLoaded(result.payload))
      payload.resolve(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'getLastestOffset')
  }
}

export function* getLastestOffsetWatcher () {
  yield fork(takeLatest, LOAD_LASTEST_OFFSET, getLastestOffset)
}

export function* addUserTopic ({payload}) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: `${api.projectUserList}/${payload.projectId}/flows/${payload.streamId}/topics/userdefined`,
      data: payload.topic
    })
    if (result.header.code && result.header.code === 200) {
      yield put(postUserTopicLoaded(result.payload))
      payload.resolve(result.payload)
    } else {
      payload.reject(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'addUserTopic')
  }
}

export function* addUserTopicWatcher () {
  yield fork(takeEvery, POST_USER_TOPIC, addUserTopic)
}

export function* removeUserTopic ({payload}) {
  try {
    const result = yield call(request, {
      method: 'delete',
      url: `${api.projectUserList}/${payload.projectId}/flows/${payload.streamId}/topics/userdefined/${payload.topicId}`
    })
    if (result.header.code && result.header.code === 200) {
      yield put(deleteUserTopicLoaded(result.payload))
      payload.resolve(result.payload)
    } else {
      payload.reject(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'removeUserTopic')
  }
}

export function* removeUserTopicWatcher () {
  yield fork(takeEvery, DELETE_USER_TOPIC, removeUserTopic)
}
export function* getUdfs ({payload}) {
  const apiFinal = payload.roleType === 'admin'
  ? `${api.projectAdminStream}`
  : `${api.projectStream}`
  try {
    const result = yield call(request, `${apiFinal}/${payload.projectId}/flows/${payload.streamId}/udfs`)
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getUdfs')
  }
}

export function* getUdfsWatcher () {
  yield fork(takeEvery, LOAD_UDFS, getUdfs)
}

export function* getLogs ({ payload }) {
  try {
    const result = yield call(request, `${api.projectStream}/${payload.projectId}/flows/${payload.flowId}/logs`)
    yield put(logsInfoLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getLogs')
  }
}

export function* getLogsWatcher () {
  yield fork(takeLatest, LOAD_LOGS_INFO, getLogs)
}

export function* getAdminLogs ({ payload }) {
  try {
    const result = yield call(request, `${api.projectList}/${payload.projectId}/flows/${payload.flowId}/logs`)
    yield put(adminLogsInfoLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getAdminLogs')
  }
}

export function* getAdminLogsWatcher () {
  yield fork(takeLatest, LOAD_ADMIN_LOGS_INFO, getAdminLogs)
}

export function* getDriftList ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/flows/${payload.flowId}/drift/streams`)
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'getDriftList')
  }
}

export function* getDriftListWatcher () {
  yield fork(takeLatest, LOAD_DRIFT_LIST, getDriftList)
}

export function* verifyDrift ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/flows/${payload.flowId}/drift/tip?streamId=${payload.streamId}`)
    payload.resolve(result)
  } catch (err) {
    notifySagasError(err, 'verifyDrift')
  }
}

export function* verifyDriftWatcher () {
  yield fork(takeLatest, VERIFY_DRIFT, verifyDrift)
}

export function* submitDrift ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'put',
      url: `${api.projectUserList}/${payload.projectId}/flows/${payload.flowId}/drift`,
      data: {streamId: payload.streamId}
    })
    if (result.header && result.header.code === 200) {
      yield put(flinkFlowStartSucc(result.payload))
    }
    payload.resolve(result.payload)
  } catch (err) {
    notifySagasError(err, 'submitDrift')
  }
}

export function* postDriftWatcher () {
  yield fork(takeLatest, POST_DRIFT, submitDrift)
}

export function* searchPerformance ({ payload }) {
  let { startTime, endTime } = payload
  try {
    const result = yield call(request, {
      method: 'post',
      url: `${api.projectUserList}/monitor/${payload.projectId}/flow/${payload.flowId}`,
      data: {startTime, endTime}
    })
    if (result.header && result.header.code === 200) {
      payload.resolve(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'searchPerformance')
  }
}

export function* postPerformanceWatcher () {
  yield fork(takeLatest, LOAD_FLOW_PERFORMANCE, searchPerformance)
}

export function* getRechargeHistory ({ payload }) {
  try {
    const result = yield call(request, `${api.projectUserList}/${payload.projectId}/errors/${payload.id}/log`)
    yield put(rechargeHistoryLoaded(result.payload))
    payload.resolve(result.payload)
  } catch (err) {
    yield put(flowsLoadingError(err))
  }
}

export function* getRechargeHistoryWatcher () {
  yield fork(takeLatest, LOAD_RECHARGE_HISTORY, getRechargeHistory)
}

export function* reChangeConfirm ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'post',
      url: `${api.projectUserList}/${payload.projectId}/errors/${payload.id}/backfill`,
      data: {protocolType: payload.protocolType}
    })
    if (result.header && result.header.code === 200) {
      yield put(confirmReChangeLoaded(result.payload))
      payload.resolve(result.payload)
    } else {
      yield put(operateFlowError(result.msg, payload.reject))
      payload.reject(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'reChangeConfirm')
  }
}

export function* reChangeConfirmWatcher () {
  yield fork(takeLatest, COMFIRM_RECHARGE, reChangeConfirm)
}

export function* getErrorList ({ payload }) {
  try {
    const result = yield call(request, {
      method: 'get',
      url: `${api.projectUserList}/${payload.projectId}/flows/${payload.flowId}/errors`
    })
    if (result.header && result.header.code === 200) {
      payload.resolve(result.payload)
    } else {
      payload.reject(result.payload)
    }
  } catch (err) {
    notifySagasError(err, 'getErrorList')
  }
}

export function* getErrorListWatcher () {
  yield fork(takeLatest, LOAD_FLOW_ERROR_LIST, getErrorList)
}

export default [
  getAdminAllFlowsWatcher,
  getUserAllFlowsWatcher,
  getAdminSingleFlowWatcher,
  getSelectStreamKafkaTopicWatcher,
  getTypeNamespaceWatcher,
  getSinkTypeNamespaceWatcher,
  getTranSinkTypeNamespaceWatcher,
  getSourceToSinkWatcher,
  addFlowWatcher,
  operateUserFlowWatcher,
  queryFlowWatcher,
  queryLookupSqlWatcher,

  getSourceLogDetailWatcher,
  getSourceSinkDetailWatcher,
  getSinkWriteRrrorDetailWatcher,
  getSourceInputWatcher,
  editLogFormWatcher,
  saveFormWatcher,
  checkOutFormWatcher,
  editFlowWatcher,
  queryFormWatcher,

  startFlinkFlowWathcer,
  getLastestOffsetWatcher,
  addUserTopicWatcher,
  removeUserTopicWatcher,
  getUdfsWatcher,
  stopFlinkFlowWathcer,
  getLogsWatcher,
  getAdminLogsWatcher,
  getDriftListWatcher,
  postDriftWatcher,
  verifyDriftWatcher,
  postPerformanceWatcher,
  getRechargeHistoryWatcher,
  reChangeConfirmWatcher,
  getErrorListWatcher
]
