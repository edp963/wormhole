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
  LOAD_ADMIN_ALL_FLOWS,
  LOAD_ADMIN_ALL_FLOWS_SUCCESS,
  LOAD_USER_ALL_FLOWS,
  LOAD_USER_ALL_FLOWS_SUCCESS,
  LOAD_FLOWS_ERROR,
  LOAD_ADMIN_SINGLE_FLOW,
  LOAD_ADMIN_SINGLE_FLOW_SUCCESS,
  LOAD_SELECT_STREAM_KAFKA_TOPIC,
  LOAD_SELECT_STREAM_KAFKA_TOPIC_SUCCESS,
  LOAD_SOURCESINKTYPE_NAMESPACE,
  LOAD_SOURCESINKTYPE_NAMESPACE_SUCCESS,
  LOAD_SINKTYPE_NAMESPACE,
  LOAD_SINKTYPE_NAMESPACE_SUCCESS,
  LOAD_TRANSINKTYPE_NAMESPACE,
  LOAD_TRANSINKTYPE_NAMESPACE_SUCCESS,
  LOAD_SOURCETOSINK_EXIST,
  LOAD_SOURCETOSINK_EXIST_SUCCESS,
  LOAD_SOURCETOSINK_EXIST_ERROR,
  ADD_FLOWS,
  ADD_FLOWS_SUCCESS,
  OPERATE_USER_FLOW,
  OPERATE_USER_FLOW_SUCCESS,
  OPERATE_FLOW_ERROR,
  LOAD_FLOW_DETAIL,
  LOAD_FLOW_DETAIL_SUCCESS,
  CHUCKAWAY_FLOW,
  LOAD_LOOKUP_SQL,
  LOAD_LOOKUP_SQL_SUCCESS,
  LOAD_LOOKUP_SQL_ERROR,

  LOAD_SOURCELOG_DETAIL,
  LOAD_SOURCELOG_DETAIL_SUCCESS,
  LOAD_SOURCELOG_DETAIL_ERROR,
  LOAD_SOURCESINK_DETAIL,
  LOAD_SOURCESINK_DETAIL_SUCCESS,
  LOAD_SOURCESINK_DETAIL_ERROR,
  LOAD_SINKWRITERROR_DETAIL,
  LOAD_SINKWRITERROR_DETAIL_SUCCESS,
  LOAD_SINKWRITERROR_DETAIL_ERROR,
  EDIT_LOGFORM,
  EDIT_LOGFORM_SUCCESS,
  EDIT_LOGFORM_ERROR,
  SAVE_FORM,
  SAVE_FORM_SUCCESS,
  SAVE_FORM_ERROR,
  CHECKOUT_FORM,
  CHECKOUT_FORM_SUCCESS,
  CHECKOUT_FORM_ERROR,
  EDIT_FLOWS,
  EDIT_FLOWS_SUCCESS,
  LOAD_SOURCEINPUT,
  LOAD_SOURCEINPUT_SUCCESS,
  LOAD_SOURCEINPUT_ERROR,
  QUERY_FLOW,
  QUERY_FLOW_SUCCESS,
  STARTFLINK_FLOWS,
  STARTFLINK_FLOWS_SUCCESS,
  OPERATE_FLOWS_ERROR,
  LOAD_LASTEST_OFFSET,
  LOAD_LASTEST_OFFSET_SUCCESS,
  POST_USER_TOPIC,
  POST_USER_TOPIC_SUCCESS,
  DELETE_USER_TOPIC,
  DELETE_USER_TOPIC_SUCCESS,
  LOAD_UDFS,
  STOPFLINK_FLOWS,
  LOAD_ADMIN_LOGS_INFO,
  LOAD_ADMIN_LOGS_INFO_SUCCESS,
  LOAD_LOGS_INFO,
  LOAD_LOGS_INFO_SUCCESS,
  LOAD_DRIFT_LIST,
  POST_DRIFT,
  VERIFY_DRIFT,
  LOAD_FLOW_PERFORMANCE,
  LOAD_RECHARGE_HISTORY,
  LOAD_RECHARGE_HISTORY_SUCCESS,
  COMFIRM_RECHARGE,
  COMFIRM_RECHARGE_SUCCESS,
  LOAD_FLOW_ERROR_LIST
} from './constants'

export function loadAdminAllFlows (resolve) {
  return {
    type: LOAD_ADMIN_ALL_FLOWS,
    payload: {
      resolve
    }
  }
}

export function adminAllFlowsLoaded (flows) {
  return {
    type: LOAD_ADMIN_ALL_FLOWS_SUCCESS,
    payload: {
      flows
    }
  }
}

export function loadUserAllFlows (projectId, resolve) {
  return {
    type: LOAD_USER_ALL_FLOWS,
    payload: {
      projectId,
      resolve
    }
  }
}

export function userAllFlowsLoaded (flows) {
  return {
    type: LOAD_USER_ALL_FLOWS_SUCCESS,
    payload: {
      flows
    }
  }
}

export function loadAdminSingleFlow (projectId, resolve) {
  return {
    type: LOAD_ADMIN_SINGLE_FLOW,
    payload: {
      projectId,
      resolve
    }
  }
}

export function adminSingleFlowLoaded (flow) {
  return {
    type: LOAD_ADMIN_SINGLE_FLOW_SUCCESS,
    payload: {
      flow
    }
  }
}

export function loadSelectStreamKafkaTopic (projectId, streamType, functionType, resolve) {
  return {
    type: LOAD_SELECT_STREAM_KAFKA_TOPIC,
    payload: {
      projectId,
      streamType,
      functionType,
      resolve
    }
  }
}

export function selectStreamKafkaTopicLoaded (result) {
  return {
    type: LOAD_SELECT_STREAM_KAFKA_TOPIC_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadSourceSinkTypeNamespace (projectId, streamId, value, type, resolve) {
  return {
    type: LOAD_SOURCESINKTYPE_NAMESPACE,
    payload: {
      projectId,
      streamId,
      value,
      type,
      resolve
    }
  }
}

export function sourceSinkTypeNamespaceLoaded (result) {
  return {
    type: LOAD_SOURCESINKTYPE_NAMESPACE_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadSinkTypeNamespace (projectId, streamId, value, type, resolve) {
  return {
    type: LOAD_SINKTYPE_NAMESPACE,
    payload: {
      projectId,
      streamId,
      value,
      type,
      resolve
    }
  }
}

export function sinkTypeNamespaceLoaded (result) {
  return {
    type: LOAD_SINKTYPE_NAMESPACE_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadTranSinkTypeNamespace (projectId, streamId, value, type, resolve) {
  return {
    type: LOAD_TRANSINKTYPE_NAMESPACE,
    payload: {
      projectId,
      streamId,
      value,
      type,
      resolve
    }
  }
}

export function tranSinkTypeNamespaceLoaded (result) {
  return {
    type: LOAD_TRANSINKTYPE_NAMESPACE_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadSourceToSinkExist (projectId, sourceNs, sinkNs, resolve, reject) {
  return {
    type: LOAD_SOURCETOSINK_EXIST,
    payload: {
      projectId,
      sourceNs,
      sinkNs,
      resolve,
      reject
    }
  }
}

export function sourceToSinkExistLoaded (result) {
  return {
    type: LOAD_SOURCETOSINK_EXIST_SUCCESS,
    payload: {
      result
    }
  }
}

export function sourceToSinkExistErrorLoaded (result) {
  return {
    type: LOAD_SOURCETOSINK_EXIST_ERROR,
    payload: {
      result
    }
  }
}

export function addFlow (values, resolve, final) {
  return {
    type: ADD_FLOWS,
    payload: {
      values,
      resolve,
      final
    }
  }
}

export function flowAdded (result) {
  return {
    type: ADD_FLOWS_SUCCESS,
    payload: {
      result
    }
  }
}

export function operateUserFlow (values, resolve, reject) {
  return {
    type: OPERATE_USER_FLOW,
    payload: {
      values,
      resolve,
      reject
    }
  }
}

export function userFlowOperated (result) {
  return {
    type: OPERATE_USER_FLOW_SUCCESS,
    payload: {
      result
    }
  }
}

export function operateFlowError (message) {
  return {
    type: OPERATE_FLOW_ERROR,
    payload: {
      message
    }
  }
}

export function queryFlow (values, resolve) {
  return {
    type: QUERY_FLOW,
    payload: {
      values,
      resolve
    }
  }
}

export function flowQueryed (result) {
  return {
    type: QUERY_FLOW_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadLookupSql (values, resolve, reject) {
  return {
    type: LOAD_LOOKUP_SQL,
    payload: {
      values,
      resolve,
      reject
    }
  }
}

export function lookupSqlExisted (result) {
  return {
    type: LOAD_LOOKUP_SQL_SUCCESS,
    payload: {
      result
    }
  }
}

export function lookupSqlExistedError (result) {
  return {
    type: LOAD_LOOKUP_SQL_ERROR,
    payload: {
      result
    }
  }
}

export function loadSourceLogDetail (id, pageIndex, pageSize, resolve) {
  return {
    type: LOAD_SOURCELOG_DETAIL,
    payload: {
      id,
      pageIndex,
      pageSize,
      resolve
    }
  }
}

export function sourceLogLoadedDetail (total, sourceLog) {
  return {
    type: LOAD_SOURCELOG_DETAIL_SUCCESS,
    payload: {
      total,
      sourceLog
    }
  }
}

export function sourceLogDetailLoadingError (error) {
  return {
    type: LOAD_SOURCELOG_DETAIL_ERROR,
    payload: {
      error
    }
  }
}

export function loadSourceSinkDetail (id, pageIndex, pageSize, resolve) {
  return {
    type: LOAD_SOURCESINK_DETAIL,
    payload: {
      id,
      pageIndex,
      pageSize,
      resolve
    }
  }
}

export function sourceSinkDetailLoaded (total, sourceSink) {
  return {
    type: LOAD_SOURCESINK_DETAIL_SUCCESS,
    payload: {
      total,
      sourceSink
    }
  }
}

export function sourceSinkDetailLoadingError (error) {
  return {
    type: LOAD_SOURCESINK_DETAIL_ERROR,
    payload: {
      error
    }
  }
}

export function loadSinkWriteRrrorDetail (id, pageIndex, pageSize, resolve) {
  return {
    type: LOAD_SINKWRITERROR_DETAIL,
    payload: {
      id,
      pageIndex,
      pageSize,
      resolve
    }
  }
}

export function sinkWriteRrrorDetailLoaded (total, sinkWriteRrror) {
  return {
    type: LOAD_SINKWRITERROR_DETAIL_SUCCESS,
    payload: {
      total,
      sinkWriteRrror
    }
  }
}

export function sinkWriteRrrorDetailLoadingError (error) {
  return {
    type: LOAD_SINKWRITERROR_DETAIL_ERROR,
    payload: {
      error
    }
  }
}

export function loadSourceInput (flowId, taskType, resolve) {
  return {
    type: LOAD_SOURCEINPUT,
    payload: {
      flowId,
      taskType,
      resolve
    }
  }
}

export function sourceInputLoaded (result) {
  return {
    type: LOAD_SOURCEINPUT_SUCCESS,
    payload: {
      result
    }
  }
}

export function sourceInputLoadingError (error) {
  return {
    type: LOAD_SOURCEINPUT_ERROR,
    payload: {
      error
    }
  }
}

export function editLogForm (flow, resolve) {
  return {
    type: EDIT_LOGFORM,
    payload: {
      flow,
      resolve
    }
  }
}

export function logFormEdited (result) {
  return {
    type: EDIT_LOGFORM_SUCCESS,
    payload: {
      result
    }
  }
}

export function logFormEditingError (error) {
  return {
    type: EDIT_LOGFORM_ERROR,
    payload: {
      error
    }
  }
}

export function saveForm (flowId, taskType, value, resolve) {
  return {
    type: SAVE_FORM,
    payload: {
      flowId,
      taskType,
      value,
      resolve
    }
  }
}

export function formSaved (result) {
  return {
    type: SAVE_FORM_SUCCESS,
    payload: {
      result
    }
  }
}

export function formSavingError (error) {
  return {
    type: SAVE_FORM_ERROR,
    payload: {
      error
    }
  }
}

export function checkOutForm (flowId, taskType, startDate, endDate, value, resolve) {
  return {
    type: CHECKOUT_FORM,
    payload: {
      flowId,
      taskType,
      startDate,
      endDate,
      value,
      resolve
    }
  }
}

export function formCheckOuted (result) {
  return {
    type: CHECKOUT_FORM_SUCCESS,
    payload: {
      result
    }
  }
}

export function formCheckOutingError (error) {
  return {
    type: CHECKOUT_FORM_ERROR,
    payload: {
      error
    }
  }
}

export function editFlow (values, resolve, final) {
  return {
    type: EDIT_FLOWS,
    payload: {
      values,
      resolve,
      final
    }
  }
}

export function flowEdited (result) {
  return {
    type: EDIT_FLOWS_SUCCESS,
    payload: {
      result
    }
  }
}

export function flowsLoadingError (error) {
  return {
    type: LOAD_FLOWS_ERROR,
    payload: {
      error
    }
  }
}

export function loadFlowDetail (value, resolve) {
  return {
    type: LOAD_FLOW_DETAIL,
    payload: {
      value,
      resolve
    }
  }
}

export function flowDetailLoad (result) {
  return {
    type: LOAD_FLOW_DETAIL_SUCCESS,
    payload: {
      result
    }
  }
}

export function chuckAwayFlow () {
  return {
    type: CHUCKAWAY_FLOW
  }
}

export function startFlinkFlow (projectId, id, topicResult, action, resolve, reject) {
  return {
    type: STARTFLINK_FLOWS,
    payload: {
      projectId,
      id,
      topicResult,
      action,
      resolve,
      reject
    }
  }
}

export function flinkFlowStartSucc (result) {
  return {
    type: STARTFLINK_FLOWS_SUCCESS,
    payload: {
      result
    }
  }
}

export function stopFlinkFlow (projectId, id, resolve, reject) {
  return {
    type: STOPFLINK_FLOWS,
    payload: {
      projectId,
      id,
      resolve,
      reject
    }
  }
}
export function flowOperatedError (message) {
  return {
    type: OPERATE_FLOWS_ERROR,
    payload: {
      message
    }
  }
}

export function loadLastestOffset (projectId, streamId, resolve, type = 'get', topics = []) {
  return {
    type: LOAD_LASTEST_OFFSET,
    payload: {
      projectId,
      streamId,
      type,
      topics,
      resolve
    }
  }
}

export function lastestOffsetLoaded (result) {
  return {
    type: LOAD_LASTEST_OFFSET_SUCCESS,
    payload: {
      result
    }
  }
}

export function postUserTopic (projectId, streamId, topic, resolve, reject) {
  return {
    type: POST_USER_TOPIC,
    payload: {
      projectId,
      streamId,
      topic,
      resolve,
      reject
    }
  }
}

export function postUserTopicLoaded (result) {
  return {
    type: POST_USER_TOPIC_SUCCESS,
    payload: {
      result
    }
  }
}

export function deleteUserTopic (projectId, streamId, topicId, resolve, reject) {
  return {
    type: DELETE_USER_TOPIC,
    payload: {
      projectId,
      streamId,
      topicId,
      resolve,
      reject
    }
  }
}

export function deleteUserTopicLoaded (result) {
  return {
    type: DELETE_USER_TOPIC_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadUdfs (projectId, streamId, roleType, resolve) {
  return {
    type: LOAD_UDFS,
    payload: {
      projectId,
      streamId,
      roleType,
      resolve
    }
  }
}

export function loadLogsInfo (projectId, flowId, resolve) {
  return {
    type: LOAD_LOGS_INFO,
    payload: {
      projectId,
      flowId,
      resolve
    }
  }
}

export function logsInfoLoaded (result) {
  return {
    type: LOAD_LOGS_INFO_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadAdminLogsInfo (projectId, flowId, resolve) {
  return {
    type: LOAD_ADMIN_LOGS_INFO,
    payload: {
      projectId,
      flowId,
      resolve
    }
  }
}

export function adminLogsInfoLoaded (result) {
  return {
    type: LOAD_ADMIN_LOGS_INFO_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadDriftList (projectId, flowId, resolve) {
  return {
    type: LOAD_DRIFT_LIST,
    payload: {
      projectId,
      flowId,
      resolve
    }
  }
}

export function verifyDrift (projectId, flowId, streamId, resolve) {
  return {
    type: VERIFY_DRIFT,
    payload: {
      projectId,
      flowId,
      streamId,
      resolve
    }
  }
}

export function postDriftList (projectId, flowId, streamId, resolve) {
  return {
    type: POST_DRIFT,
    payload: {
      projectId,
      flowId,
      streamId,
      resolve
    }
  }
}
export function postFlowPerformance (projectId, flowId, startTime, endTime, resolve) {
  return {
    type: LOAD_FLOW_PERFORMANCE,
    payload: {
      projectId,
      flowId,
      startTime,
      endTime,
      resolve
    }
  }
}

export function loadRechargeHistory (projectId, id, resolve) {
  return {
    type: LOAD_RECHARGE_HISTORY,
    payload: {
      projectId,
      id,
      resolve
    }
  }
}

export function rechargeHistoryLoaded (list) {
  return {
    type: LOAD_RECHARGE_HISTORY_SUCCESS,
    payload: {
      list
    }
  }
}

export function confirmReChange (projectId, id, protocolType, resolve, reject) {
  return {
    type: COMFIRM_RECHARGE,
    payload: {
      projectId,
      id,
      protocolType,
      resolve,
      reject
    }
  }
}

export function confirmReChangeLoaded (result) {
  return {
    type: COMFIRM_RECHARGE_SUCCESS,
    payload: {
      result
    }
  }
}
export function getErrorList (projectId, flowId, resolve) {
  return {
    type: LOAD_FLOW_ERROR_LIST,
    payload: {
      projectId,
      flowId,
      resolve
    }
  }
}
