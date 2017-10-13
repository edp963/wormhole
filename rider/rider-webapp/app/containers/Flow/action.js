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
  CHUCKAWAY_FLOW,

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
  OPERATE_FLOWS,
  OPERATE_FLOWS_SUCCESS,
  QUERY_FLOW,
  QUERY_FLOW_SUCCESS
} from './constants'

export function loadAdminAllFlows (resolve) {
  return {
    type: LOAD_ADMIN_ALL_FLOWS,
    payload: {
      resolve
    }
  }
}

export function adminAllFlowsLoaded (flows, resolve) {
  return {
    type: LOAD_ADMIN_ALL_FLOWS_SUCCESS,
    payload: {
      flows,
      resolve
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

export function userAllFlowsLoaded (flows, resolve) {
  return {
    type: LOAD_USER_ALL_FLOWS_SUCCESS,
    payload: {
      flows,
      resolve
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

export function adminSingleFlowLoaded (flow, resolve) {
  return {
    type: LOAD_ADMIN_SINGLE_FLOW_SUCCESS,
    payload: {
      flow,
      resolve
    }
  }
}

export function loadSelectStreamKafkaTopic (projectId, value, resolve) {
  return {
    type: LOAD_SELECT_STREAM_KAFKA_TOPIC,
    payload: {
      projectId,
      value,
      resolve
    }
  }
}

export function selectStreamKafkaTopicLoaded (result, resolve) {
  return {
    type: LOAD_SELECT_STREAM_KAFKA_TOPIC_SUCCESS,
    payload: {
      result,
      resolve
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

export function sourceSinkTypeNamespaceLoaded (result, resolve) {
  return {
    type: LOAD_SOURCESINKTYPE_NAMESPACE_SUCCESS,
    payload: {
      result,
      resolve
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

export function sinkTypeNamespaceLoaded (result, resolve) {
  return {
    type: LOAD_SINKTYPE_NAMESPACE_SUCCESS,
    payload: {
      result,
      resolve
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

export function tranSinkTypeNamespaceLoaded (result, resolve) {
  return {
    type: LOAD_TRANSINKTYPE_NAMESPACE_SUCCESS,
    payload: {
      result,
      resolve
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

export function sourceToSinkExistLoaded (result, resolve) {
  return {
    type: LOAD_SOURCETOSINK_EXIST_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function sourceToSinkExistErrorLoaded (result, reject) {
  return {
    type: LOAD_SOURCETOSINK_EXIST_ERROR,
    payload: {
      result,
      reject
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

export function flowAdded (result, resolve, final) {
  return {
    type: ADD_FLOWS_SUCCESS,
    payload: {
      result,
      resolve,
      final
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

export function userFlowOperated (result, resolve) {
  return {
    type: OPERATE_USER_FLOW_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function operateFlowError (message, reject) {
  return {
    type: OPERATE_FLOW_ERROR,
    payload: {
      message,
      reject
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

export function flowQueryed (result, resolve) {
  return {
    type: QUERY_FLOW_SUCCESS,
    payload: {
      result,
      resolve
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

export function sourceLogLoadedDetail (total, sourceLog, resolve) {
  return {
    type: LOAD_SOURCELOG_DETAIL_SUCCESS,
    payload: {
      total,
      sourceLog,
      resolve
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

export function sourceSinkDetailLoaded (total, sourceSink, resolve) {
  return {
    type: LOAD_SOURCESINK_DETAIL_SUCCESS,
    payload: {
      total,
      sourceSink,
      resolve
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

export function sinkWriteRrrorDetailLoaded (total, sinkWriteRrror, resolve) {
  return {
    type: LOAD_SINKWRITERROR_DETAIL_SUCCESS,
    payload: {
      total,
      sinkWriteRrror,
      resolve
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

export function sourceInputLoaded (result, resolve) {
  return {
    type: LOAD_SOURCEINPUT_SUCCESS,
    payload: {
      result,
      resolve
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

export function logFormEdited (result, resolve) {
  return {
    type: EDIT_LOGFORM_SUCCESS,
    payload: {
      result,
      resolve
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

export function formSaved (result, resolve) {
  return {
    type: SAVE_FORM_SUCCESS,
    payload: {
      result,
      resolve
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

export function formCheckOuted (result, resolve) {
  return {
    type: CHECKOUT_FORM_SUCCESS,
    payload: {
      result,
      resolve
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

export function flowEdited (result, resolve, final) {
  return {
    type: EDIT_FLOWS_SUCCESS,
    payload: {
      result,
      resolve,
      final
    }
  }
}

export function operateFlow (projectId, flowIds, operate, startDate, endDate, resolve, reject) {
  return {
    type: OPERATE_FLOWS,
    payload: {
      projectId,
      flowIds,
      operate,
      startDate,
      endDate,
      resolve,
      reject
    }
  }
}

export function flowOperated (projectId, flowIds, operate, resolve, reject) {
  return {
    type: OPERATE_FLOWS_SUCCESS,
    payload: {
      projectId,
      flowIds,
      operate,
      resolve,
      reject
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

export function chuckAwayFlow () {
  return {
    type: CHUCKAWAY_FLOW
  }
}

