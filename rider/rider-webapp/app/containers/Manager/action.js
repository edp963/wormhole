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
  LOAD_USER_STREAMS,
  LOAD_USER_STREAMS_SUCCESS,
  LOAD_ADMIN_ALL_STREAMS,
  LOAD_ADMIN_ALL_STREAMS_SUCCESS,
  LOAD_ADMIN_SINGLE_STREAM,
  LOAD_ADMIN_SINGLE_STREAM_SUCCESS,
  LOAD_STREAM_DETAIL,
  LOAD_STREAM_DETAIL_SUCCESS,
  LOAD_STREAM_NAME_VALUE,
  LOAD_STREAM_NAME_VALUE_SUCCESS,
  LOAD_STREAM_NAME_VALUE_ERROR,
  LOAD_KAFKA,
  LOAD_KAFKA_SUCCESS,
  LOAD_STREAM_CONFIG_JVM,
  LOAD_STREAM_CONFIG_JVM_SUCCESS,
  LOAD_STREAM_CONFIG_SPARK,
  LOAD_STREAM_CONFIG_SPARK_SUCCESS,
  LOAD_LOGS_INFO,
  LOAD_LOGS_INFO_SUCCESS,
  LOAD_ADMIN_LOGS_INFO,
  LOAD_ADMIN_LOGS_INFO_SUCCESS,
  ADD_STREAMS,
  ADD_STREAMS_SUCCESS,
  EDIT_STREAM,
  EDIT_STREAM_SUCCESS,
  OPERATE_STREAMS,
  OPERATE_STREAMS_SUCCESS,
  DELETE_STREAMS,
  DELETE_STREAMS_SUCCESS,
  STARTORRENEW_STREAMS,
  STARTORRENEW_STREAMS_SUCCESS,
  OPERATE_STREAMS_ERROR,
  LOAD_LASTEST_OFFSET,
  LOAD_LASTEST_OFFSET_SUCCESS,
  POST_USER_TOPIC,
  POST_USER_TOPIC_SUCCESS,
  DELETE_USER_TOPIC,
  DELETE_USER_TOPIC_SUCCESS,
  LOAD_UDFS,
  LOAD_STREAM_CONFIGS,
  JUMP_STREAM_TO_FLOW_FILTER
} from './constants'

export function loadUserStreams (projectId, resolve) {
  return {
    type: LOAD_USER_STREAMS,
    payload: {
      projectId,
      resolve
    }
  }
}

export function userStreamsLoaded (streams) {
  return {
    type: LOAD_USER_STREAMS_SUCCESS,
    payload: {
      streams
    }
  }
}

export function loadAdminAllStreams (resolve) {
  return {
    type: LOAD_ADMIN_ALL_STREAMS,
    payload: {
      resolve
    }
  }
}

export function adminAllStreamsLoaded (streams) {
  return {
    type: LOAD_ADMIN_ALL_STREAMS_SUCCESS,
    payload: {
      streams
    }
  }
}

export function loadAdminSingleStream (projectId, resolve) {
  return {
    type: LOAD_ADMIN_SINGLE_STREAM,
    payload: {
      projectId,
      resolve
    }
  }
}

export function adminSingleStreamLoaded (stream) {
  return {
    type: LOAD_ADMIN_SINGLE_STREAM_SUCCESS,
    payload: {
      stream
    }
  }
}

export function loadStreamDetail (projectId, streamId, roleType, resolve) {
  return {
    type: LOAD_STREAM_DETAIL,
    payload: {
      projectId,
      streamId,
      roleType,
      resolve
    }
  }
}

export function streamDetailLoaded (result) {
  return {
    type: LOAD_STREAM_DETAIL_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadStreamNameValue (projectId, value, resolve, reject) {
  return {
    type: LOAD_STREAM_NAME_VALUE,
    payload: {
      projectId,
      value,
      resolve,
      reject
    }
  }
}

export function streamNameValueLoaded (result) {
  return {
    type: LOAD_STREAM_NAME_VALUE_SUCCESS,
    payload: {
      result
    }
  }
}

export function streamNameValueErrorLoaded (result) {
  return {
    type: LOAD_STREAM_NAME_VALUE_ERROR,
    payload: {
      result
    }
  }
}

export function loadKafka (projectId, nsSys, resolve) {
  return {
    type: LOAD_KAFKA,
    payload: {
      projectId,
      nsSys,
      resolve
    }
  }
}

export function kafkaLoaded (result) {
  return {
    type: LOAD_KAFKA_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadStreamConfigJvm (resolve) {
  return {
    type: LOAD_STREAM_CONFIG_JVM,
    payload: {
      resolve
    }
  }
}

export function streamConfigJvmLoaded (result) {
  return {
    type: LOAD_STREAM_CONFIG_JVM_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadStreamConfigSpark (resolve) {
  return {
    type: LOAD_STREAM_CONFIG_SPARK,
    payload: {
      resolve
    }
  }
}

export function streamConfigSparkLoaded (result) {
  return {
    type: LOAD_STREAM_CONFIG_SPARK_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadStreamConfigs (type, resolve) {
  return {
    type: LOAD_STREAM_CONFIGS,
    payload: {
      type,
      resolve
    }
  }
}
export function loadLogsInfo (projectId, streamId, resolve) {
  return {
    type: LOAD_LOGS_INFO,
    payload: {
      projectId,
      streamId,
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

export function loadAdminLogsInfo (projectId, streamId, resolve) {
  return {
    type: LOAD_ADMIN_LOGS_INFO,
    payload: {
      projectId,
      streamId,
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

export function addStream (projectId, stream, resolve) {
  return {
    type: ADD_STREAMS,
    payload: {
      projectId,
      stream,
      resolve
    }
  }
}

export function streamAdded (result) {
  return {
    type: ADD_STREAMS_SUCCESS,
    payload: {
      result
    }
  }
}

export function editStream (stream, resolve) {
  return {
    type: EDIT_STREAM,
    payload: {
      stream,
      resolve
    }
  }
}

export function streamEdited (result) {
  return {
    type: EDIT_STREAM_SUCCESS,
    payload: {
      result
    }
  }
}

export function operateStream (projectId, id, action, resolve, reject) {
  return {
    type: OPERATE_STREAMS,
    payload: {
      projectId,
      id,
      action,
      resolve,
      reject
    }
  }
}

export function streamOperated (result) {
  return {
    type: OPERATE_STREAMS_SUCCESS,
    payload: {
      result
    }
  }
}

export function deleteStream (projectId, id, action, resolve, reject) {
  return {
    type: DELETE_STREAMS,
    payload: {
      projectId,
      id,
      action,
      resolve,
      reject
    }
  }
}

export function streamDeleted (result) {
  return {
    type: DELETE_STREAMS_SUCCESS,
    payload: {
      result
    }
  }
}

export function startOrRenewStream (projectId, id, topicResult, action, resolve, reject) {
  return {
    type: STARTORRENEW_STREAMS,
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

export function streamStartOrRenewed (result) {
  return {
    type: STARTORRENEW_STREAMS_SUCCESS,
    payload: {
      result
    }
  }
}

export function streamOperatedError (message) {
  return {
    type: OPERATE_STREAMS_ERROR,
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
export function jumpStreamToFlowFilter (streamFilterId) {
  return {
    type: JUMP_STREAM_TO_FLOW_FILTER,
    payload: {
      streamFilterId
    }
  }
}
