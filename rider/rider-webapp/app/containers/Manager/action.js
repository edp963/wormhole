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
  LOAD_ADMIN_OFFSET,
  LOAD_ADMIN_OFFSET_SUCCESS,
  LOAD_OFFSET,
  LOAD_OFFSET_SUCCESS,
  CHUCKAWAY_TOPIC,
  LOAD_STREAM_NAME_VALUE,
  LOAD_STREAM_NAME_VALUE_SUCCESS,
  LOAD_STREAM_NAME_VALUE_ERROR,
  LOAD_KAFKA,
  LOAD_KAFKA_SUCCESS,
  LOAD_STREAM_CONFIG_JVM,
  LOAD_STREAM_CONFIG_JVM_SUCCESS,
  LOAD_TOPICS,
  LOAD_TOPICS_SUCCESS,
  EDIT_TOPICS,
  EDIT_TOPICS_SUCCESS,
  LOAD_LOGS_INFO,
  LOAD_LOGS_INFO_SUCCESS,
  LOAD_ADMIN_LOGS_INFO,
  LOAD_ADMIN_LOGS_INFO_SUCCESS,
  ADD_STREAMS,
  ADD_STREAMS_SUCCESS,
  LOAD_SINGLE_STREAM,
  LOAD_SINGLE_STREAM_SUCCESS,
  EDIT_STREAM,
  EDIT_STREAM_SUCCESS,
  OPERATE_STREAMS,
  OPERATE_STREAMS_SUCCESS,
  STARTORRENEW_STREAMS,
  STARTORRENEW_STREAMS_SUCCESS,
  OPERATE_STREAMS_ERROR
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

export function userStreamsLoaded (streams, resolve) {
  return {
    type: LOAD_USER_STREAMS_SUCCESS,
    payload: {
      streams,
      resolve
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

export function adminAllStreamsLoaded (streams, resolve) {
  return {
    type: LOAD_ADMIN_ALL_STREAMS_SUCCESS,
    payload: {
      streams,
      resolve
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

export function adminSingleStreamLoaded (stream, resolve) {
  return {
    type: LOAD_ADMIN_SINGLE_STREAM_SUCCESS,
    payload: {
      stream,
      resolve
    }
  }
}

export function loadAdminOffset (projectId, streamId, resolve) {
  return {
    type: LOAD_ADMIN_OFFSET,
    payload: {
      projectId,
      streamId,
      resolve
    }
  }
}

export function adminOffsetLoaded (result, resolve) {
  return {
    type: LOAD_ADMIN_OFFSET_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function loadOffset (projectId, streamId, resolve) {
  return {
    type: LOAD_OFFSET,
    payload: {
      projectId,
      streamId,
      resolve
    }
  }
}

export function offsetLoaded (result, resolve) {
  return {
    type: LOAD_OFFSET_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function chuckAwayTopic () {
  return {
    type: CHUCKAWAY_TOPIC
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

export function streamNameValueLoaded (result, resolve) {
  return {
    type: LOAD_STREAM_NAME_VALUE_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function streamNameValueErrorLoaded (result, reject) {
  return {
    type: LOAD_STREAM_NAME_VALUE_ERROR,
    payload: {
      result,
      reject
    }
  }
}

export function loadKafka (projectId, resolve) {
  return {
    type: LOAD_KAFKA,
    payload: {
      projectId,
      resolve
    }
  }
}

export function kafkaLoaded (result, resolve) {
  return {
    type: LOAD_KAFKA_SUCCESS,
    payload: {
      result,
      resolve
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

export function streamConfigJvmLoaded (result, resolve) {
  return {
    type: LOAD_STREAM_CONFIG_JVM_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function loadTopics (projectId, instanceId, resolve) {
  return {
    type: LOAD_TOPICS,
    payload: {
      projectId,
      instanceId,
      resolve
    }
  }
}

export function topicsLoaded (result, resolve) {
  return {
    type: LOAD_TOPICS_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function editTopics (projectId, streamId, values, resolve) {
  return {
    type: EDIT_TOPICS,
    payload: {
      projectId,
      streamId,
      values,
      resolve
    }
  }
}

export function topicsEdited (result, resolve) {
  return {
    type: EDIT_TOPICS_SUCCESS,
    payload: {
      result,
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

export function logsInfoLoaded (result, resolve) {
  return {
    type: LOAD_LOGS_INFO_SUCCESS,
    payload: {
      result,
      resolve
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

export function adminLogsInfoLoaded (result, resolve) {
  return {
    type: LOAD_ADMIN_LOGS_INFO_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function addStream (stream, resolve, reject) {
  return {
    type: ADD_STREAMS,
    payload: {
      stream,
      resolve,
      reject
    }
  }
}

export function streamAdded (result, resolve, reject) {
  return {
    type: ADD_STREAMS_SUCCESS,
    payload: {
      result,
      resolve,
      reject
    }
  }
}

export function loadSingleStream (projectId, streamId, resolve) {
  return {
    type: LOAD_SINGLE_STREAM,
    payload: {
      projectId,
      streamId,
      resolve
    }
  }
}

export function singleStreamLoaded (result, resolve) {
  return {
    type: LOAD_SINGLE_STREAM_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function editStream (stream, resolve, reject) {
  return {
    type: EDIT_STREAM,
    payload: {
      stream,
      resolve,
      reject
    }
  }
}

export function streamEdited (result, resolve, reject) {
  return {
    type: EDIT_STREAM_SUCCESS,
    payload: {
      result,
      resolve,
      reject
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

export function streamOperated (result, resolve, reject) {
  return {
    type: OPERATE_STREAMS_SUCCESS,
    payload: {
      result,
      resolve,
      reject
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

export function streamStartOrRenewed (result, resolve, reject) {
  return {
    type: STARTORRENEW_STREAMS_SUCCESS,
    payload: {
      result,
      resolve,
      reject
    }
  }
}

export function streamOperatedError (message, reject) {
  return {
    type: OPERATE_STREAMS_ERROR,
    payload: {
      message,
      reject
    }
  }
}
