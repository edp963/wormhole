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
  LOAD_ADMIN_ALL_NAMESPACES,
  LOAD_ADMIN_ALL_NAMESPACES_SUCCESS,
  LOAD_USER_NAMESPACES,
  LOAD_USER_NAMESPACES_SUCCESS,
  LOAD_SELECT_NAMESPACES,
  LOAD_SELECT_NAMESPACES_SUCCESS,
  LOAD_NAMESPACE_DATABASE,
  LOAD_NAMESPACE_DATABASE_SUCCESS,
  LOAD_TABLE_NAME_EXIST,
  LOAD_TABLE_NAME_EXIST_SUCCESS,
  LOAD_TABLE_NAME_EXIST_ERROR,
  ADD_NAMESPACE,
  ADD_NAMESPACE_SUCCESS,
  LOAD_SINGLE_NAMESPACE,
  LOAD_SINGLE_NAMESPACE_SUCCESS,
  EDIT_NAMESPACE,
  EDIT_NAMESPACE_SUCCESS,

  LOAD_PROJECT_NS_ALL,
  LOAD_PROJECT_NS_ALL_SUCCESS,
  GET_ERROR
} from './constants'

export function loadAdminAllNamespaces (resolve) {
  return {
    type: LOAD_ADMIN_ALL_NAMESPACES,
    payload: {
      resolve
    }
  }
}

export function adminAllNamespacesLoaded (namespaces, resolve) {
  return {
    type: LOAD_ADMIN_ALL_NAMESPACES_SUCCESS,
    payload: {
      namespaces,
      resolve
    }
  }
}

export function loadSelectNamespaces (projectId, resolve) {
  return {
    type: LOAD_SELECT_NAMESPACES,
    payload: {
      projectId,
      resolve
    }
  }
}

export function selectNamespacesLoaded (namespaces, resolve) {
  return {
    type: LOAD_SELECT_NAMESPACES_SUCCESS,
    payload: {
      namespaces,
      resolve
    }
  }
}

export function loadUserNamespaces (projectId, resolve) {
  return {
    type: LOAD_USER_NAMESPACES,
    payload: {
      projectId,
      resolve
    }
  }
}

export function userNamespacesLoaded (namespaces, resolve) {
  return {
    type: LOAD_USER_NAMESPACES_SUCCESS,
    payload: {
      namespaces,
      resolve
    }
  }
}

export function loadNamespaceDatabase (instanceId, resolve) {
  return {
    type: LOAD_NAMESPACE_DATABASE,
    payload: {
      instanceId,
      resolve
    }
  }
}

export function namespaceDatabaseLoaded (database, resolve) {
  return {
    type: LOAD_NAMESPACE_DATABASE_SUCCESS,
    payload: {
      database,
      resolve
    }
  }
}

export function loadTableNameExist (value, resolve, reject) {
  return {
    type: LOAD_TABLE_NAME_EXIST,
    payload: {
      value,
      resolve,
      reject
    }
  }
}

export function tableNameExistLoaded (result, resolve) {
  return {
    type: LOAD_TABLE_NAME_EXIST_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function tableNameExistErrorLoaded (result, reject) {
  return {
    type: LOAD_TABLE_NAME_EXIST_ERROR,
    payload: {
      result,
      reject
    }
  }
}

export function addNamespace (value, resolve) {
  return {
    type: ADD_NAMESPACE,
    payload: {
      value,
      resolve
    }
  }
}

export function namespaceAdded (result, resolve) {
  return {
    type: ADD_NAMESPACE_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function loadSingleNamespace (namespaceId, resolve) {
  return {
    type: LOAD_SINGLE_NAMESPACE,
    payload: {
      namespaceId,
      resolve
    }
  }
}

export function singleNamespaceLoaded (result, resolve) {
  return {
    type: LOAD_SINGLE_NAMESPACE_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function editNamespace (value, resolve) {
  return {
    type: EDIT_NAMESPACE,
    payload: {
      value,
      resolve
    }
  }
}

export function namespaceEdited (result, resolve) {
  return {
    type: EDIT_NAMESPACE_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function loadProjectNsAll (resolve) {
  return {
    type: LOAD_PROJECT_NS_ALL,
    payload: {
      resolve
    }
  }
}

export function projectNsAllLoaded (result, resolve) {
  return {
    type: LOAD_PROJECT_NS_ALL_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function getError (error) {
  return {
    type: GET_ERROR,
    payload: {
      error
    }
  }
}
