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
  LOAD_PROJECTS,
  LOAD_PROJECTS_SUCCESS,
  LOAD_USER_PROJECTS,
  LOAD_USER_PROJECTS_SUCCESS,
  LOAD_SINGLE_PROJECT,
  LOAD_SINGLE_PROJECT_SUCCESS,
  ADD_PROJECT,
  ADD_PROJECT_SUCCESS,
  EDIT_PROJECT,
  EDIT_PROJECT_SUCCESS,
  LOAD_PROJECT_NAME_VALUE,
  LOAD_PROJECT_NAME_VALUE_SUCCESS,
  LOAD_PROJECT_NAME_VALUE_ERROR,
  DELETE_SINGLE_PROJECT,
  DELETE_SINGLE_PROJECT_SUCCESS,
  DELETE_SINGLE_PROJECT_ERROR,
  GET_ERROR
} from './constants'

export function loadProjects (visible) {
  return {
    type: LOAD_PROJECTS,
    payload: {
      visible
    }
  }
}

export function projectsLoaded (projects) {
  return {
    type: LOAD_PROJECTS_SUCCESS,
    payload: {
      projects
    }
  }
}

export function loadUserProjects () {
  return {
    type: LOAD_USER_PROJECTS
  }
}

export function userProjectsLoaded (projects) {
  return {
    type: LOAD_USER_PROJECTS_SUCCESS,
    payload: {
      projects
    }
  }
}

export function loadSingleProject (projectId, resolve) {
  return {
    type: LOAD_SINGLE_PROJECT,
    payload: {
      projectId,
      resolve
    }
  }
}

export function singleProjectLoaded (result, resolve) {
  return {
    type: LOAD_SINGLE_PROJECT_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function addProject (project, resolve, final) {
  return {
    type: ADD_PROJECT,
    payload: {
      project,
      resolve,
      final
    }
  }
}

export function projectAdded (result, resolve, final) {
  return {
    type: ADD_PROJECT_SUCCESS,
    payload: {
      result,
      resolve,
      final
    }
  }
}

export function editProject (project, resolve, final) {
  return {
    type: EDIT_PROJECT,
    payload: {
      project,
      resolve,
      final
    }
  }
}

export function projectEdited (result, resolve, final) {
  return {
    type: EDIT_PROJECT_SUCCESS,
    payload: {
      result,
      resolve,
      final
    }
  }
}

export function loadProjectNameInputValue (value, resolve, reject) {
  return {
    type: LOAD_PROJECT_NAME_VALUE,
    payload: {
      value,
      resolve,
      reject
    }
  }
}

export function projectNameInputValueLoaded (result, resolve) {
  return {
    type: LOAD_PROJECT_NAME_VALUE_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function projectNameInputValueErrorLoaded (result, reject) {
  return {
    type: LOAD_PROJECT_NAME_VALUE_ERROR,
    payload: {
      result,
      reject
    }
  }
}

export function deleteSingleProject (projectId, resolve, reject) {
  return {
    type: DELETE_SINGLE_PROJECT,
    payload: {
      projectId,
      resolve,
      reject
    }
  }
}

export function singleProjectDeleted (result, resolve) {
  return {
    type: DELETE_SINGLE_PROJECT_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function singleProjectDeletedError (result, reject) {
  return {
    type: DELETE_SINGLE_PROJECT_ERROR,
    payload: {
      result,
      reject
    }
  }
}

export function getError (final) {
  return {
    type: GET_ERROR,
    payload: {
      final
    }
  }
}
