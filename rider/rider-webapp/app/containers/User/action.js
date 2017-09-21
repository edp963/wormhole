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
  LOAD_ADMIN_ALL_USERS,
  LOAD_ADMIN_ALL_USERS_SUCCESS,
  LOAD_USER_USERS,
  LOAD_USER_USERS_SUCCESS,
  LOAD_SELECT_USERS,
  LOAD_SELECT_USERS_SUCCESS,
  ADD_USER,
  ADD_USER_SUCCESS,
  EDIT_USER,
  EDIT_USER_SUCCESS,
  LOAD_EMAIL_INPUT_VALUE,
  LOAD_EMAIL_INPUT_VALUE_SUCCESS,
  LOAD_EMAIL_INPUT_VALUE_ERROR,
  EDIT_ROLETYPE_USERPSW,
  EDIT_ROLETYPE_USERPSW_SUCCESS,
  EDIT_ROLETYPE_USERPSW_ERROR,

  LOAD_PROJECT_USER_ALL,
  LOAD_PROJECT_USER_ALL_SUCCESS,
  GET_ERROR
} from './constants'

export function loadAdminAllUsers (resolve) {
  return {
    type: LOAD_ADMIN_ALL_USERS,
    payload: {
      resolve
    }
  }
}

export function adminAllUsersLoaded (users, resolve) {
  return {
    type: LOAD_ADMIN_ALL_USERS_SUCCESS,
    payload: {
      users,
      resolve
    }
  }
}

export function loadUserUsers (projectId, resolve) {
  return {
    type: LOAD_USER_USERS,
    payload: {
      projectId,
      resolve
    }
  }
}

export function userUsersLoaded (users, resolve) {
  return {
    type: LOAD_USER_USERS_SUCCESS,
    payload: {
      users,
      resolve
    }
  }
}

export function loadSelectUsers (projectId, resolve) {
  return {
    type: LOAD_SELECT_USERS,
    payload: {
      projectId,
      resolve
    }
  }
}

export function selectUsersLoaded (users, resolve) {
  return {
    type: LOAD_SELECT_USERS_SUCCESS,
    payload: {
      users,
      resolve
    }
  }
}

export function addUser (user, resolve) {
  return {
    type: ADD_USER,
    payload: {
      user,
      resolve
    }
  }
}

export function userAdded (result, resolve) {
  return {
    type: ADD_USER_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function editUser (user, resolve) {
  return {
    type: EDIT_USER,
    payload: {
      user,
      resolve
    }
  }
}

export function userEdited (result, resolve) {
  return {
    type: EDIT_USER_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function loadEmailInputValue (value, resolve, reject) {
  return {
    type: LOAD_EMAIL_INPUT_VALUE,
    payload: {
      value,
      resolve,
      reject
    }
  }
}

export function emailInputValueLoaded (result, resolve) {
  return {
    type: LOAD_EMAIL_INPUT_VALUE_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function emailInputValueErrorLoaded (result, reject) {
  return {
    type: LOAD_EMAIL_INPUT_VALUE_ERROR,
    payload: {
      result,
      reject
    }
  }
}

export function editroleTypeUserPsw (pwdValues, resolve, reject) {
  return {
    type: EDIT_ROLETYPE_USERPSW,
    payload: {
      pwdValues,
      resolve,
      reject
    }
  }
}

export function roleTypeUserPswEdited (result, resolve) {
  return {
    type: EDIT_ROLETYPE_USERPSW_SUCCESS,
    payload: {
      result,
      resolve
    }
  }
}

export function roleTypeUserPswErrorEdited (result, reject) {
  return {
    type: EDIT_ROLETYPE_USERPSW_ERROR,
    payload: {
      result,
      reject
    }
  }
}

export function loadProjectUserAll (resolve) {
  return {
    type: LOAD_PROJECT_USER_ALL,
    payload: {
      resolve
    }
  }
}

export function projectUserAllLoaded (result, resolve) {
  return {
    type: LOAD_PROJECT_USER_ALL_SUCCESS,
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
