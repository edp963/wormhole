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
  LOAD_NORMAL,
  LOAD_NORMAL_SUCCESS,
  LOAD_SELECT_USERS,
  LOAD_SELECT_USERS_SUCCESS,
  ADD_USER,
  ADD_USER_SUCCESS,
  EDIT_USER,
  EDIT_USER_SUCCESS,
  EDIT_NORMAL,
  EDIT_NORMAL_SUCCESS,
  LOAD_EMAIL_INPUT_VALUE,
  LOAD_EMAIL_INPUT_VALUE_SUCCESS,
  LOAD_EMAIL_INPUT_VALUE_ERROR,
  EDIT_ROLETYPE_USERPSW,
  EDIT_ROLETYPE_USERPSW_SUCCESS,
  EDIT_ROLETYPE_USERPSW_ERROR,
  LOAD_USER_DETAIL,
  LOAD_USER_DETAIL_SUCCESS,
  DELETE_USER,
  DELETE_USER_SUCCESS,
  DELETE_USER_ERROR,

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

export function adminAllUsersLoaded (users) {
  return {
    type: LOAD_ADMIN_ALL_USERS_SUCCESS,
    payload: {
      users
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

export function userUsersLoaded (users) {
  return {
    type: LOAD_USER_USERS_SUCCESS,
    payload: {
      users
    }
  }
}

export function loadNormalDetail (userId, resolve) {
  return {
    type: LOAD_NORMAL,
    payload: {
      userId,
      resolve
    }
  }
}

export function normalDetailLoaded (users) {
  return {
    type: LOAD_NORMAL_SUCCESS,
    payload: {
      users
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

export function selectUsersLoaded (users) {
  return {
    type: LOAD_SELECT_USERS_SUCCESS,
    payload: {
      users
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

export function userAdded (result) {
  return {
    type: ADD_USER_SUCCESS,
    payload: {
      result
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

export function userEdited (result) {
  return {
    type: EDIT_USER_SUCCESS,
    payload: {
      result
    }
  }
}

export function editNormal (value, resolve) {
  return {
    type: EDIT_NORMAL,
    payload: {
      value,
      resolve
    }
  }
}

export function normalEdited (result) {
  return {
    type: EDIT_NORMAL_SUCCESS,
    payload: {
      result
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

export function emailInputValueLoaded (result) {
  return {
    type: LOAD_EMAIL_INPUT_VALUE_SUCCESS,
    payload: {
      result
    }
  }
}

export function emailInputValueErrorLoaded (result) {
  return {
    type: LOAD_EMAIL_INPUT_VALUE_ERROR,
    payload: {
      result
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

export function roleTypeUserPswEdited (result) {
  return {
    type: EDIT_ROLETYPE_USERPSW_SUCCESS,
    payload: {
      result
    }
  }
}

export function roleTypeUserPswErrorEdited (result) {
  return {
    type: EDIT_ROLETYPE_USERPSW_ERROR,
    payload: {
      result
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

export function projectUserAllLoaded (result) {
  return {
    type: LOAD_PROJECT_USER_ALL_SUCCESS,
    payload: {
      result
    }
  }
}

export function loadUserDetail (userId, resolve) {
  return {
    type: LOAD_USER_DETAIL,
    payload: {
      userId,
      resolve
    }
  }
}

export function userDetailLoaded (result) {
  return {
    type: LOAD_USER_DETAIL_SUCCESS,
    payload: {
      result
    }
  }
}

export function deleteUser (userId, resolve, reject) {
  return {
    type: DELETE_USER,
    payload: {
      userId,
      resolve,
      reject
    }
  }
}

export function userDeleted (result) {
  return {
    type: DELETE_USER_SUCCESS,
    payload: {
      result
    }
  }
}

export function userDeletedError (result) {
  return {
    type: DELETE_USER_ERROR,
    payload: {
      result
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
