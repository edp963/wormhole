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

/**
 * Combine all reducers in this file and export the combined reducers.
 * If we were to do this in store.js, reducers wouldn't be hot reloadable.
 */

import { fromJS } from 'immutable'
import { combineReducers } from 'redux-immutable'
import { LOCATION_CHANGE } from 'react-router-redux'

// import globalReducer from 'containers/App/reducer'
import languageProviderReducer from 'containers/LanguageProvider/reducer'
import appReducer from 'containers/App/reducer'

import projectReducer from 'containers/Project/reducer'
import flowReducer from 'containers/Flow/reducer'
import streamReducer from 'containers/Manager/reducer'
import jobReducer from 'containers/Job/reducer'
import namespaceReducer from 'containers/Namespace/reducer'
import userReducer from 'containers/User/reducer'
import udfReducer from 'containers/Udf/reducer'
import riderInfoReducer from 'containers/RiderInfo/reducer'

import instanceReducer from 'containers/Instance/reducer'
import databaseReducer from 'containers/DataBase/reducer'

import workbenchReducer from 'containers/Workbench/reducer'
import resourceReducer from 'containers/Resource/reducer'
import performanceReducer from 'containers/Performance/reducer'

/*
 * routeReducer
 *
 * The reducer merges route location changes into our immutable state.
 * The change is necessitated by moving to react-router-redux@4
 *
 */

// Initial routing state
const routeInitialState = fromJS({
  locationBeforeTransitions: null
})

/**
 * Merge route into the global application state
 */
function routeReducer (state = routeInitialState, action) {
  switch (action.type) {
    /* istanbul ignore next */
    case LOCATION_CHANGE:
      return state.merge({
        locationBeforeTransitions: action.payload
      })
    default:
      return state
  }
}

/**
 * Creates the main reducer with the asynchronously loaded ones
 */
export default function createReducer (asyncReducers) {
  return (state, action) => {
    if (action.type === 'LOG_OUT') {
      state = undefined
    }
    return combineReducers({
      route: routeReducer,
      global: appReducer,
      language: languageProviderReducer,
      login: appReducer,

      project: projectReducer,
      flow: flowReducer,
      stream: streamReducer,
      job: jobReducer,
      namespace: namespaceReducer,
      user: userReducer,
      udf: udfReducer,
      riderInfo: riderInfoReducer,

      instance: instanceReducer,
      database: databaseReducer,

      workbench: workbenchReducer,
      resource: resourceReducer,
      performance: performanceReducer,

      ...asyncReducers
    })(state, action)
  }
}
