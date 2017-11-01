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

// These are the pages you can go to.
// They are all wrapped in the App component, which should contain the navbar etc
// See http://blog.mxstbr.com/2016/01/react-apps-with-pages for more information
// about the code splitting business
// import { getAsyncInjectors } from './utils/asyncInjectors'

import Login from './containers/Login'
import SecondNavigatorContainer from './containers/SecondNavigatorContainer'

import Project from './containers/Project'
import Flow from './containers/Flow'
import Manager from './containers/Manager'
import Namespace from './containers/Namespace'
import Instance from './containers/Instance'
import DataBase from './containers/DataBase'
import User from './containers/User'
import Udf from './containers/Udf'
import RiderInfo from './containers/RiderInfo'

import Workbench from './containers/Workbench'
import Performance from './containers/Performance'
import NotFound from './containers/NotFoundPage'

export default function createRoutes (store) {
  return [
    {
      path: 'login',
      name: 'login',
      components: Login
    }, {
      path: 'projects',
      name: 'projects',
      components: Project
    }, {
      path: 'flows',
      name: 'flows',
      components: Flow
    }, {
      path: 'streams',
      name: 'streams',
      components: Manager
    }, {
      path: 'namespaces',
      name: 'namespaces',
      components: Namespace
    }, {
      path: 'instance',
      name: 'instance',
      components: Instance
    }, {
      path: 'database',
      name: 'database',
      components: DataBase
    }, {
      path: 'users',
      name: 'users',
      components: User
    }, {
      path: 'udf',
      name: 'udf',
      components: Udf
    }, {
      path: 'riderInfo',
      name: 'riderInfo',
      components: RiderInfo
    }, {
      path: 'project/:projectId',
      components: SecondNavigatorContainer,
      indexRoute: {
        onEnter: (_, replace) => {
          replace(`/project/${_.params.projectId}/workbench`)
        }
      },
      childRoutes: [
        {
          path: 'workbench',
          name: 'workbench',
          components: Workbench
        }, {
          path: 'performance',
          name: 'performance',
          components: Performance
        }, {
          path: '*',
          name: 'notfound',
          indexRoute: {
            onEnter: (_, replace) => {
              replace(`/project/${_.params.projectId}/workbench`)
            }
          }
        }
      ]
    }, {
      path: '*',
      name: 'notfound',
      components: NotFound
    }
  ]
}
