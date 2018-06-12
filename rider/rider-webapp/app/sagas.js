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

import AppSagas from './containers/App/sagas'

import ProjectSagas from './containers/Project/sagas'
import FlowSagas from './containers/Flow/sagas'
import StreamSagas from './containers/Manager/sagas'
import JobSagas from './containers/Job/sagas'
import NamespaceSagas from './containers/Namespace/sagas'
import UserSagas from './containers/User/sagas'
import UdfSagas from './containers/Udf/sagas'
import RiderInfoSagas from './containers/RiderInfo/sagas'

import InstanceSagas from './containers/Instance/sagas'
import DatabaseSagas from './containers/DataBase/sagas'

import WorkbenchSagas from './containers/Workbench/sagas'
import ResourceSagas from './containers/Resource/sagas'
import PerformanceSagas from './containers/Performance/sagas'

export default [
  ...AppSagas,

  ...ProjectSagas,
  ...FlowSagas,
  ...StreamSagas,
  ...JobSagas,
  ...NamespaceSagas,
  ...UserSagas,
  ...UdfSagas,
  ...RiderInfoSagas,

  ...InstanceSagas,
  ...DatabaseSagas,

  ...WorkbenchSagas,
  ...ResourceSagas,
  ...PerformanceSagas
]
