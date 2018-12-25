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

import { createSelector } from 'reselect'

const selectFlow = () => (state) => state.get('flow')
const selectStream = () => (state) => state.get('stream')

const selectFlows = () => createSelector(
  selectFlow(),
  (flowState) => flowState.get('flows')
)

const selectError = () => createSelector(
  selectFlow(),
  (flowState) => flowState.get('error')
)

const selectFlowSubmitLoading = () => createSelector(
  selectFlow(),
  (flowState) => flowState.get('flowSubmitLoading')
)

const selectFlowStartModalLoading = () => createSelector(
  selectFlow(),
  (flowState) => flowState.get('flowStartModalLoading')
)
const selectStreamFilterId = () => createSelector(
  selectStream(),
  (streamState) => streamState.get('streamFilterId')
)
export {
  selectFlow,
  selectFlows,
  selectError,
  selectFlowSubmitLoading,
  selectFlowStartModalLoading,
  selectStreamFilterId
}
