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
 * input/Input placeholder change language
 */

import React from 'react'
import { injectIntl, intlShape } from 'react-intl'
import Input from 'antd/lib/input'

const PlaceholderInputIntl = ({ intl: { formatMessage }, idValue, disabledValue, onChangeEvent }) => {
  const placeholder = formatMessage({ id: idValue })
  return (
    <Input
      placeholder={placeholder}
      disabled={disabledValue}
      onChange={onChangeEvent}
    />
  )
}

PlaceholderInputIntl.propTypes = {
  intl: intlShape.isRequired,
  idValue: React.PropTypes.string,
  disabledValue: React.PropTypes.bool,
  onChangeEvent: React.PropTypes.func
}

export default injectIntl(PlaceholderInputIntl)
