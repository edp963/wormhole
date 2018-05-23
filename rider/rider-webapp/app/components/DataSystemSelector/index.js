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

import React from 'react'
import PropTypes from 'prop-types'

export class DataSystemSelector extends React.PureComponent {
  componentWillReceiveProps (props) {
    const { onItemSelect } = this.props
    if (this.props.value !== props.value) {
      if (onItemSelect) {
        onItemSelect(props.value)
      }
    }
  }

  itemClick = (i) => () => {
    const { dataSystemDisabled, onChange, onItemSelect } = this.props
    if (!dataSystemDisabled) {
      onChange(i.value)
      if (onItemSelect) {
        onItemSelect(i.value)
      }
    }
  }

  render () {
    const data = this.props.data || []
    const items = data.map(i => {
      const content = i.icon
        ? <i className={`iconfont ${i.icon}`} style={i.style} />
        : i.text
      return (
        <li
          key={i.value}
          className={`${i.value === this.props.value ? 'active' : ''} ${this.props.dataSystemDisabled === true ? 'dataSystem-style' : ''}`}
          onClick={this.itemClick(i)}>
          {content}
        </li>
      )
    })

    return (
      <ul className="ri-datasystem-selector clearfix">
        {items}
      </ul>
    )
  }
}

DataSystemSelector.propTypes = {
  data: PropTypes.array,
  value: PropTypes.string,
  onItemSelect: PropTypes.func,
  onChange: PropTypes.func,
  dataSystemDisabled: PropTypes.bool
}

export default DataSystemSelector
