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

/*
 * this component connects the redux state language locale to the
 * IntlProvider component and i18n messages (loaded from `app/translations`)
 */

import React from 'react'
import PropTypes from 'prop-types'
import { connect } from 'react-redux'
import { createSelector } from 'reselect'
import { IntlProvider } from 'react-intl'
import { selectLocale } from './selectors'

import LocaleProvider from 'antd/lib/locale-provider' // 全局生效
import enUS from 'antd/lib/locale-provider/en_US' // 英文

export class LanguageProvider extends React.PureComponent { // eslint-disable-line react/prefer-stateless-function
  render () {
    const { locale } = this.props
    let languageGlobal = {}
    if (locale === 'en') {
      languageGlobal = enUS
    }
    return (
      <LocaleProvider locale={languageGlobal}>
        <IntlProvider
          locale={locale}
          key={locale}
          messages={this.props.messages[locale]}
        >
          {React.Children.only(this.props.children)}
        </IntlProvider>
      </LocaleProvider>
    )
  }
}

LanguageProvider.propTypes = {
  locale: PropTypes.string,
  messages: PropTypes.object,
  children: PropTypes.element.isRequired
}

const mapStateToProps = createSelector(
  selectLocale(),
  (locale) => ({ locale })
)

export default connect(mapStateToProps)(LanguageProvider)
