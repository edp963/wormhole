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
 * This will setup the i18n language files and locale data for your app.
 */
import { addLocaleData } from 'react-intl'
import { DEFAULT_LOCALE } from '../app/containers/App/constants'

import enLocaleData from 'react-intl/locale-data/en'
import zhLocaleData from 'react-intl/locale-data/zh'

addLocaleData(enLocaleData)
addLocaleData(zhLocaleData)

export const appLocales = [
  'en',
  'zh'
]

import enTranslationMessages from './translations/en.json'
import zhTranslationMessages from './translations/zh.json'

export const formatTranslationMessages = (locale, messages) => {
  const defaultFormattedMessages = locale !== DEFAULT_LOCALE
    ? formatTranslationMessages(DEFAULT_LOCALE, enTranslationMessages)
    : {}
  const formattedMessages = {}
  const messageKeys = Object.keys(messages)
  for (const messageKey of messageKeys) {
    if (locale === DEFAULT_LOCALE) {
      formattedMessages[messageKey] = messages[messageKey]
    } else {
      formattedMessages[messageKey] = messages[messageKey] || defaultFormattedMessages[messageKey]
    }
  }

  return formattedMessages
}

export const translationMessages = {
  en: formatTranslationMessages('en', enTranslationMessages),
  zh: formatTranslationMessages('zh', zhTranslationMessages)
}
