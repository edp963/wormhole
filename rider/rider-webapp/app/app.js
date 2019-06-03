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
 * app.js
 *
 * This is the entry file for the application, only setup and boilerplate
 * code.
 */

// Import CSS reset and Global Styles
import './assets/less/main.less'

// import './assets/fonts/iconfont.css'

// Needed for redux-saga es6 generator support
import 'babel-polyfill'

/* eslint-disable import/no-unresolved, import/extensions */
// Load the favicon, the manifest.json file and the .htaccess file
import 'file-loader?name=[name].[ext]!./favicon.ico'
// import '!file-loader?name=[name].[ext]!./manifest.json'
import 'file-loader?name=[name].[ext]!./.htaccess'
/* eslint-enable import/no-unresolved, import/extensions */

// Import all the third party stuff
import React from 'react'
import ReactDOM from 'react-dom'
import { Provider } from 'react-redux'
import { applyRouterMiddleware, Router, hashHistory } from 'react-router'
import { syncHistoryWithStore } from 'react-router-redux'
import { useScroll } from 'react-router-scroll'
import configureStore from './store'

// Import Language Provider
import LanguageProvider from 'containers/LanguageProvider'

// Import i18n messages
import { translationMessages } from './i18n'

// Create redux store with history
// this uses the singleton hashHistory provided by react-router
// Optionally, this could be changed to leverage a created history
// e.g. `const hashHistory = useRouterHistory(createhashHistory)()`
const initialState = {}
const store = configureStore(initialState, hashHistory)

// Sync history and store, as the react-router-redux reducer
// is under the non-default key ("routing"), selectLocationState
// must be provided for resolving how to retrieve the "route" in the state
import { selectLocationState } from 'containers/App/selectors'
const history = syncHistoryWithStore(hashHistory, store, {
  selectLocationState: selectLocationState()
})

// Set up the router, wrapping all Routes in the App component
import App from 'containers/App'
import createRoutes from './routes'
const rootRoute = {
  path: '/',
  component: App,
  indexRoute: {
    onEnter: (_, replace) => {
      replace('/login')
    }
  },
  childRoutes: createRoutes(store)
}

const render = (messages) => {
  ReactDOM.render(
    <Provider store={store}>
      <LanguageProvider messages={messages}>
        <Router
          history={history}
          routes={rootRoute}
          render={
            // Scroll to top when going to a new page, imitating default browser
            // behaviour
            applyRouterMiddleware(useScroll())
          }
        />
      </LanguageProvider>
    </Provider>,
    document.getElementById('app')
  )
}

// Hot reloadable translation json files
if (module.hot) {
  // modules.hot.accept does not accept dynamic dependencies,
  // have to be constants at compile-time
  module.hot.accept('./i18n', () => {
    render(translationMessages)
  })
}

// Chunked polyfill for browsers without Intl support
if (!window.Intl) {
  (new Promise((resolve) => {
    resolve(System.import('intl'))
  }))
    .then(() => Promise.all([
      System.import('intl/locale-data/jsonp/en.js'),
      System.import('intl/locale-data/jsonp/zh.js')
    ]))
    .then(() => render(translationMessages))
    .catch((err) => {
      throw err
    })
} else {
  render(translationMessages)
}

// Install ServiceWorker and AppCache in the end since
// it's not most important operation and if main code fails,
// we do not want it installed
// if (process.env.NODE_ENV === 'production') {
//   require('offline-plugin/runtime').install() // eslint-disable-line global-require
// }
