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

import React, { PropTypes, PureComponent } from 'react'
import classnames from 'classnames'

import Form from 'antd/lib/form'
import Input from 'antd/lib/input'
import Select from 'antd/lib/select'
import Radio from 'antd/lib/radio'
import Button from 'antd/lib/button'
import Icon from 'antd/lib/icon'
const Option = Select.Option
const FormItem = Form.Item
const RadioGroup = Radio.Group
const RadioButton = Radio.Button

import { uuid } from '../../../utils/util'

const operators = [
  ['=', '>', '<', '>=', '<=', '!=', 'like', 'startWith', 'endWith']
]
const operatorsMap = {
  '=': 'equiv',
  '>': 'gt',
  '<': 'lt',
  '>=': 'gteq',
  '<=': 'lteq',
  '!=': 'neq',
  'startWith': 'startwith',
  'endWith': 'endwith',
  'startwith': 'startWith',
  'endwith': 'endWith',
  'like': 'like',
  'equiv': '=',
  'gt': '>',
  'lt': '<',
  'gteq': '>=',
  'lteq': '<=',
  'neq': '!='
}
export class DashboardItemFilters extends PureComponent {
  constructor (props) {
    super(props)
    this.state = {
      filterTree: {},
      flattenTree: null
    }
  }

  componentWillMount () {
    this.initTree()
  }
  componentWillReceiveProps (nextProps) {
    if (nextProps.isOpen !== this.props.isOpen) {
      this.initTree(nextProps)
    }
    // if ('value' in nextProps) {
    //   const value = nextProps.value
    //   this.setState({value})
    // }
  }
  componentWillUpdate (nextProps) {
    if (nextProps.sourceData && nextProps.sourceData !== this.props.sourceData) {
      this.initTree(nextProps)
    }
  }

  componentDidMount () {
    this.props.onRef(this)
    this.initTree(this.props)
  }

  initTree = (props) => {
    if (!props) return
    const { sourceData } = props
    let parsedData = JSON.parse(sourceData)
    if (sourceData) {
      const filterTree = this.translateData(parsedData)
      const flattenTree = this.initFlattenTree(filterTree, {})
      this.setState({
        filterTree,
        flattenTree
      }, () => {
        this.reSetFormValue(filterTree)
        window.getValue = this.props.form
      })
    }
  }

  initFlattenTree = (tree, flatten) => {
    flatten[tree.id] = tree
    if (tree.children) {
      tree.children.forEach(c => {
        this.initFlattenTree(c, flatten)
      })
    }
    return flatten
  }

  renderFilterList = (filter, items) => {
    const { getFieldDecorator } = this.props.form
    const itemClass = classnames({
      filterItem: true,
      noPadding: true,
      root: filter.root
    })

    return (
      <div key={filter.id} className={itemClass}>
        <div className={`filterBlock`}>
          <div className={`filterRel`}>
            <FormItem className={`filterFormItem`}>
              {getFieldDecorator(`${filter.id}Rel`, {
                initialValue: filter.rel
              })(
                <RadioGroup onChange={this.changeLinkRel(filter)}>
                  <RadioButton value="and">And</RadioButton>
                  <RadioButton value="or">Or</RadioButton>
                </RadioGroup>
              )}
            </FormItem>
          </div>
          <list className={`filterList`}>
            {items}
          </list>
        </div>
      </div>
    )
  }

  renderFilterItem = (filter) => {
    const {
      form
    } = this.props

    const { getFieldDecorator } = form

    const itemClass = classnames({
      filterItem: true,
      root: filter.root
    })

    const forkButton = filter.root || (
      <Button shape="circle" icon="fork" type="primary" onClick={this.forkNode(filter.id)} />
    )

    const operatorSelectOptions = this.generateFilterOperatorOptions()

    const valueInput = this.generateFilterValueInput(filter)

    const keyValueInput = this.generateFilterKeyValueInput(filter)

    return (
      <div className={itemClass} key={filter.id}>
        <FormItem className={`filterFormItem filterFormKey`}>
          {getFieldDecorator(`${filter.id}KeySelect`, {
            rules: [{
              required: true,
              message: 'Name 不能为空'
            }],
            initialValue: filter.filterKey
          })(
            keyValueInput
          )}
        </FormItem>
        <FormItem className={`filterFormItem filterFormOperator`}>
          {getFieldDecorator(`${filter.id}OperatorSelect`, {
            rules: [{
              required: true,
              message: 'Operator 不能为空'
            }],
            initialValue: filter.filterOperator
          })(
            <Select onSelect={this.changeFilterOperator(filter)}>
              {operatorSelectOptions}
            </Select>
          )}
        </FormItem>
        <FormItem className={`filterFormItem`}>
          {getFieldDecorator(`${filter.id}Input`, {
            rules: [{
              required: true,
              message: 'Value 不能为空'
            }],
            initialValue: filter.filterValue || null
          })(
            valueInput
          )}
        </FormItem>
        <Button shape="circle" icon="plus" type="primary" onClick={this.addParallelNode(filter.id)} />
        {forkButton}
        <Button shape="circle" icon="minus" onClick={this.deleteNode(filter.id)} />
      </div>
    )
  }

  renderFilters (filter) {
    if (filter.type === 'link') {
      const items = filter.children.map(c => this.renderFilters(c))
      return this.renderFilterList(filter, items)
    } else if (filter.type === 'node') {
      return this.renderFilterItem(filter)
    } else {
      return (
        <div className={`empty`} onClick={this.addTreeRoot}>
          <h3>
            <Icon type="plus" /> 点击添加查询条件
          </h3>
        </div>
      )
    }
  }

  generateFilterKeyValueInput = (filter) => {
    const stringInput = (
      <Input placeholder="Field Name" onChange={this.changeStringFilterKeyValue(filter)} />
    )
    return stringInput
  }

  generateFilterOperatorOptions = () => {
    const numbersAndDateOptions = operators[0].slice().map(o => (
      <Option key={o} value={o}>{o}</Option>
    ))
    return numbersAndDateOptions
  }

  generateFilterValueInput = (filter) => {
    const stringInput = (
      <Input placeholder="Field Value" onChange={this.changeStringFilterValue(filter)} />
    )
    return stringInput
  }

  addTreeRoot = () => {
    const rootId = uuid(8, 16)
    const root = {
      id: rootId,
      root: true,
      type: 'node'
    }
    this.setState({
      filterTree: root,
      flattenTree: {
        [rootId]: root
      }
    })
  }

  addParallelNode = (nodeId) => () => {
    const { flattenTree } = this.state

    let currentNode = flattenTree[nodeId]
    let newNode = {
      id: uuid(8, 16),
      type: 'node'
    }

    if (currentNode.parent) {
      let parent = flattenTree[currentNode.parent]
      newNode.parent = parent.id
      parent.children.push(newNode)
      flattenTree[newNode.id] = newNode

      this.setState({
        flattenTree: Object.assign({}, flattenTree)
      })
    } else {
      let parent = {
        id: uuid(8, 16),
        root: true,
        type: 'link',
        rel: 'and',
        children: []
      }

      newNode.parent = parent.id
      parent.children.push(currentNode)
      parent.children.push(newNode)

      delete currentNode.root
      delete flattenTree[currentNode.id]
      currentNode.id = uuid(8, 16)
      currentNode.parent = parent.id

      flattenTree[currentNode.id] = currentNode
      flattenTree[parent.id] = parent
      flattenTree[newNode.id] = newNode

      this.setState({
        filterTree: parent,
        flattenTree: Object.assign({}, flattenTree)
      })
    }
  }

  forkNode = (nodeId) => () => {
    const { flattenTree } = this.state

    let currentNode = flattenTree[nodeId]
    let cloneNode = Object.assign({}, currentNode, {
      id: uuid(8, 16),
      parent: currentNode.id
    })
    let newNode = {
      id: uuid(8, 16),
      type: 'node',
      parent: currentNode.id
    }

    currentNode = Object.assign(currentNode, {
      type: 'link',
      rel: 'and',
      children: [cloneNode, newNode]
    })

    flattenTree[cloneNode.id] = cloneNode
    flattenTree[newNode.id] = newNode

    this.setState({
      flattenTree: Object.assign({}, flattenTree)
    })
  }

  deleteNode = (nodeId) => () => {
    const { flattenTree } = this.state

    let currentNode = flattenTree[nodeId]
    delete flattenTree[nodeId]

    if (currentNode.parent) {
      let parent = flattenTree[currentNode.parent]
      parent.children = parent.children.filter(c => c.id !== nodeId)

      if (parent.children.length === 1) {
        let onlyChild = parent.children[0]
        this.refreshTreeId(onlyChild)

        parent = Object.assign(parent, {
          id: onlyChild.id,
          type: onlyChild.type,
          rel: onlyChild.rel,
          filterKey: onlyChild.filterKey,
          filterOperator: onlyChild.filterOperator,
          filterValue: onlyChild.filterValue,
          children: onlyChild.children
        })

        delete flattenTree[parent.id]
        flattenTree[onlyChild.id] = parent
      }

      this.setState({
        flattenTree: Object.assign({}, flattenTree)
      })
    } else {
      this.setState({
        filterTree: {},
        flattenTree: {}
      }, () => {
        this.triggerChange('')
      })
    }
  }

  refreshTreeId = (treeNode) => {
    const { flattenTree } = this.state
    const oldId = treeNode.id
    delete flattenTree[oldId]

    treeNode.id = uuid(8, 16)
    flattenTree[treeNode.id] = treeNode

    if (treeNode.children) {
      treeNode.children.forEach(c => {
        c.parent = treeNode.id
        this.refreshTreeId(c)
      })
    }
  }

  changeLinkRel = (filter) => (e) => {
    filter.rel = e.target.value
  }

  changeStringFilterKeyValue = (filter) => (event) => {
    filter.filterKey = event.target.value
    this.triggerChange(filter)
  }

  changeFilterOperator = (filter) => (val) => {
    filter.filterOperator = val
    this.triggerChange(filter)
  }

  changeStringFilterValue = (filter) => (event) => {
    filter.filterValue = event.target.value
    this.triggerChange(filter)
  }

  changeNumberFilterValue = (filter) => (val) => {
    filter.filterValue = val
    this.triggerChange(filter)
  }

  changeDateFilterValue = (filter) => (date) => {
    filter.filterValue = date
    this.triggerChange(filter)
  }

  doQuery = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const { onQuery } = this.props
        const { filterTree } = this.state

        // onQuery(this.getSqlExpresstions(filterTree))
        onQuery(this.translateData(filterTree, false))
        this.resetTree()
      }
    })
  }

  resetTree = () => {
    this.setState({
      filterTree: {},
      flattenTree: null
    })
  }

  getSqlExpresstions = (tree) => {
    if (Object.keys(tree).length) {
      if (tree.type === 'link') {
        const partials = tree.children.map(c => {
          if (c.type === 'link') {
            return this.getSqlExpresstions(c)
          } else {
            return `${c.filterKey} ${c.filterOperator} ${c.filterValue}`
          }
        })
        const expressions = partials.join(` ${tree.rel} `)
        return `(${expressions})`
      } else {
        return `${tree.filterKey} ${tree.filterOperator} ${tree.filterValue}`
      }
    } else {
      return ''
    }
  }

  /**
   * pattern数据转换, isPare => true: 解析，false: 转译
   * @param {boolean}
   * @memberof DashboardItemFilters
   */
  translateData = (data, isParse = true) => {
    if (!data) return
    let myData = this.forEachFilterData(data, isParse)
    if (isParse) {
      let type = Object.prototype.toString.call(data.logic) === '[object Array]' ? 'link' : 'node'
      let filterTree = {
        root: true,
        id: uuid(8, 16),
        type
      }
      if (type === 'node') {
        filterTree.filterKey = data.logic && data.logic.field_name
        filterTree.filterOperator = data.logic && operatorsMap[data.logic.compare_type]
        filterTree.filterValue = data.logic && data.logic.value
      } else if (type === 'link') {
        filterTree.rel = data.operator
      }
      if (Object.prototype.toString.call(myData) === '[object Array]') {
        myData.forEach(v => {
          v.parent = filterTree.id
        })
        filterTree.children = myData
        this.forEachAddParent(filterTree)
      } else {
        filterTree = Object.assign({}, filterTree, myData)
      }
      return filterTree
    } else {
      if (data.root) {
        if (data.rel) {
          return {
            operator: data.rel,
            logic: myData
          }
        } else {
          return myData
        }
      } else {
        return myData
      }
    }
  }

  forEachFilterData = (data, isParse = true) => {
    if (!data) return
    if (isParse) {
      if (Object.keys(data).length > 0) {
        if (Object.prototype.toString.call(data.logic) === '[object Array]') {
          const filterTreeExcludeRoot = data.logic.map(v => {
            if (Object.prototype.toString.call(v.logic) === '[object Array]') {
              return {
                type: 'link',
                rel: v.operator,
                id: uuid(8, 16),
                children: this.forEachFilterData(v)
              }
            } else {
              return {
                type: 'node',
                id: uuid(8, 16),
                filterKey: v.logic.field_name,
                filterValue: v.logic.value,
                filterOperator: operatorsMap[v.logic.compare_type]
              }
            }
          })
          return filterTreeExcludeRoot
        } else {
          return {
            type: 'node',
            id: uuid(8, 16),
            filterKey: data.logic && data.logic.field_name,
            filterValue: data.logic && data.logic.value,
            filterOperator: data.logic && operatorsMap[data.logic.compare_type]
          }
        }
      }
    } else {
      if (Object.keys(data).length > 0) {
        if (data.type === 'link') {
          const logic = data.children.map(v => {
            if (v.type === 'link') {
              return {
                operator: v.rel,
                logic: this.forEachFilterData(v, false)
              }
            } else {
              return {
                operator: 'single',
                logic: {
                  field_name: v.filterKey,
                  compare_type: operatorsMap[v.filterOperator],
                  value: v.filterValue
                }
              }
            }
          })
          return logic
        } else {
          return {
            operator: 'single',
            logic: {
              field_name: data.filterKey,
              compare_type: operatorsMap[data.filterOperator],
              value: data.filterValue
            }
          }
        }
      }
    }
  }

  forEachAddParent = (filterData) => {
    if (!filterData) return
    filterData.children.forEach(v => {
      if (v.children && v.children.length > 0) {
        this.forEachAddParent(v)
      } else {
        v.parent = filterData.id
      }
    })
  }

  reSetFormValue = (data) => {
    const { setFieldsValue } = this.props.form
    if (data && data.length > 0) {
      if (data.type === 'link') {
        if (data.children && data.children.length > 0) {
          setFieldsValue({
            [`${data.id}Rel`]: data.rel
          })
          this.reSetFormValue(data.children)
        } else {
          setFieldsValue({
            [`${data.id}KeySelect`]: data.filterKey,
            [`${data.id}OperatorSelect`]: data.filterOperator,
            [`${data.id}Input`]: data.filterValue
          })
        }
      } else if (data.type === 'node') {
        data.forEach(v => {
          if (v.type === 'link') {
            this.reSetFormValue(v.children)
          } else if (v.type === 'node') {
            setFieldsValue({
              [`${v.id}KeySelect`]: v.filterKey,
              [`${v.id}OperatorSelect`]: v.filterOperator,
              [`${v.id}Input`]: v.filterValue
            })
          }
        })
      }
    }
  }

  triggerChange = (changedValue) => {
    const onChange = this.props.onChange
    if (onChange) {
      onChange(changedValue)
    }
  }

  render () {
    const {
      filterTree
    } = this.state

    return (
      <div style={{width: '100%', height: '260px', padding: '20px', border: '1px solid #ddd'}}>
        <div className={`filtersComponent`}>
          <Form className={`filterFormComponent`}>
            {this.renderFilters(filterTree)}
          </Form>
          {/* <div className={`buttons`}>
            <Button size="large" type="primary" onClick={this.doQuery}>查询</Button>
          </div> */}
        </div>
      </div>
    )
  }
}

DashboardItemFilters.propTypes = {
  form: PropTypes.any,
  onQuery: PropTypes.func,
  onRef: PropTypes.func,
  sourceData: PropTypes.string,
  onChange: PropTypes.func,
  isOpen: PropTypes.bool
}

export default Form.create()(DashboardItemFilters)
