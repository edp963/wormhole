import React from 'react'
import PropTypes from 'prop-types'
import { connect } from 'react-redux'

import echarts from 'echarts/lib/echarts'
import 'echarts/lib/chart/line'
import 'echarts/lib/component/tooltip'
import 'echarts/lib/component/dataZoom'
import 'echarts/lib/component/title'
import 'echarts/lib/component/legend'
import 'echarts/lib/component/toolbox'
import 'echarts/lib/component/grid'
import 'echarts/lib/component/axis'

export class Line extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      echart: null
    }
  }
  componentWillReceiveProps (newProps) {
    this.updateChart(newProps)
  }
  componentDidMount () {
    this.initChart()
    window.addEventListener('resize', () => {
      this.state.echart.resize()
    })
  }

  initChart () {
    const { id, options } = this.props
    let chart = document.getElementById(id)
    let echart = echarts.init(chart)
    echart.setOption(options)
    this.setState({echart})
  }
  updateChart (props) {
    const { options } = props
    this.state.echart.setOption(options)
  }
  render () {
    const { id, style } = this.props
    const _style = Object.assign({}, {width: '100%', height: '230px'}, style)
    return (
      <div id={id} style={_style}></div>
    )
  }
}

Line.propTypes = {
  id: PropTypes.string,
  options: PropTypes.object,
  style: PropTypes.object
}
export default connect()(Line)
