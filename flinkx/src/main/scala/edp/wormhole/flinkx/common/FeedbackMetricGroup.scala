package edp.wormhole.flinkx.common

import org.apache.flink.metrics.MetricGroup
import org.apache.flink.runtime.metrics.MetricRegistry
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup

class FeedbackMetricGroup(registry: MetricRegistry,parent:MetricGroup)
  extends AbstractMetricGroup(registry,"feedback",parent){

}
