package edp.wormhole.flinkx.util

import akka.stream.stage.TimerMessages.Scheduled
import org.apache.flink.metrics.reporter.MetricReporter

class MetricsReporter extends MetricReporter  with Scheduled{

}
