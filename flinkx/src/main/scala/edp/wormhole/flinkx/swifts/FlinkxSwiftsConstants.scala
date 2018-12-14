package edp.wormhole.flinkx.swifts

object FlinkxSwiftsConstants {

  val SWIFTS_SPECIFIC_CONFIG ="swifts_specific_config"
  val EXCEPTION_PROCESS_METHOD= "exception_process_method"
  val MIN_IDLE_STATE_RETENTION_TIME = "min_idle_state_retention_time"
  val MAX_IDLE_STATE_RETENTION_TIME = "max_idle_state_retention_time"
  val PRESERVE_MESSAGE_FLAG = "preserve_message_flag"
  val MESSAGE_FLAG = "message_flag"
  val KEY_BY_FIELDS="key_by_fields"
}


object FlinkxTimeCharacteristicConstants {
  val TIME_CHARACTERISTIC = "time_characteristic"
  val PROCESSING_TIME = "processing_time"
  val EVENT_TIME = "event_time"
}
