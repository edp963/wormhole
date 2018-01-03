package edp.rider.common

case class InstanceNotExistException(message: String, cause: Throwable = null) extends RuntimeException(message)

case class DatabaseSearchException(message: String, cause: Throwable = null) extends RuntimeException(message)

case class GetZookeeperDataException(message: String, cause: Throwable = null) extends RuntimeException(message)

case class GetStreamDetailException(message: String, cause: Throwable = null) extends RuntimeException(message)