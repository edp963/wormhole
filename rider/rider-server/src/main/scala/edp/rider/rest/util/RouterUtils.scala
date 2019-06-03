package edp.rider.rest.util

import edp.rider.common.{RiderLogger, UserRoleType}
import edp.rider.common.UserRoleType.UserRoleType
import edp.rider.rest.router.SessionClass

object RouterUtils extends RiderLogger {

  def checkRoleType(roleType: UserRoleType, userSession: SessionClass): Boolean = {
    if (UserRoleType.withName(userSession.roleType) == roleType)
      true
    else
      false
  }

  def checkUserType(userSession: SessionClass): Boolean = {
    checkRoleType(UserRoleType.USER, userSession)
  }

  def checkAdminType(userSession: SessionClass): Boolean = {
    checkRoleType(UserRoleType.ADMIN, userSession)
  }

  def checkAppType(userSession: SessionClass): Boolean = {
    checkRoleType(UserRoleType.APP, userSession)
  }
}
