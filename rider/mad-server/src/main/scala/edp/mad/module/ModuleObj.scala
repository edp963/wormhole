package edp.mad.module

object ModuleObj {
  lazy val modules = new ConfigModuleImpl
    with ConfigOptionImpl
    with ActorModuleImpl
    with  DBDriverModuleImpl
    with PersistenceModuleImpl
    with CacheImpl

  def getModule: ConfigModuleImpl with ConfigOptionImpl with ActorModuleImpl with DBDriverModuleImpl with PersistenceModuleImpl with CacheImpl  = {
    modules
  }
}

