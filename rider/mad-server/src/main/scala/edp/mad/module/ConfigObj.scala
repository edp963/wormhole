package  edp.mad.module


object ConfigObj{
  lazy val configModule = new ConfigModuleImpl
    with ConfigOptionImpl

  def getModule: ConfigModuleImpl with ConfigOptionImpl ={
    configModule
  }
}