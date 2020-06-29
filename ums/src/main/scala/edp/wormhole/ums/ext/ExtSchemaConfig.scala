package edp.wormhole.ums.ext

/**
  * @author Suxy
  * @date 2020/5/20
  * @description add ext schema config
  */
case class ExtSchemaConfig(`type`: String,
                           field_delimiter: String,
                           obj_fields: Array[ExtSchemaFieldConfig]
                          )

case class ExtSchemaFieldConfig(name: String,
                                `type`: String,
                                field_delimiter: String,
                                obj_fields: Array[ExtSchemaFieldConfig]
                               )
