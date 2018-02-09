CREATE TABLE IF NOT EXISTS `user` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `email` VARCHAR(200) NOT NULL,
  `password` VARCHAR(32) NOT NULL,
  `name` VARCHAR(200) NOT NULL,
  `role_type` VARCHAR(100) NOT NULL,
  `preferred_language` VARCHAR(20) NOT NULL,
  `active` TINYINT(1) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `email_UNIQUE` (`email` ASC))
ENGINE = InnoDB;

alter table `user` add column `preferred_language` VARCHAR(20) default "chinese";


CREATE TABLE IF NOT EXISTS `instance` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `ns_instance` VARCHAR(200) NOT NULL,
  `desc` VARCHAR(1000) NULL,
  `ns_sys` VARCHAR(30) NOT NULL,
  `conn_url` VARCHAR(200) NOT NULL,
  `active` TINYINT(1) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `instance_UNIQUE` (`ns_instance` ASC, `ns_sys` ASC))
ENGINE = InnoDB;

drop index `conn_url` on `instance`;

CREATE TABLE IF NOT EXISTS `ns_database` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `ns_database` VARCHAR(200) NOT NULL,
  `desc` VARCHAR(1000) NULL,
  `ns_instance_id` BIGINT NOT NULL,
  `user` VARCHAR(200) NULL,
  `pwd` VARCHAR(200) NULL,
  `partitions` INT NOT NULL,
  `config` VARCHAR(2000) NULL,
  `active` TINYINT(1) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `database_UNIQUE` (`ns_database` ASC, `ns_instance_id` ASC))
ENGINE = InnoDB;

alter table `ns_database` drop column `permission`;
drop index `database_UNIQUE` on `ns_database`;
alter table `ns_database` add UNIQUE index `database_UNIQUE` (`ns_database` ASC, `ns_instance_id` ASC);


CREATE TABLE IF NOT EXISTS `namespace` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `ns_sys` VARCHAR(100) NOT NULL,
  `ns_instance` VARCHAR(100) NOT NULL,
  `ns_database` VARCHAR(100) NOT NULL,
  `ns_table` VARCHAR(100) NOT NULL,
  `ns_version` VARCHAR(20) NOT NULL,
  `ns_dbpar` VARCHAR(100) NOT NULL,
  `ns_tablepar` VARCHAR(100) NOT NULL,
  `keys` VARCHAR(1000) NULL,
  `ums_info` LONGTEXT NULL,
  `sink_info` LONGTEXT NULL,
  `ns_database_id` BIGINT NOT NULL,
  `ns_instance_id` BIGINT NOT NULL,
  `active` TINYINT(1) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `namespace_UNIQUE` (`ns_sys` ASC, `ns_instance` ASC, `ns_database` ASC, `ns_table` ASC, `ns_version` ASC, `ns_dbpar` ASC, `ns_tablepar` ASC))
ENGINE = InnoDB;

alter table `namespace` drop column `permission`;
alter table `namespace` add column `ums_info` LONGTEXT default null;
alter table `namespace` add column `sink_info` LONGTEXT default null;
alter table `namespace`  modify column `ums_info` LONGTEXT;
drop index `namespace_UNIQUE` on `namespace`;
alter table `namespace` add UNIQUE index `namespace_UNIQUE` (`ns_sys` ASC, `ns_instance` ASC, `ns_database` ASC, `ns_table` ASC, `ns_version` ASC, `ns_dbpar` ASC, `ns_tablepar` ASC);

CREATE TABLE IF NOT EXISTS `stream` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(200) NOT NULL,
  `desc` VARCHAR(1000) NULL,
  `project_id` BIGINT NOT NULL,
  `instance_id` BIGINT NOT NULL,
  `stream_type` VARCHAR(100) NOT NULL,
  `spark_config` VARCHAR(5000) NULL,
  `start_config` VARCHAR(1000) NOT NULL,
  `launch_config` VARCHAR(1000) NOT NULL,
  `spark_appid` VARCHAR(200) NULL,
  `log_path` VARCHAR(200) NULL,
  `status` VARCHAR(200) NOT NULL,
  `started_time` TIMESTAMP NULL,
  `stopped_time` TIMESTAMP NULL,
  `active` TINYINT(1) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `project` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(200) NOT NULL,
  `desc` VARCHAR(1000) NULL,
  `pic` INT NOT NULL,
  `res_cores` INT NOT NULL,
  `res_memory_g` INT NOT NULL,
  `active` TINYINT(1) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `rel_project_ns` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `project_id` BIGINT NOT NULL,
  `ns_id` BIGINT NOT NULL,
  `active` TINYINT(1) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `rel_project_ns_UNIQUE` (`project_id` ASC, `ns_id` ASC))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `rel_project_user` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `project_id` BIGINT NOT NULL,
  `user_id` BIGINT NOT NULL,
  `active` TINYINT(1) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `rel_project_user_UNIQUE` (`project_id` ASC, `user_id` ASC))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `rel_project_udf` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `project_id` BIGINT NOT NULL,
  `udf_id` BIGINT NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `rel_project_udf_UNIQUE` (`project_id` ASC, `udf_id` ASC))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `rel_stream_udf` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `stream_id` BIGINT NOT NULL,
  `udf_id` BIGINT NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `rel_stream_udf_UNIQUE` (`stream_id` ASC, `udf_id` ASC))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `dbus_setting` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(200) NOT NULL,
  `desc` VARCHAR(1000) NULL,
  `dbus_url` VARCHAR(200) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC),
  UNIQUE INDEX `dbus_url_UNIQUE` (`dbus_url` ASC))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `dbus` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `dbus_id` BIGINT NOT NULL,
  `namespace` VARCHAR(200) NOT NULL,
  `kafka` VARCHAR(200) NOT NULL,
  `topic` VARCHAR(200) NOT NULL,
  `instance_id` BIGINT NOT NULL,
  `database_id` BIGINT NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `synchronized_time` TIMESTAMP NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `dbus_UNIQUE` (`namespace` ASC))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `flow` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `project_id` BIGINT NOT NULL,
  `stream_id` BIGINT NOT NULL,
  `source_ns` VARCHAR(200) NOT NULL,
  `sink_ns` VARCHAR(200) NOT NULL,
  `consumed_protocol` VARCHAR(20) NOT NULL,
  `sink_config` VARCHAR(5000) NOT NULL,
  `tran_config` LONGTEXT NULL,
  `status` VARCHAR(200) NOT NULL,
  `started_time` TIMESTAMP NULL,
  `stopped_time` TIMESTAMP NULL,
  `active` TINYINT(1) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `flow_UNIQUE` (`source_ns` ASC, `sink_ns` ASC))
ENGINE = InnoDB;

alter table `flow`  modify column `tran_config` LONGTEXT;

CREATE TABLE IF NOT EXISTS `directive` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `protocol_type` VARCHAR(200) NOT NULL,
  `stream_id` BIGINT NOT NULL,
  `flow_id` BIGINT NOT NULL,
  `directive` LONGTEXT NOT NULL,
  `zk_path` VARCHAR(200) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`))
  ENGINE = InnoDB;

alter table `directive` modify column `directive` LONGTEXT;

CREATE TABLE IF NOT EXISTS `rel_stream_intopic` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `stream_id` BIGINT NOT NULL,
  `ns_instance_id` BIGINT NOT NULL,
  `ns_database_id` BIGINT NOT NULL,
  `partition_offsets` VARCHAR(5000) NOT NULL,
  `rate` INT NOT NULL,
  `active` TINYINT(1) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `rel_stream_intopic_UNIQUE` (`stream_id` ASC, `ns_instance_id` ASC, `ns_database_id` ASC))
ENGINE = InnoDB;

alter table `rel_stream_intopic`  modify column `partition_offsets` VARCHAR(5000);

CREATE TABLE IF NOT EXISTS `job` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(200) NOT NULL,
  `project_id` BIGINT NOT NULL,
  `source_ns` VARCHAR(200) NOT NULL,
  `sink_ns` VARCHAR(200) NOT NULL,
  `source_type` VARCHAR(30) NOT NULL,
  `spark_config` VARCHAR(4000) NULL,
  `start_config` VARCHAR(1000) NOT NULL,
  `event_ts_start` VARCHAR(50) NOT NULL,
  `event_ts_end` VARCHAR(50) NOT NULL,
  `source_config` VARCHAR(5000) NULL,
  `sink_config` VARCHAR(5000) NULL,
  `tran_config` VARCHAR(5000) NULL,
  `status` VARCHAR(200) NOT NULL,
  `spark_appid` VARCHAR(200) NULL,
  `log_path` VARCHAR(200) NULL,
  `started_time` TIMESTAMP NULL,
  `stopped_time` TIMESTAMP NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC),
  UNIQUE INDEX `job_UNIQUE` (`source_ns` ASC, `sink_ns` ASC))
ENGINE = InnoDB;

alter table `job` drop column `consumed_protocol`;
alter table `job` drop column `job_config`;
alter table `job` add column `spark_config` VARCHAR(4000) NULL after `source_type`;
alter table `job` add column `start_config` VARCHAR(1000) NOT NULL after `spark_config`;


CREATE TABLE IF NOT EXISTS `udf` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `function_name` VARCHAR(200) NOT NULL,
  `full_class_name` VARCHAR(200) NOT NULL,
  `jar_name` VARCHAR(200) NOT NULL,
  `desc` VARCHAR(200) NULL,
  `public` TINYINT(1) NOT NULL,
  `create_time` TIMESTAMP NOT NULL,
  `create_by` BIGINT NOT NULL,
  `update_time` TIMESTAMP NOT NULL,
  `update_by` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `function_name_UNIQUE` (`function_name` ASC))
ENGINE = InnoDB;

drop index `full_class_name_UNIQUE` on `udf`;


CREATE TABLE IF NOT EXISTS `feedback_heartbeat` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `protocol_type` VARCHAR(200) NOT NULL,
  `ums_ts` TIMESTAMP NOT NULL,
  `stream_id` BIGINT NOT NULL,
  `namespace` VARCHAR(1000) NOT NULL,
  `feedback_time` TIMESTAMP NOT NULL,
  PRIMARY KEY (`id`)
)ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `feedback_stream_offset` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `protocol_type` VARCHAR(200) NOT NULL,
  `ums_ts` TIMESTAMP NOT NULL,
  `stream_id` BIGINT NOT NULL,
  `topic_name` VARCHAR(200) NOT NULL,
  `partition_num` INT NOT NULL,
  `partition_offsets` VARCHAR(5000) NOT NULL,
  `feedback_time` TIMESTAMP NOT NULL,
  PRIMARY KEY (`id`),
  KEY `streamIndex` (`stream_id`),
  KEY `timeIndex` (`feedback_time`)
)ENGINE = InnoDB;

alter table `feedback_stream_offset`  modify column `partition_offsets` VARCHAR(5000);


CREATE TABLE IF NOT EXISTS `feedback_stream_error` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `protocol_type` VARCHAR(200) NOT NULL,
  `ums_ts` TIMESTAMP NOT NULL,
  `stream_id`   BIGINT NOT NULL,
  `status`    VARCHAR(32) NOT NULL,
  `result_desc` VARCHAR(5000) NOT NULL,
  `feedback_time` TIMESTAMP NOT NULL,
  PRIMARY KEY (`id`)
)ENGINE = InnoDB;

alter table `feedback_stream_error` modify column `result_desc` varchar(5000);

CREATE TABLE IF NOT EXISTS `feedback_flow_error` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `protocol_type` VARCHAR(200) NOT NULL,
  `ums_ts` TIMESTAMP NOT NULL,
  `stream_id`   BIGINT default 0,
  `source_namespace`  VARCHAR(1000) NOT NULL,
  `sink_namespace`  VARCHAR(1000) NOT NULL,
  `error_count`   INT NOT NULL,
  `error_max_watermark_ts`  TIMESTAMP,
  `error_min_watermark_ts`  TIMESTAMP,
  `error_info` VARCHAR(5000) NOT NULL,
  `feedback_time` TIMESTAMP NOT NULL,
  PRIMARY KEY (`id`)
)ENGINE = InnoDB;

alter table `feedback_flow_error` modify column `error_info` varchar(5000);

CREATE TABLE IF NOT EXISTS `feedback_directive` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `protocol_type` VARCHAR(200) NOT NULL,
  `ums_ts` TIMESTAMP NOT NULL,
  `stream_id`  BIGINT NOT NULL,
  `directive_id`  BIGINT NOT NULL,
  `status`      VARCHAR(32) NOT NULL,
  `result_desc` VARCHAR(5000),
  `feedback_time` TIMESTAMP NOT NULL,
  PRIMARY KEY (`id`)
)ENGINE = InnoDB;

alter table `feedback_directive` modify column `result_desc` varchar(5000);

