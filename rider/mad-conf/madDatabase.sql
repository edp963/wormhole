CREATE TABLE IF NOT EXISTS `offset_saved`(
	`id` BIGINT auto_increment PRIMARY KEY,
	`stream_id` BIGINT,
	`topic_name` varchar(128),
	`partition_offsets` varchar(512),
	`partition_num` INT,
	`feedback_time` TIMESTAMP
	);
CREATE UNIQUE INDEX offset_unique_index on `offset_saved`(`stream_id`,`topic_name`);

CREATE TABLE IF NOT EXISTS `cache_effect`(
	`id` BIGINT auto_increment PRIMARY KEY,
	`stream_id` BIGINT ,
	`hit_count` BIGINT,
	`latest_hit_time` TIMESTAMP,
	`missed_count` BIGINT,
	`latest_missed_time` TIMESTAMP
	);
CREATE UNIQUE INDEX cache_effect_unique_index on `cache_effect`(`stream_id`);

CREATE TABLE IF NOT EXISTS `application_cache`(
    `id` BIGINT auto_increment PRIMARY KEY,
	`application_id` varchar(128) ,
	`stream_name` varchar(128) ,
	`update_time` TIMESTAMP,
  `create_time` TIMESTAMP
	);

CREATE TABLE IF NOT EXISTS `stream_cache`(
  `id` BIGINT auto_increment PRIMARY KEY,
  `stream_id`    BIGINT ,
  `stream_name`   varchar(128),
  `project_info`  nvarchar(2000),
  `stream_info`   nvarchar(2000),
  `flow_info_list`    nvarchar(4000),
  `update_time` TIMESTAMP,
  `create_time` TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `namespace_cache`(
  `id` BIGINT auto_increment PRIMARY KEY,
  `namespace`   varchar(128),
  `topic_name`   varchar(128),
  `update_time` TIMESTAMP,
  `create_time` TIMESTAMP
);

