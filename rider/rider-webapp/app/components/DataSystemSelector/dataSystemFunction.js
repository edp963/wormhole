/*
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */

/**
 * data system
 */
export function loadDataSystemData () {
  const dataSystemData = [
    { value: 'kafka', icon: 'icon-kafka', style: { fontSize: '35px' } },
    { value: 'oracle', icon: 'icon-amy-db-oracle', style: { lineHeight: '40px' } },
    { value: 'mysql', icon: 'icon-mysql' },
    { value: 'es', icon: 'icon-elastic', style: { fontSize: '24px' } },
    { value: 'hbase', icon: 'icon-hbase1' },
    { value: 'phoenix', text: 'Phoenix' },
    { value: 'cassandra', icon: 'icon-cass', style: { fontSize: '52px', lineHeight: '60px' } },
    { value: 'postgresql', icon: 'icon-postgresql', style: { fontSize: '31px' } },
    { value: 'mongodb', icon: 'icon-mongodb', style: { fontSize: '26px' } },
    { value: 'redis', icon: 'icon-redis', style: { fontSize: '31px' } },
    { value: 'vertica', icon: 'icon-vertica', style: { fontSize: '45px' } },
    { value: 'parquet', text: 'Parquet' },
    { value: 'kudu', text: 'Kudu' },
    { value: 'greenplum', text: 'Greenplum' },
    { value: 'clickhouse', text: 'ClickHouse' },
    { value: 'rocketmq', text: 'RocketMQ' },
    { value: 'http', text: 'HTTP' }
  ]
  return dataSystemData
}

export function filterDataSystemData () {
  const dataSystemValue = [
    { text: 'kafka', value: 'kafka' },
    { text: 'oracle', value: 'oracle' },
    { text: 'mysql', value: 'mysql' },
    { text: 'es', value: 'es' },
    { text: 'hbase', value: 'hbase' },
    { text: 'phoenix', value: 'phoenix' },
    { text: 'cassandra', value: 'cassandra' },
    { text: 'postgresql', value: 'postgresql' },
    { text: 'mongodb', value: 'mongodb' },
    { text: 'redis', value: 'redis' },
    { text: 'vertica', value: 'vertica' },
    { text: 'parquet', value: 'parquet' },
    { text: 'kudu', value: 'kudu' },
    { text: 'log', value: 'log' },
    { value: 'greenplum', text: 'Greenplum' },
    { value: 'clickhouse', text: 'ClickHouse' }
  ]
  return dataSystemValue
}

export function sourceDataSystemData () {
  const sourceDataSystemData = [
    { value: 'kafka', icon: 'icon-kafka', style: { fontSize: '35px' } },
    { value: 'log', text: 'Log' },
    { value: 'file', text: 'File' },
    { value: 'app', text: 'App' },
    { value: 'mysql', icon: 'icon-mysql' },
    { value: 'oracle', icon: 'icon-amy-db-oracle', style: { lineHeight: '40px' } },
    { value: 'mongodb', icon: 'icon-mongodb', style: { fontSize: '26px' } },
    { value: 'greenplum', text: 'Greenplum' }
  ]
  return sourceDataSystemData
}

export function sinkDataSystemData () {
  const sinkDataSystemData = [
    { value: 'oracle', icon: 'icon-amy-db-oracle', style: { lineHeight: '40px' } },
    { value: 'mysql', icon: 'icon-mysql' },
    { value: 'es', icon: 'icon-elastic', style: { fontSize: '24px' } },
    { value: 'hbase', icon: 'icon-hbase1' },
    { value: 'phoenix', text: 'Phoenix' },
    { value: 'kafka', icon: 'icon-kafka', style: { fontSize: '35px' } },
    { value: 'postgresql', icon: 'icon-postgresql', style: { fontSize: '31px' } },
    { value: 'cassandra', icon: 'icon-cass', style: { fontSize: '52px', lineHeight: '60px' } },
    { value: 'mongodb', icon: 'icon-mongodb', style: { fontSize: '26px' } },
    { value: 'vertica', icon: 'icon-vertica', style: { fontSize: '45px' } },
    { value: 'kudu', text: 'Kudu' },
    { value: 'greenplum', text: 'Greenplum' },
    { value: 'clickhouse', text: 'ClickHouse' },
    { value: 'redis', text: 'Redis' },
    { value: 'rocketmq', text: 'RocketMQ' },
    { value: 'http', text: 'HTTP' }
  ]
  return sinkDataSystemData
}

export function jobSinkDataSystemData () {
  const sinkDataSystemData = [
    { value: 'oracle', icon: 'icon-amy-db-oracle', style: { lineHeight: '40px' } },
    { value: 'mysql', icon: 'icon-mysql' },
    { value: 'es', icon: 'icon-elastic', style: { fontSize: '24px' } },
    { value: 'hbase', icon: 'icon-hbase1' },
    { value: 'phoenix', text: 'Phoenix' },
    { value: 'kafka', icon: 'icon-kafka', style: { fontSize: '35px' } },
    { value: 'postgresql', icon: 'icon-postgresql', style: { fontSize: '31px' } },
    { value: 'cassandra', icon: 'icon-cass', style: { fontSize: '52px', lineHeight: '60px' } },
    { value: 'mongodb', icon: 'icon-mongodb', style: { fontSize: '26px' } },
    { value: 'vertica', icon: 'icon-vertica', style: { fontSize: '45px' } },
    { value: 'parquet', text: 'Parquet' },
    { value: 'kudu', text: 'Kudu' },
    { value: 'greenplum', text: 'Greenplum' },
    { value: 'clickhouse', text: 'ClickHouse' },
    { value: 'redis', text: 'Redis' }
  ]
  return sinkDataSystemData
}

export function flowTransformationDadaHide () {
  const transformData = [
    { value: 'mysql', icon: 'icon-mysql' },
    { value: 'oracle', icon: 'icon-amy-db-oracle' },
    { value: 'postgresql', icon: 'icon-postgresql', style: { fontSize: '31px' } },
    { value: 'cassandra', icon: 'icon-cass', style: { fontSize: '52px', lineHeight: '60px' } },
    { value: 'mongodb', icon: 'icon-mongodb', style: { fontSize: '26px' } },
    { value: 'phoenix', text: 'Phoenix' },
    { value: 'es', icon: 'icon-elastic', style: { fontSize: '24px' } },
    { value: 'greenplum', text: 'Greenplum' },
    { value: 'kudu', text: 'Kudu' },
    { value: 'clickhouse', text: 'ClickHouse' }
  ]
  return transformData
}

export function flowTransformationDadaShow () {
  const transformData = [
    { value: 'mysql', icon: 'icon-mysql' },
    { value: 'oracle', icon: 'icon-amy-db-oracle' },
    { value: 'postgresql', icon: 'icon-postgresql', style: { fontSize: '31px' } },
    { value: 'cassandra', icon: 'icon-cass', style: { fontSize: '52px', lineHeight: '60px' } },
    { value: 'mongodb', icon: 'icon-mongodb', style: { fontSize: '26px' } },
    { value: 'phoenix', text: 'Phoenix' },
    { value: 'hbase', icon: 'icon-hbase1' },
    { value: 'es', icon: 'icon-elastic', style: { fontSize: '24px' } },
    { value: 'redis', icon: 'icon-redis', style: { fontSize: '31px' } },
    { value: 'kudu', text: 'Kudu' },
    { value: 'greenplum', text: 'Greenplum' },
    { value: 'clickhouse', text: 'ClickHouse' }
  ]
  return transformData
}
