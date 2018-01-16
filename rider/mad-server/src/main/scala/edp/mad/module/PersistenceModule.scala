/*-
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


package edp.mad.module

import edp.mad.persistence.dal._
import edp.mad.persistence.entities._
import slick.lifted.TableQuery

trait PersistenceModule {
  val offsetTableQuery = TableQuery[OffsetSavedTable]
  val cacheEffectTableQuery = TableQuery[CacheEffectTable]

  val applicationCacheTableQuery = TableQuery[ApplicationCacheTable]
  val streamCacheTableQuery = TableQuery[StreamCacheTable]
  val namespaceCacheTableQuery  = TableQuery[NamespaceCacheTable]


  val offsetSavedDal: OffsetSavedDal
  val cacheEffectDal: CacheEffectDal

  val applicationCacheDal: ApplicationCacheDal
  val streamCacheDal: StreamCacheDal
  val namespaceCacheDal: NamespaceCacheDal

}

trait PersistenceModuleImpl extends PersistenceModule {
  this: DBDriverModuleImpl =>
  override lazy val offsetSavedDal = new OffsetSavedDal(offsetTableQuery)
  override lazy val cacheEffectDal = new CacheEffectDal(cacheEffectTableQuery)
  override lazy val applicationCacheDal = new ApplicationCacheDal(applicationCacheTableQuery)
  override lazy val streamCacheDal = new StreamCacheDal(streamCacheTableQuery)
  override lazy val namespaceCacheDal = new NamespaceCacheDal(namespaceCacheTableQuery)
}
