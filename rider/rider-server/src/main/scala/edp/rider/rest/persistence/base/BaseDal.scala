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


package edp.rider.rest.persistence.base

import edp.rider.module.DbModule._
import slick.jdbc.MySQLProfile.api._
import slick.lifted.CanBeQueryCondition

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait BaseDal[T, A] {

  def insert(row: A): Future[A]

  def insert(rows: Seq[A]): Future[Seq[A]]

  def update(row: A): Future[Int]

  def update(rows: Seq[A]): Future[Int]

  def insertOrUpdate(row: A): Future[Int]

  def insertOrUpdate(rows: Seq[A]): Future[Int]

  def findById(id: Long): Future[Option[A]]

  def findAll: Future[Seq[A]]

  def findByFilter[C: CanBeQueryCondition](f: (T) => C): Future[Seq[A]]

  def deleteById(id: Long): Future[Int]

  def deleteById(ids: Seq[Long]): Future[Int]

  def deleteByFilter[C: CanBeQueryCondition](f: (T) => C): Future[Int]

  def createTable(): Future[Unit]

}

class BaseDalImpl[T <: BaseTable[A], A <: Product with BaseEntity](tableQ: TableQuery[T]) extends BaseDal[T, A] {

  override def insert(row: A): Future[A] = insert(Seq(row)).map(_.head)

  override def insert(rows: Seq[A]): Future[Seq[A]] = {
    val action = tableQ returning tableQ.map(_.id) into ((t, id) => t.copyWithId(id = id).asInstanceOf[A]) ++= rows
    db.run(action)
    /*val ids = Await.result(db.run(action), CommonUtils.minTimeOut)*/
    /*findByFilter(_.id inSet ids)*/
    /*ids.flatMap[Seq[A]] {
      seq => findByFilter(_.id inSet seq)
    }*/
  }

  override def update(row: A): Future[Int] = db.run(tableQ.filter(_.id === row.id).update(row))

  override def update(rows: Seq[A]): Future[Int] = Future.sequence(rows.map(r => update(r))).map(_.sum).mapTo[Int]

  override def insertOrUpdate(row: A): Future[Int] = db.run(tableQ.insertOrUpdate(row)).mapTo[Int]

  override def insertOrUpdate(rows: Seq[A]): Future[Int] = Future.sequence(rows.map(row => insertOrUpdate(row))).map(_.sum).mapTo[Int]

  override def findById(id: Long): Future[Option[A]] = db.run(tableQ.filter(_.id === id).result.headOption)


  override def findAll: Future[Seq[A]] = db.run(tableQ.result).mapTo[Seq[A]]

  override def findByFilter[C: CanBeQueryCondition](f: (T) => C): Future[Seq[A]] = db.run(tableQ.withFilter(f).result).mapTo[Seq[A]]

  override def deleteById(id: Long): Future[Int] = deleteById(Seq(id))

  override def deleteById(ids: Seq[Long]): Future[Int] = db.run(tableQ.filter(_.id.inSet(ids)).delete)

  override def deleteByFilter[C: CanBeQueryCondition](f: (T) => C): Future[Int] = db.run(tableQ.withFilter(f).delete)

  override def createTable(): Future[Unit] = db.run(DBIO.seq(tableQ.schema.create))

}
