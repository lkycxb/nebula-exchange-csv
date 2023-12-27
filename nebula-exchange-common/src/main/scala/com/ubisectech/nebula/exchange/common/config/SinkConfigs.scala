/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.ubisectech.nebula.exchange.common.config

/**
 * SinkCategory is used to expression the writer's type.
 */
object SinkCategory extends Enumeration {
  type Type = Value

  val CLIENT = Value("CLIENT")
  val SST = Value("SST")
  val CSV = Value("CSV")
}

class SinkCategory

/**
 * DataSinkConfigEntry
 */
sealed trait DataSinkConfigEntry {
  def category: SinkCategory.Value
}

/**
 * FileBaseSinkConfigEntry
 */
case class FileBaseSinkConfigEntry(override val category: SinkCategory.Value,
                                   localPath: String,
                                   remotePath: String,
                                   fsName: Option[String],
                                   separator: Option[String] = Some('\001'.toString))
  extends DataSinkConfigEntry {

  override def toString: String = {
    val fullRemotePath =
      if (fsName.isDefined) s"${fsName.get}$remotePath"
      else remotePath
    s"File sink: from ${localPath} to $fullRemotePath"
  }
}

/**
 * NebulaSinkConfigEntry use to specified the nebula service's address.
 */
case class NebulaSinkConfigEntry(override val category: SinkCategory.Value, addresses: List[String])
  extends DataSinkConfigEntry {
  override def toString: String = {
    s"Nebula sink addresses: ${addresses.mkString("[", ", ", "]")}"
  }
}
