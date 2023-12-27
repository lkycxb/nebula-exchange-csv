/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.nebula.exchange.bean

case class TagKeyData(partitionId: Int, vid: String, tagId: Int)

case class TagValueData(values: Seq[Any])

case class EdgeKeyData(partitionId: Int, srcId: String, dstId: String, edgeType: Int, edgeRank: Long, edgeVerHolder: Byte)

case class EdgeValueData(values: Seq[Any])


case class ValueStrs(values: Seq[String])
