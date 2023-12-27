/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.nebula.exchange.coder

import com.ubisectech.nebula.exchange.bean.{EdgeKeyData, EdgeValueData, TagKeyData, TagValueData}
import com.ubisectech.nebula.exchange.common.VidType
import com.vesoft.nebula.encoder.SchemaProvider

import java.nio.ByteOrder

class NebulaDecodecImpl(byteOrder: ByteOrder, nebulaKeys: List[String], schemaProvider: SchemaProvider) extends NebulaDecodec {
  private val rowReader = new RowReaderImpl(this.byteOrder, nebulaKeys, schemaProvider)

  val PARTITION_ID_SIZE = 4;
  val TAG_ID_SIZE = 4;
  val EDGE_TYPE_SIZE = 4;
  val EDGE_RANKING_SIZE = 8;
  val EDGE_VER_PLACE_HOLDER_SIZE = 1;
  val VERTEX_SIZE = PARTITION_ID_SIZE + TAG_ID_SIZE;
  val EDGE_SIZE = PARTITION_ID_SIZE + EDGE_TYPE_SIZE + EDGE_RANKING_SIZE + EDGE_VER_PLACE_HOLDER_SIZE;

  val VERTEX_KEY_TYPE = 0x00000001;
  val EDGE_KEY_TYPE = 0x00000002;
  val ORPHAN_VERTEX_KEY_TYPE = 0x00000007;
  val SEEK = 0xc70f6907;


  def this(nebulaKeys: List[String], schemaProvider: SchemaProvider) {
    this(java.nio.ByteOrder.nativeOrder(), nebulaKeys, schemaProvider)
  }


  def parseTagKey(vidLen: Int, vidType: VidType.Value, data: Array[Byte]): TagKeyData = {
    val buffer = java.nio.ByteBuffer.allocate(data.length)
    buffer.order(this.byteOrder);
    buffer.put(data)

    //buffer.flip() //NoSuchMethodError
    val partitionId = buffer.getInt(0) //4
    val vid = if (vidType == VidType.INT) {
      buffer.getLong(4).toString
    } else {
      //val fillByte='\0'.toByte
      val bs = new Array[Byte](vidLen)
      buffer.get(bs, 4, vidLen)
      new String(bs)
    }
    //最后4个字节
    val tagId = buffer.getInt(data.length - 4)
    //buffer.clear()   //NoSuchMethodError
    TagKeyData(partitionId = partitionId, vid = vid, tagId = tagId)
  }


  private def getEdgeRank(bs: Array[Byte]): Long = {
    val rankBuffer = java.nio.ByteBuffer.allocate(bs.length)
    rankBuffer.order(ByteOrder.BIG_ENDIAN)
    rankBuffer.put(bs)
    val edgeRank = rankBuffer.getLong(0) ^ (1L << 63)
    edgeRank
  }

  def parseTagValue(data: Array[Byte]): TagValueData = {
    val buffer = java.nio.ByteBuffer.allocate(data.length)
    buffer.order(this.byteOrder)
    buffer.put(data)
    //    buffer.flip()
    rowReader.init(buffer)
    val valueSeq = rowReader.reader()
    TagValueData(valueSeq.map(_._2))
  }


  def parseEdgeKey(vidLen: Int, vidType: VidType.Value, data: Array[Byte]): EdgeKeyData = {
    val buffer = java.nio.ByteBuffer.allocate(data.length)
    buffer.order(this.byteOrder);
    buffer.put(data)

    //buffer.flip() //NoSuchMethodError
    var offset = 0
    val partitionId = buffer.getInt(offset) //4
    offset += java.lang.Integer.BYTES
    val srcId = if (vidType == VidType.INT) {
      val id = buffer.getLong(offset).toString
      offset += java.lang.Long.BYTES
      id
    } else {
      //val fillByte='\0'.toByte
      val bs = new Array[Byte](vidLen)
      buffer.get(bs, offset, vidLen)
      offset += vidLen
      new String(bs).trim
    }

    val edgeType = buffer.getInt(offset)
    offset += java.lang.Integer.BYTES

    //buffer.get(rankBs, offset, rankBs.length)   //报异常
    val rankBs = buffer.array().slice(offset, offset + java.lang.Long.BYTES)
    val edgeRank = getEdgeRank(rankBs)
    //    val edgeRank = 0L
    offset += java.lang.Long.BYTES
    val dstId = if (vidType == VidType.INT) {
      val id = buffer.getLong(offset).toString
      offset += java.lang.Long.BYTES
      id
    } else {
      //val fillByte='\0'.toByte
      val bs = new Array[Byte](vidLen)
      offset += vidLen
      buffer.get(bs, offset, vidLen)
      new String(bs).trim
    }
    val edgeVerHolder = buffer.get(offset)
    EdgeKeyData(partitionId = partitionId, srcId = srcId, dstId = dstId, edgeType = edgeType, edgeRank = edgeRank, edgeVerHolder = edgeVerHolder)
  }

  def parseEdgeValue(data: Array[Byte]): EdgeValueData = {
    val buffer = java.nio.ByteBuffer.allocate(data.length)
    buffer.order(this.byteOrder)
    buffer.put(data)
    //    buffer.flip()
    rowReader.init(buffer)
    val valueSeq = rowReader.reader()
    EdgeValueData(valueSeq.map(_._2))
  }

}
