/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.nebula.exchange

import com.ubisectech.nebula.exchange.coder.NebulaDecodecImpl
import com.ubisectech.nebula.exchange.common.config.{CaSignParam, SelfSignParam, SslConfigEntry, SslType}
import com.ubisectech.nebula.exchange.common.utils.NebulaUtils
import com.ubisectech.nebula.exchange.common.{MetaProvider, VidType}
import com.ubisectech.nebula.exchange.processor.VerticesProcessor
import com.vesoft.nebula.client.graph.data.HostAddress
import com.vesoft.nebula.encoder.NebulaCodecImpl
import com.vesoft.nebula.meta.TagItem
import junit.framework.TestCase
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.rocksdb.{Options, ReadOptions, SstFileReader}

import java.io.File
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


class TestNebulaSst extends TestCase {
  private val metaAddresses = Seq("192.88.1.241:9559")
  private val space = "compographtest"
  private val labelTag = "testtag"
  private val labelEdge = "cdc"

  private var sslConfig: SslConfigEntry = null
  private var metaProvider: MetaProvider = null

  override def setUp() = {
    val hostAndPorts = new ListBuffer[HostAddress]
    for (address <- metaAddresses) {
      hostAndPorts.append(NebulaUtils.getAddressFromString(address))
    }
    val metaAddress = hostAndPorts.toList
    val caParam: CaSignParam = null
    val selfParam: SelfSignParam = null
    sslConfig = SslConfigEntry(false, false, SslType.SELF, caParam, selfParam)
    metaProvider = new MetaProvider(metaAddress, 5000, 2, sslConfig)
  }

  val PARTITION_ID_SIZE = 4;
  val TAG_ID_SIZE = 4;

  val VERTEX_SIZE = PARTITION_ID_SIZE + TAG_ID_SIZE


  def getKeyBytes(vidLen: Int, vidType: VidType.Value): Array[Byte] = {
    val vertexId: String = "7839471913536911103"
    val partitionNum = metaProvider.getPartNumber(space)
    val partitionId = NebulaUtils.getPartitionId(vertexId, partitionNum, vidType)
    val tagItem = metaProvider.getTagItem(space, labelTag)
    val vidBytes = if (vidType == VidType.INT) {
      ByteBuffer
        .allocate(8)
        .order(ByteOrder.nativeOrder)
        .putLong(vertexId.toLong)
        .array
    } else {
      vertexId.getBytes()
    }

    val codec = new NebulaCodecImpl()
    val vertexKey = codec.vertexKey(vidLen, partitionId, vidBytes, tagItem.getTag_id)
    vertexKey
  }

  def testParseKey(): Unit = {
    val decodec = new NebulaDecodecImpl(null, null)
    val vidType = metaProvider.getVidType(space)
    val vidLen = metaProvider.getSpaceVidLen(space)
    val keyBs = getKeyBytes(vidLen, vidType)
    val k = decodec.parseTagKey(vidLen, vidType, keyBs)
    println(k)
  }

  private def getValueBs(fields: List[String], rowValues: Array[Any], tagItem: TagItem, fieldTypeMap: Map[String, Int]): Array[Byte] = {

    val fieldKeys = fields // List("name", "vendor")
    val rowFields = fieldKeys.map(d => StructField(d, StringType, true))
    val rowSchema: StructType = StructType(rowFields)
    val row: Row = new GenericRowWithSchema(rowValues, rowSchema)
    //    val partitionNum = metaProvider.getPartNumber(space)
    //    val vidType = metaProvider.getVidType(space)
    //    val spaceVidLen = metaProvider.getSpaceVidLen(space)

    val encodec = new NebulaCodecImpl
    //row, partitionNum, vidType, spaceVidLen, tagItem, fieldTypeMap
    //    encodec.encodeVertex(row,partitionNum,vidType,spaceVidLen,tagItem,fieldTypeMap)

    val processor = new VerticesProcessor(
      null,
      null,
      null,
      fieldKeys,
      null,
      null,
      null,
      null
    )


    val values = for {
      property <- fieldKeys if property.trim.length != 0
    } yield
      processor.extraValueForSST(row, property, fieldTypeMap)
        .asInstanceOf[AnyRef]

    val vertexValue = encodec.encodeTag(tagItem, fieldKeys.asJava, values.asJava)
    vertexValue
  }

  def testBytes(): Unit = {
    val bs: Array[Byte] = Array(111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 115, 112, 97, 114, 107)
    println(new String(bs))
  }

  def testBuffer(): Unit = {
    val buffer = java.nio.ByteBuffer.allocate(8)
    buffer.order(java.nio.ByteOrder.nativeOrder())
    buffer.putInt(1)
    buffer.putInt(1)
    val arr = buffer.array()
    println(arr.length)
  }

  def testWriteValue(): Unit = {
    val fields = List("name", "vendor")
    val rowValues: Array[Any] = Array[Any]("abc", "qqq")
    val tagItem = metaProvider.getTagItem(space, labelTag)
    val fieldTypeMap = metaProvider.getTagSchemaSeq(space, labelTag).toMap
    val vertexValue = getValueBs(fields, rowValues, tagItem, fieldTypeMap)
    println("vertexValue.length:" + vertexValue.length)
    vertexValue.foreach(println)
  }

  def testParseTagValue(): Unit = {
    val schemaProvider = NebulaUtils.getSchemaProvider(metaProvider, space, labelTag, null)
    //    List("name", "vendor")
    val fields = "dlong,dbool,dstr1,dstr2,ddouble,dint,dshort,dbyte,dfloat,ddate,dtime,ddatetime,dtimestamp,dgeo,dgeopoint,dgeoline,dgeopoly".split(",").toList


    val rowValues: Array[Any] = Array[Any](
      1L, false, "str1", "str2", 2.2d, 10, 1, 1, 1.1,
      "2023-12-18",
      "02:59:40.000000",
      "2021-03-17T17:53:59.000000",
      System.currentTimeMillis() / 1000L,
      "POINT(120.12 30.16)", "POINT(120.12 30.16)", "LINESTRING(3.0 8.0,4.7 73.23)",
      "POLYGON((75.3 45.4,112.5 53.6,122.7 25.5,93.9 28.6,75.3 45.4))"
    )

    val tagItem = metaProvider.getTagItem(space, labelTag)
    val fieldTypeMap = metaProvider.getTagSchemaSeq(space, labelTag).toMap


    val vertexValue = getValueBs(fields, rowValues, tagItem, fieldTypeMap)
    val decodec = new NebulaDecodecImpl(List[String]("dt"), schemaProvider)
    val v = decodec.parseTagValue(vertexValue)
    println(v)
  }

  //match (v1)-[e:cdc]-(v2) return v1 limit 5
  def testParseTagSst(): Unit = {
    val vidType = metaProvider.getVidType(space)
    val vidLen = metaProvider.getSpaceVidLen(space)
    val readEnv = new Options()
    val reader = new SstFileReader(readEnv)
    val path = "F:\\share\\vulncomponents\\nebula\\compograph\\sst1\\testtag\\1\\1-62-1.sst"
    reader.open(path)
    reader.verifyChecksum
    println("numEntries:" + reader.getTableProperties().getNumEntries())


    val schemaProvider = NebulaUtils.getSchemaProvider(metaProvider, space, labelTag, null)
    val readOptions = new ReadOptions()
    val iterator = reader.newIterator(readOptions)
    var index = 0
    iterator.seekToFirst
    val decodec = new NebulaDecodecImpl(List[String]("name", "vendor"), schemaProvider)
    while (iterator.isValid() && index < 10) {
      val k = decodec.parseTagKey(vidLen, vidType, iterator.key())
      val v = decodec.parseTagValue(iterator.value())
      println(s"k:${k},v:${v}")
      iterator.next()
      index += 1
    }
    reader.close()
    readOptions.close()
    readEnv.close()
  }

  def testParseEdgeSst(): Unit = {
    val vidType = metaProvider.getVidType(space)
    val vidLen = metaProvider.getSpaceVidLen(space)
    val pathArr = new ArrayBuffer[String]()
    val parentPath = "F:\\share\\vulncomponents\\nebula\\compograph\\sst\\cdc"
    val parentFile = new File(parentPath)
    pathArr ++= parentFile.listFiles().flatMap(_.listFiles()).filter(_.getName.endsWith(".sst")).map(_.getAbsolutePath)

    var totalEntry = 0L
    for (path <- pathArr) {
      val readEnv = new Options()
      val reader = new SstFileReader(readEnv)
      reader.open(path)
      reader.verifyChecksum
      val numEntries = reader.getTableProperties().getNumEntries()
      println(s"path:${path},numEntries:" + numEntries)
      totalEntry += numEntries
      val schemaProvider = NebulaUtils.getSchemaProvider(metaProvider, space, labelEdge, null)
      val readOptions = new ReadOptions()
      val iterator = reader.newIterator(readOptions)
      var index = 0
      iterator.seekToFirst
      val decodec = new NebulaDecodecImpl(List[String]("dt"), schemaProvider)
      while (iterator.isValid() && index < 10) {
        val k = decodec.parseEdgeKey(vidLen, vidType, iterator.key())
        val v = decodec.parseEdgeValue(iterator.value()).values.mkString(",")
        //println(s"k:${k},v:${v}")
        iterator.next()
        index += 1
      }
      reader.close()
      readOptions.close()
      readEnv.close()
    }
    println(s"totalEntry:${totalEntry}")
  }

}
