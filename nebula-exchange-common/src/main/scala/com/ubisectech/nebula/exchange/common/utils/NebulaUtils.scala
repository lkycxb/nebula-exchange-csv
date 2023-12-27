/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.ubisectech.nebula.exchange.common.utils

import com.google.common.base.Strings
import com.google.common.primitives.UnsignedLong
import com.ubisectech.nebula.exchange.common.config.{SchemaConfigEntry, Type}
import com.ubisectech.nebula.exchange.common.{MetaProvider, VidType}
import com.vesoft.nebula.client.graph.data.HostAddress
import com.vesoft.nebula.encoder.{SchemaProvider, SchemaProviderImpl}
import com.vesoft.nebula.meta.{EdgeItem, GeoShape, Schema, TagItem}
import org.apache.commons.codec.digest.MurmurHash2
import org.slf4j.LoggerFactory

import java.nio.charset.Charset
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ListBuffer

object NebulaUtils {
  val DEFAULT_EMPTY_VALUE: String = "_NEBULA_EMPTY"

  private[this] val LOG = LoggerFactory.getLogger(getClass)

  def getDataSourceFieldType(sourceConfig: SchemaConfigEntry,
                             space: String,
                             metaProvider: MetaProvider): Map[String, Int] = {
    //    val nebulaFields = sourceConfig.nebulaFields
    //    val sourceFields = sourceConfig.fields
    val label = sourceConfig.name

    var nebulaSchemaMap: Map[String, Integer] = null
    val dataType: Type.Value = metaProvider.getLabelType(space, label)
    if (dataType == null) {
      throw new IllegalArgumentException(s"label $label does not exist.")
    }
    if (dataType == Type.VERTEX) {
      nebulaSchemaMap = metaProvider.getTagSchema(space, label)
    } else {
      nebulaSchemaMap = metaProvider.getEdgeSchema(space, label)
    }

    //    val sourceSchemaMap = mutable.LinkedHashMap[String, Int]()
    //    for (i <- nebulaFields.indices) {
    //      val nebulaField = nebulaFields.get(i)
    //      if (!nebulaSchemaMap.contains(nebulaField)) {
    //        throw new IllegalArgumentException(
    //          s"property name $nebulaField is not defined in NebulaGraph")
    //      }
    //      sourceSchemaMap.put(sourceFields.get(i), nebulaSchemaMap(nebulaField))
    //    }
    //sourceSchemaMap.toMap
    nebulaSchemaMap.map(kv => kv._1 -> Integer2int(kv._2))
  }


  def isNumic(str: String): Boolean = {
    val newStr: String = if (str.startsWith("-")) {
      str.substring(1)
    } else {
      str
    }

    for (char <- newStr.toCharArray) {
      if (!Character.isDigit(char)) return false
    }
    true
  }

  def escapeUtil(str: String): String = {
    var s = str
    if (s.contains("\\")) {
      s = s.replaceAll("\\\\", "\\\\\\\\")
    }
    if (s.contains("\t")) {
      s = s.replaceAll("\t", "\\\t")
    }
    if (s.contains("\n")) {
      s = s.replaceAll("\n", "\\\n")
    }
    if (s.contains("\"")) {
      s = s.replaceAll("\"", "\\\"")
    }
    if (s.contains("\'")) {
      s = s.replaceAll("\'", "\\\'")
    }
    if (s.contains("\r")) {
      s = s.replaceAll("\r", "\\\r")
    }
    if (s.contains("\b")) {
      s = s.replaceAll("\b", "\\\b")
    }
    s
  }

  def getPartitionId(id: String, partitionSize: Int, vidType: VidType.Value): Int = {
    val hashValue: Long = if (vidType == VidType.STRING) {
      // todo charset must be the same with Nebula Space
      val byteId = id.getBytes(Charset.forName("UTF-8"))
      if (byteId.length == 8) {
        //byte array to long, need to take care of endianess
        ByteBuffer.wrap(byteId).order(ByteOrder.nativeOrder).getLong
      } else {
        MurmurHash2.hash64(byteId, byteId.length, 0xc70f6907)
      }
    } else {
      id.toLong
    }
    val unsignedValue = UnsignedLong.fromLongBits(hashValue)
    val partSize = UnsignedLong.fromLongBits(partitionSize)
    unsignedValue.mod(partSize).intValue + 1
  }

  def escapePropName(nebulaFields: List[String]): List[String] = {
    val propNames: ListBuffer[String] = new ListBuffer[String]
    for (key <- nebulaFields) {
      val sb = new StringBuilder()
      sb.append("`")
      sb.append(key)
      sb.append("`")
      propNames.append(sb.toString())
    }
    propNames.toList
  }

  def getAddressFromString(addr: String): HostAddress = {
    if (addr == null) {
      throw new IllegalArgumentException("wrong address format.")
    }
    var host: String = null
    var portString: String = null

    if (addr.startsWith("[")) {
      val hostAndPort = getHostAndPortFromBracketedHost(addr)
      host = hostAndPort._1
      portString = hostAndPort._2
    } else {
      val colonPos = addr.indexOf(":")
      if (colonPos >= 0 && addr.indexOf(":", colonPos + 1) == -1) {
        host = addr.substring(0, colonPos)
        portString = addr.substring(colonPos + 1)
      } else {
        host = addr
      }
    }

    var port = -1;
    if (!Strings.isNullOrEmpty(portString)) {
      for (c <- portString.toCharArray) {
        if (!Character.isDigit(c)) {
          throw new IllegalArgumentException(s"Port must be numeric: $addr")
        }
      }
      port = Integer.parseInt(portString)
      if (port < 0 || port > 65535) {
        throw new IllegalArgumentException(s"Port number out of range: $addr")
      }
    }
    new HostAddress(host, port)
  }

  def getHostAndPortFromBracketedHost(addr: String): (String, String) = {
    val colonIndex = addr.indexOf(":")
    val closeBracketIndex = addr.lastIndexOf("]")
    if (colonIndex < 0 || closeBracketIndex < colonIndex) {
      throw new IllegalArgumentException(s"invalid bracketed host/port: $addr")
    }
    val host: String = addr.substring(1, closeBracketIndex)
    if (closeBracketIndex + 1 == addr.length) {
      return (host, "")
    } else {
      if (addr.charAt(closeBracketIndex + 1) != ':') {
        throw new IllegalArgumentException(s"only a colon may follow a close bracket: $addr")
      }
      for (i <- closeBracketIndex + 2 until addr.length) {
        if (!Character.isDigit(addr.charAt(i))) {
          throw new IllegalArgumentException(s"Port must be numeric: $addr")
        }
      }
    }
    (host, addr.substring(closeBracketIndex + 2))
  }

  def getFieldNameToTypeSeq(metaProvider: MetaProvider, space: String, label: String): Seq[(String, Int)] = {
    val dataType: Type.Value = metaProvider.getLabelType(space, label)
    if (dataType == null) {
      throw new IllegalArgumentException(s"label $label does not exist.")
    }
    val nameToTypeSeq = if (dataType == Type.VERTEX) {
      metaProvider.getTagSchemaSeq(space, label)
    } else {
      metaProvider.getEdgeSchemaSeq(space, label)
    }
    //    val sourceSchemaMap = new mutable.LinkedHashMap[String, Int]()
    //    nameToTypeSeq.foreach(kv => sourceSchemaMap += kv._1 -> kv._2)
    //    sourceSchemaMap.toMap
    nameToTypeSeq
  }

  private def getVersionSchema(metaProvider: MetaProvider, space: String, label: String): (Schema, Long) = {
    val dataType: Type.Value = metaProvider.getLabelType(space, label)
    if (dataType == Type.VERTEX) {
      val item = metaProvider.getTagItem(space, label)
      (item.getSchema, item.version)
    } else {
      val item = metaProvider.getEdgeItem(space, label)
      (item.getSchema, item.version)
    }
  }

  private def getTagVersionSchema(item: TagItem): (Schema, Long) = {
    (item.getSchema, item.version)
  }

  private def getEdgeVersionSchema(item: EdgeItem): (Schema, Long) = {
    (item.getSchema, item.version)
  }


  def getSchemaProvider(schema: Schema, version: Long, fields: Seq[String]): SchemaProvider = {
    val schemaProvider = new SchemaProviderImpl(version)
    import collection.JavaConverters._
    val columnsSeq=schema.getColumns().asScala.map(d=>new String(d.name)->d)
    val nameToColumnMap = columnsSeq.toMap
    val useFields = if(fields!=null&&fields.nonEmpty) fields else columnsSeq.map(_._1)
    for(field<-useFields){
      val col = nameToColumnMap(field)
      val colName = new String(col.name)
      val colType = col.getType();
      val colTypeValue = colType.`type`.getValue()
      val nullable = col.isSetNullable() && col.isNullable();
      val hasDefault = col.isSetDefault_value();
      val len = if (colType.isSetType_length()) {
        colType.getType_length
      } else {
        0
      }
      val geoShape = if (colType.isSetGeo_shape()) {
        colType.getGeo_shape()
      } else {
        GeoShape.ANY
      }
      val defValue: Array[Byte] = if (hasDefault) {
        col.getDefault_value()
      } else {
        null
      }
      schemaProvider.addField(colName, colTypeValue, len, nullable, defValue, geoShape.getValue())
    }
    schemaProvider
  }

  def getSchemaProvider(metaProvider: MetaProvider, space: String, label: String, fields: Seq[String]): SchemaProvider = {
    val (schema, version) = getVersionSchema(metaProvider, space, label)
    getSchemaProvider(schema, version, fields)
  }


}
