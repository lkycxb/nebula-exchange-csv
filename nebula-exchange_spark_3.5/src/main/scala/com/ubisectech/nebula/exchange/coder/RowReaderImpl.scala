/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.nebula.exchange.coder

import com.vesoft.nebula._
import com.vesoft.nebula.encoder.SchemaProvider

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RowReaderImpl(byteOrder: ByteOrder, nebulaKeys: List[String], schema: SchemaProvider) extends RowReader {

  //  val andBits: Array[Int] = Array(0x7F, 0xBF, 0xDF, 0xEF, 0xF7, 0xFB, 0xFD, 0xFE)
  //  val orBits: Array[Int] = Array(0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01)
  val nullBits: Array[Int] = Array(0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01)

  var buff: ByteBuffer = _
  var buffSize: Int = _
  var version: Long = 0
  var headerLen = 0
  var offset = 0
  var numNullBytes = 0

  var schemaSize = 0
  var writeBuffSize = 0
  var writeStrOffset = 0


  /**
   * @see com.vesoft.nebula.encoder.RowWriterImpl.convertGeographyToJTSGeometry
   * @param header
   * @return
   */
  private def convertJTSGeometryToGeography(geo: org.locationtech.jts.geom.Geometry): Geography = {
    //val geomFactory = new GeometryFactory()
    geo.getGeometryType match {
      case org.locationtech.jts.geom.Geometry.TYPENAME_LINESTRING => {
        val lineString = geo.asInstanceOf[org.locationtech.jts.geom.LineString]
        val coordList = new java.util.ArrayList[Coordinate](lineString.getCoordinates.size)
        lineString.getCoordinates.foreach(coord => {
          coordList.add(new Coordinate(coord.getX, coord.getY))
        })
        val geog = new Geography
        geog.setLsVal(new LineString(coordList))
        geog
      }
      case org.locationtech.jts.geom.Geometry.TYPENAME_POLYGON => {
        val polygon = geo.asInstanceOf[org.locationtech.jts.geom.Polygon]
        val coordListList = new java.util.ArrayList[java.util.List[Coordinate]]()
        val coordList = new java.util.ArrayList[Coordinate]()
        coordListList.add(coordList)
        val coordes = polygon.getCoordinates
        coordes.foreach(coord => {
          coordList.add(new Coordinate(coord.getX, coord.getY))
        })
        val geog = new Geography
        geog.setPgVal(new Polygon(coordListList))
        geog
      }
      case org.locationtech.jts.geom.Geometry.TYPENAME_LINEARRING => {
        val linearRing = geo.asInstanceOf[org.locationtech.jts.geom.LinearRing]
        val coordList = new java.util.ArrayList[Coordinate](linearRing.getCoordinates.size)
        linearRing.getCoordinates.foreach(coord => {
          coordList.add(new Coordinate(coord.getX, coord.getY))
        })
        val geog = new Geography
        geog.setLsVal(new LineString(coordList))
        geog
      }
      case _ => {
        val x = geo.getCoordinate.getX
        val y = geo.getCoordinate.getY
        val geog = new Geography
        geog.setPtVal(new Point(new Coordinate(x, y)))
        geog
      }
    }
  }

  private def parseHeadLen(header: Byte): (Int, Int) = {
    var headerLen = 0
    var schemaVerSize = 0
    header match {
      case 0x09 =>
        headerLen = 2
        schemaVerSize = 1
      case 0x0A =>
        headerLen = 3
        schemaVerSize = 2
      case 0x0B =>
        headerLen = 4
        schemaVerSize = 3
      case 0x0C =>
        headerLen = 5
        schemaVerSize = 4
      case 0x0D =>
        headerLen = 6
        schemaVerSize = 5
      case 0x0E =>
        headerLen = 7
        schemaVerSize = 6
      case 0x0F =>
        headerLen = 8
        schemaVerSize = 7
      case _ =>
        headerLen = 1
        schemaVerSize = 0
    }
    (headerLen, schemaVerSize)
  }

  private def checkNullBit(pos: Int): Boolean = {
    val offset = headerLen + (pos >> 3)
    val nb = (pos & 0x0000000000000007L).toInt
    val flag = buff.get(offset) & nullBits(nb)
    flag != 0;
  }


  def init(buff: ByteBuffer): Unit = {
    //buff size : headerLen + numNullBytes + schema.size() + Long.BYTES
    //temp size : headerLen + numNullBytes + schema.size() + approxStrLen + Long.BYTES
    //str size :  buf.array().length - Long.BYTES
    this.buff = buff
    this.buffSize = buff.array().size
    val header = buff.get(offset) //8
    offset = 1
    val (headerLen, schemaVerSize) = parseHeadLen(header) //1,0
    this.headerLen = headerLen

    if (header != 0x08) {
      version = buff.getLong(offset)
      offset += Integer.BYTES
    }
    val numNullables = schema.getNumNullableFields();
    if (numNullables > 0) {
      numNullBytes = ((numNullables - 1) >> 3) + 1
    }
    schemaSize = schema.size()
    //headerLen + numNullBytes + schema.size() + Long.BYTES
    writeBuffSize = headerLen + numNullBytes + schema.size() + java.lang.Long.BYTES
    writeStrOffset = writeBuffSize - java.lang.Long.BYTES
  }


  def reader(): Seq[(String,Any)] = {
    var strNum = 0
    val valueSeq = new ArrayBuffer[ (String,Any)]

   val outOfSpaceNameSeq = nebulaKeys.filter(d=> {
      val typeEnum=PropertyType.findByValue(schema.field(d).`type`())
      typeEnum == PropertyType.STRING || typeEnum == PropertyType.GEOGRAPHY
    } )

    for (fieldName <- nebulaKeys) {
      val field = schema.field(fieldName)
      val typeEnum = PropertyType.findByValue(field.`type`())
      if (typeEnum != null) {
        val offset = headerLen + numNullBytes + field.offset()
        val v = typeEnum match {
          case PropertyType.BOOL | PropertyType.INT8 => {
            buff.get(offset)
          }
          case PropertyType.INT16 => {
            buff.getShort(offset)
          }
          case PropertyType.INT32 => {
            buff.getInt(offset)
          }
          case PropertyType.INT32 => {
            buff.getInt(offset)
          }
          case PropertyType.INT64 | PropertyType.TIMESTAMP => {
            buff.getLong(offset)
          }
          case PropertyType.FIXED_STRING => {
            new String(buff.array.slice(offset, offset + field.size())).trim
          }
          case PropertyType.FLOAT => {
            buff.getFloat(offset)
          }
          case PropertyType.DOUBLE => {
            buff.getDouble(offset)
          }
          case PropertyType.DATE => {
            //yyyy-MM-dd
            val year = buff.getShort(offset)
            val month = buff.get(offset + java.lang.Short.BYTES)
            val day = buff.get(offset + java.lang.Short.BYTES + java.lang.Byte.BYTES)
            new Date(year, month, day)
          }
          case PropertyType.TIME => {
            //02:59:40.000000
            val hour = buff.get(offset)
            val minute = buff.get(offset + java.lang.Byte.BYTES)
            val sec = buff.get(offset + 2 * java.lang.Byte.BYTES)
            val microsec = buff.getInt(offset + 3 * java.lang.Byte.BYTES)
            new Time(hour, minute, sec, microsec)
          }
          case PropertyType.DATETIME => {
            //2021-03-17T17:53:59.000000
            val year = buff.getShort(offset)
            val month = buff.get(offset + java.lang.Short.BYTES)
            val day = buff.get(offset + java.lang.Short.BYTES + java.lang.Byte.BYTES)
            val hour = buff.get(offset + java.lang.Short.BYTES + 2 * java.lang.Byte.BYTES)
            val minute = buff.get(offset + java.lang.Short.BYTES + 3 * java.lang.Byte.BYTES)
            val sec = buff.get(offset + java.lang.Short.BYTES + 4 * java.lang.Byte.BYTES)
            val microsec = buff.getInt(offset + java.lang.Short.BYTES + 5 * java.lang.Byte.BYTES)
            new DateTime(year, month, day, hour, minute, sec, microsec)
          }
          case PropertyType.STRING | PropertyType.GEOGRAPHY => {
            if (field.nullable() && checkNullBit(field.nullFlagPos())) {
              // Null string
              // Set the new offset and length
              val strOffset = buff.getInt(offset)
              val len = buff.getInt(offset + Integer.BYTES)
              typeEnum match {
                case PropertyType.STRING => null
                case _ => null.asInstanceOf[Geography]
              }
            } else {
              strNum += 1
              val strOffset = buff.getInt(offset)
              val len = buff.getInt(offset + Integer.BYTES)
              //error IndexOutOfBoundsException
              //val strBs = new Array[Byte](len)
              //buff.get(strBs, strOffset, len)
              val bs = buff.array.slice(strOffset, strOffset + len)
              typeEnum match {
                case PropertyType.STRING => {
                  val str = new String(bs)
                  //println(s"test name:${field.name()},value:${str}")
                  str
                }
                case PropertyType.GEOGRAPHY => {
                  val g = new org.locationtech.jts.io.WKBReader().read(bs)
                  convertJTSGeometryToGeography(g)
                }
              }
            }
          }
          case _ => {
            null
          }
        }
        //println(s"name:${field.name()},type:${field.`type`()},value:${v}")
        valueSeq += fieldName -> v
      }
    }
    valueSeq
  }
}
