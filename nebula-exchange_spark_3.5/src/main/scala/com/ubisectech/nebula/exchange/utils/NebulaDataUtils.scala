/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.nebula.exchange.utils

import com.vesoft.nebula._
import com.vesoft.nebula.encoder.SchemaProvider
import org.apache.spark.sql.types.{DataTypes, StringType, StructField}

import scala.collection.JavaConverters._

object NebulaDataUtils {

  def getClassByType(t: Int): Class[_] = {
    PropertyType.findByValue(t) match {
      case PropertyType.UNKNOWN =>
        throw new IllegalArgumentException("date type in nebula is UNKNOWN.")
      case PropertyType.STRING | PropertyType.FIXED_STRING => classOf[String]
      case PropertyType.BOOL => classOf[Boolean]
      case PropertyType.DOUBLE => classOf[Double]
      case PropertyType.FLOAT => classOf[Float]
      case PropertyType.INT8 => classOf[Byte]
      case PropertyType.INT16 => classOf[Short]
      case PropertyType.INT32 => classOf[Int]
      case PropertyType.INT64 | PropertyType.VID => classOf[Long]
      case PropertyType.DATE => classOf[com.vesoft.nebula.Date]
      case PropertyType.TIME => classOf[com.vesoft.nebula.Time]
      case PropertyType.DATETIME => classOf[com.vesoft.nebula.DateTime]
      case PropertyType.TIMESTAMP => classOf[Long]
      case PropertyType.GEOGRAPHY => classOf[com.vesoft.nebula.Geography]
      case PropertyType.DURATION => classOf[com.vesoft.nebula.Duration]
      case _ => throw new IllegalArgumentException("date type in nebula is UNKNOWN.")
    }
  }

  private def dateToNebulaString(d: com.vesoft.nebula.Date): String = {
    //yyyy-MM-dd
    new StringBuilder(10)
      .append(d.getYear).append("-")
      .append(d.getMonth).append("-")
      .append(d.getDay)
      .toString()
  }

  private def timeToNebulaString(d: com.vesoft.nebula.Time): String = {
    //02:59:40.000000
    new StringBuilder(15)
      .append(d.getHour).append(":")
      .append(d.getMinute).append(":")
      .append(d.getSec).append(".")
      .append(d.getMicrosec)
      .toString()
  }

  private def dateTimeToNebulaString(d: com.vesoft.nebula.DateTime): String = {
    //2021-03-17T17:53:59.000000
    new StringBuilder(26)
      .append(d.getYear).append("-")
      .append(d.getMonth).append("-")
      .append(d.getDay).append("T")
      .append(d.getHour).append(":")
      .append(d.getMinute).append(":")
      .append(d.getSec).append(".")
      .append(d.getMicrosec)
      .toString()
  }

  private def geographyToNebulaString(geog: com.vesoft.nebula.Geography): String = {
    val sb = new StringBuilder()
    geog.getSetField() match {
      case com.vesoft.nebula.Geography.PTVAL =>
        //POINT(120.12 30.16)
        val point = geog.getPtVal
        val coord = point.getCoord
        sb.append("POINT(").append(coord.getX).append(" ").append(coord.getY).append(")")
      case com.vesoft.nebula.Geography.LSVAL =>
        //LINESTRING(4 1, 4.2 72.23)
        val line = geog.getLsVal
        val coordList = line.getCoordList.asScala
        if (coordList.nonEmpty) {
          sb.append("LINESTRING(")
          coordList.foreach(coord => {
            sb.append(coord.getX).append(" ").append(coord.getY).append(",")
          })
          sb.delete(sb.length - 1, sb.length)
          sb.append(")")
        }
      case com.vesoft.nebula.Geography.PGVAL =>
        //POLYGON((75.3 45.4, 112.5 53.6, 122.7 25.5, 93.9 28.6, 75.3 45.4))
        val polygon = geog.getPgVal
        val coordListList = polygon.getCoordListList.asScala.map(_.asScala)
        if (coordListList.nonEmpty) {
          sb.append("POLYGON((")
          coordListList.foreach(coordList => {
            coordList.foreach(coord => {
              sb.append(coord.getX).append(" ").append(coord.getY).append(",")
            })
          })
          sb.delete(sb.length - 1, sb.length)
          sb.append("))")
        }
      case _ =>
    }
    sb.toString()
  }


  def nebulaDataToNebulaString(value: Any): String = {
    if (value == null) {
      return null.asInstanceOf[String]
    }
    value match {
      case d: Boolean => if (d) "true" else "false"
      case d: com.vesoft.nebula.Date => dateToNebulaString(d)
      case d: com.vesoft.nebula.Time => timeToNebulaString(d)
      case d: com.vesoft.nebula.DateTime => dateTimeToNebulaString(d)
      case d: com.vesoft.nebula.Geography => geographyToNebulaString(d)
      case _ => value.toString
    }
  }


  private def nebulaGeographyToString(geog: Geography): String = {
    val sb = new StringBuffer()
    geog.getSetField match {
      case com.vesoft.nebula.Geography.PTVAL =>
        //POINT(120.12 30.16)
        val point = geog.getPtVal
        val coord = point.getCoord
        sb.append(coord.getX).append(" ").append(coord.getY)
      case com.vesoft.nebula.Geography.LSVAL =>
        //LINESTRING(4 1, 4.2 72.23)
        val line = geog.getLsVal
        val coordList = line.getCoordList.asScala
        if (coordList.nonEmpty) {
          val coordStr = coordList.map(coord => coord.getX + " " + coord.getY).mkString(",")
          sb.append(coordStr)
        }
      case com.vesoft.nebula.Geography.PGVAL =>
        //POLYGON((75.3 45.4, 112.5 53.6, 122.7 25.5, 93.9 28.6, 75.3 45.4))
        val polygon = geog.getPgVal
        val coordListList = polygon.getCoordListList.asScala.map(_.asScala)
        if (coordListList.nonEmpty) { //不确定是否可以存多个多边形
          val coordListStr = coordListList.map(coordList => coordList.map(coord => coord.getX + " " + coord.getY).mkString(",")).mkString("(", "),(", ")")
          sb.append(coordListStr)
        }
    }
    sb.toString
  }

  //value是可为空字段的默认值?
  private def nebulaValueToString(value: Value): String = {
    value.getSetField() match {
      case Value.NVAL => ""
      case Value.BVAL =>
        if (value.isBVal()) "1" else "0"
      case Value.IVAL =>
        value.getIVal().toString
      case Value.FVAL =>
        value.getFVal() toString
      case Value.SVAL =>
        value.getSVal().toString
      case Value.TVAL =>
        value.getTVal().toString()
      case Value.DVAL =>
        value.getDVal().toString()
      case Value.DTVAL =>
        value.getDtVal().toString()
      case Value.GGVAL =>
        nebulaGeographyToString(value.getGgVal())
      case _ =>
        throw new RuntimeException(
          "Unknown value: " + value.getFieldValue().getClass());
    }
  }


  private def nebulaDateToString(d: Date): String = {
    //yyyy-MM-dd
    new StringBuilder(10)
      .append(d.getYear).append("-")
      .append(d.getMonth).append("-")
      .append(d.getDay)
      .toString()
  }

  private def stringToNebulaDate(d: String): Date = {
    //yyyy-MM-dd
    val Array(hh, mm, dd) = d.split("-")
    new Date(hh.toShort, mm.toByte, dd.toByte)
  }

  private def nebulaTimeToString(d: Time): String = {
    //02:59:40.000000
    new StringBuilder(15)
      .append(d.getHour).append(":")
      .append(d.getMinute).append(":")
      .append(d.getSec).append(".")
      .append(d.getMicrosec)
      .toString()
  }

  private def stringToNebulaTime(d: String): Time = {
    //02:59:40.000000
    val index = d.indexOf(".")
    val t = new Time()
    var tStr = d
    if (index > 0) {
      tStr = d.slice(0, index)
      val microSec = d.slice(index + 1, d.length)
      t.setMicrosec(microSec.toByte)
    }
    val Array(hh, mm, ss) = tStr.split(":")
    t.setHour(hh.toByte)
    t.setMinute(mm.toByte)
    t.setSec(ss.toByte)
    t
  }

  private def nebulaDateTimeToString(d: DateTime): String = {
    //2021-03-17T17:53:59.000000
    new StringBuilder(26)
      .append(d.getYear).append("-")
      .append(d.getMonth).append("-")
      .append(d.getDay).append("T")
      .append(d.getHour).append(":")
      .append(d.getMinute).append(":")
      .append(d.getSec).append(".")
      .append(d.getMicrosec)
      .toString()
  }

  private def stringToNebulaDateTime(d: String): DateTime = {
    //2021-03-17T17:53:59.000000
    val Array(dateStr, timeStr) = d.split("T")
    val date = stringToNebulaDate(dateStr)
    val time = stringToNebulaTime(timeStr)
    new DateTime(date.getYear, date.getMonth, date.getDay, time.getHour, time.getMinute, time.getSec, time.getMicrosec)
  }

  private def stringToCoordinate(d: String): Coordinate = {
    val Array(x, y) = d.split(" ")
    new Coordinate(x.toDouble, y.toDouble)
  }

  private def stringToNebulaGeography(d: String, field: SchemaProvider.Field): Geography = {
    //2021-03-17T17:53:59.000000
    val leftK = d.indexOf("(")
    val geo = new Geography
    if (leftK != -1) {
      //      POLYGON
      val datas = d.tail.init.split("\\),\\(")
      val coordListList = new java.util.ArrayList[java.util.List[Coordinate]]()
      datas.foreach(pointsStr => {
        val points = pointsStr.split(",")
        val coordList = points.map(d => stringToCoordinate(d)).toList.asJava
        coordListList.add(coordList)
      })
      geo.setPgVal(new Polygon(coordListList))
    } else {
      val datas = d.split(",")
      if (datas.length <= 1) {
        //POINT
        val coord = stringToCoordinate(datas.head)
        geo.setPtVal(new Point(coord))
      } else{
        //      LINESTRING
        val coords = datas.map(d => stringToCoordinate(d)).toList.asJava
        geo.setLsVal(new LineString(coords))
      }
    }
    geo
  }


  private def nebulaDataToString(name: String, value: Any, schemaProvider: SchemaProvider): String = {
    //val field = schemaProvider.field(name)
    value match {
      case d: Value => nebulaValueToString(d)
      case d: Boolean => if (d) "true" else "false"
      case d: Date => nebulaDateToString(d)
      case d: Time => nebulaTimeToString(d)
      case d: DateTime => nebulaDateTimeToString(d)
      case d: Geography => nebulaGeographyToString(d)
      case _ => value.toString
    }
  }


  /**
   * @param tag    the TagItem
   * @param names  the property names
   * @param values the property values
   * @return the encode String
   * @throws RuntimeException expection
   */
  @throws[RuntimeException]
  def nebulaDataToString(names: Seq[String], values: Seq[AnyRef], schemaProvider: SchemaProvider): Seq[String] = {
    if (names.size != values.size) {
      throw new RuntimeException(
        s"The names' size no equal with values' size, [${names.size}] != [${values.size}]")
    }

    names.zip(values).map(nv => {
      val (name, value) = nv
      nebulaDataToString(name, value, schemaProvider)
    })
  }

  def stringToNebulaData(d: String, field: SchemaProvider.Field): Any = {
    val fieldType = field.`type`()
    PropertyType.findByValue(fieldType) match {
      case PropertyType.UNKNOWN =>
        throw new IllegalArgumentException("date type in nebula is UNKNOWN.")
      case PropertyType.STRING | PropertyType.FIXED_STRING => d
      case PropertyType.BOOL => d.toBoolean
      case PropertyType.DOUBLE => d.toDouble
      case PropertyType.FLOAT => d.toFloat
      case PropertyType.INT8 => d.toByte
      case PropertyType.INT16 => d.toShort
      case PropertyType.INT32 => d.toInt
      case PropertyType.INT64 | PropertyType.VID => d.toLong
      case PropertyType.DATE => stringToNebulaDate(d)
      case PropertyType.TIME => stringToNebulaTime(d)
      case PropertyType.DATETIME => stringToNebulaDateTime(d)
      case PropertyType.TIMESTAMP => d.toLong
      case PropertyType.GEOGRAPHY => stringToNebulaGeography(d, field)
      case _ => throw new IllegalArgumentException("date type in nebula is UNKNOWN.")
    }
  }

  def getStringStructField(schemaProvider: SchemaProvider, fieldName: String): StructField = {
    val field = schemaProvider.field(fieldName)
    DataTypes.createStructField(fieldName, StringType, field.nullable())
  }

  def getStructField(schemaProvider: SchemaProvider, fieldName: String): StructField = {
    val field = schemaProvider.field(fieldName)
    val fieldType = field.`type`()
    val t = PropertyType.findByValue(fieldType) match {
      case PropertyType.STRING | PropertyType.FIXED_STRING => DataTypes.StringType
      case PropertyType.BOOL => DataTypes.BooleanType
      case PropertyType.DOUBLE => DataTypes.DoubleType
      case PropertyType.FLOAT => DataTypes.FloatType
      case PropertyType.INT8 => DataTypes.ByteType
      case PropertyType.INT16 => DataTypes.ShortType
      case PropertyType.INT32 => DataTypes.IntegerType
      case PropertyType.INT64 | PropertyType.VID => DataTypes.LongType
      case PropertyType.DATE => DataTypes.StringType
      case PropertyType.TIME => DataTypes.StringType
      case PropertyType.DATETIME => DataTypes.StringType
      case PropertyType.TIMESTAMP => DataTypes.StringType
      case PropertyType.GEOGRAPHY => DataTypes.StringType
      case _ => throw new IllegalArgumentException("date type in nebula is UNKNOWN.")
    }
    DataTypes.createStructField(fieldName, t, field.nullable())
  }
}
