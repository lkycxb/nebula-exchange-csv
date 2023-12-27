/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.ubisectech.nebula.exchange.reader

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.ubisectech.nebula.exchange.common.config.FileBaseSourceConfigEntry
import com.ubisectech.nebula.exchange.common.utils.NebulaUtils.DEFAULT_EMPTY_VALUE
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * The FileBaseReader is the abstract class for HDFS file reader.
 *
 * @param session
 * @param path
 */
abstract class FileBaseReader(val session: SparkSession, val path: String) extends Reader {

  require(path.trim.nonEmpty)

  override def close(): Unit = {
    session.close()
  }
}

/**
 * The ParquetReader extend the FileBaseReader and support read parquet file from HDFS.
 *
 * @param session
 * @param parquetConfig
 */
class SstReader(override val session: SparkSession, sstConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, sstConfig.path) {

  import com.ubisectech.sst.connector._

  override def read(): DataFrame = {
    //import com.ubisectech.
    //TODO
    //session.read.csv(path)

    val hadoopConf = session.sparkContext.hadoopConfiguration
    val pathfs = new Path(path)
    val dfs = pathfs.getFileSystem(hadoopConf)
    val fileStatus = dfs.getFileStatus(pathfs)
    if (fileStatus.isDirectory) {
      val iterator = dfs.listFiles(pathfs, true)
      val pathBuff = new ArrayBuffer[String]()
      while (iterator.hasNext) {
        val fs = iterator.next()
        if (fs.isFile && fs.getLen > 0L && fs.getPath.getName.endsWith(".sst")) {
          pathBuff += fs.getPath.toString
        }
      }
      val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
      val paths = objectMapper.writeValueAsString(pathBuff)
      session.read.option("paths", paths).fromSst()
    } else {
      session.read.option("path", path).fromSst()
    }
  }
}

/**
 * The ParquetReader extend the FileBaseReader and support read parquet file from HDFS.
 *
 * @param session
 * @param parquetConfig
 */
class ParquetReader(override val session: SparkSession, parquetConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, parquetConfig.path) {

  override def read(): DataFrame = {
    session.read.parquet(path)
  }
}

/**
 * The ORCReader extend the FileBaseReader and support read orc file from HDFS.
 *
 * @param session
 * @param orcConfig
 */
class ORCReader(override val session: SparkSession, orcConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, orcConfig.path) {

  override def read(): DataFrame = {
    session.read.orc(path)
  }
}

/**
 * The JSONReader extend the FileBaseReader and support read json file from HDFS.
 *
 * @param session
 * @param jsonConfig
 */
class JSONReader(override val session: SparkSession, jsonConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, jsonConfig.path) {

  override def read(): DataFrame = {
    session.read.json(path)
  }
}

/**
 * The CSVReader extend the FileBaseReader and support read csv file from HDFS.
 * All types of the structure are StringType.
 *
 * @param session
 * @param csvConfig
 */
class CSVReader(override val session: SparkSession, csvConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, csvConfig.path) {

  override def read(): DataFrame = {
    session.read
      .option("delimiter", csvConfig.separator.get)
      .option("header", csvConfig.header.get)
      .option("emptyValue", DEFAULT_EMPTY_VALUE)
      .csv(path)
  }
}

/**
 * The CustomReader extend the FileBaseReader and support read text file from HDFS.
 * Transformation is a function convert a line into Row.
 * The structure of the row should be specified.
 *
 * @param session
 * @param customConfig
 * @param transformation
 * @param structType
 */
abstract class CustomReader(override val session: SparkSession,
                            customConfig: FileBaseSourceConfigEntry,
                            transformation: String => Row,
                            filter: Row => Boolean,
                            structType: StructType)
  extends FileBaseReader(session, customConfig.path) {

  override def read(): DataFrame = {
    val encoder = RowEncoder.encoderFor(structType)
    session.read
      .text(path)
      .filter(!_.getString(0).isEmpty)
      .map(row => transformation(row.getString(0)))(encoder)
      .filter(filter)
  }
}
