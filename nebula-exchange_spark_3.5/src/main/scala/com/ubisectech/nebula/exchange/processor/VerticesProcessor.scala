/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.ubisectech.nebula.exchange.processor

import com.ubisectech.nebula.exchange.TooManyErrorsException
import com.ubisectech.nebula.exchange.coder.NebulaDecodecImpl
import com.ubisectech.nebula.exchange.common.common.{KeyPolicy, Vertex, Vertices}
import com.ubisectech.nebula.exchange.common.config._
import com.ubisectech.nebula.exchange.common.processor.Processor
import com.ubisectech.nebula.exchange.common.utils.NebulaUtils
import com.ubisectech.nebula.exchange.common.utils.NebulaUtils.DEFAULT_EMPTY_VALUE
import com.ubisectech.nebula.exchange.common.writer.{GenerateSstFile, NebulaGraphClientWriter}
import com.ubisectech.nebula.exchange.common.{ErrorHandler, GraphProvider, MetaProvider, VidType}
import com.ubisectech.nebula.exchange.utils.NebulaDataUtils
import com.vesoft.nebula.encoder.{NebulaCodecImpl, SchemaProvider}
import com.vesoft.nebula.meta.TagItem
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkEnv, TaskContext}

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 *
 * @param data
 * @param tagConfig
 * @param fieldKeys
 * @param nebulaKeys
 * @param config
 * @param batchSuccess
 * @param batchFailure
 */
class VerticesProcessor(spark: SparkSession,
                        data: DataFrame,
                        tagConfig: TagConfigEntry,
                        fieldKeys: List[String],
                        nebulaKeys: List[String],
                        config: Configs,
                        batchSuccess: LongAccumulator,
                        batchFailure: LongAccumulator)
  extends Processor {
  @transient
  private[this] lazy val LOG = Logger.getLogger(this.getClass)
  val vertexId = com.ubisectech.nebula.common.connector.NebulaUtils.vertexId

  override def process(): Unit = {
    val address = config.databaseConfig.getMetaAddress
    val space = config.databaseConfig.space
    val timeout = config.connectionConfig.timeout
    val retry = config.connectionConfig.retry
    val sslConfig = config.sslConfig
    val metaProvider = new MetaProvider(address, timeout, retry, sslConfig)
    val fieldTypeMap = NebulaUtils.getDataSourceFieldType(tagConfig, space, metaProvider)
    val isVidStringType = metaProvider.getVidType(space) == VidType.STRING
    val partitionNum = metaProvider.getPartNumber(space)
    if (tagConfig.dataSinkConfigEntry.category == SinkCategory.SST) {
      writeToSst(metaProvider, space, partitionNum, fieldTypeMap)
    } else if (tagConfig.dataSinkConfigEntry.category == SinkCategory.CSV) {
      writeToCsv(metaProvider, space, partitionNum, fieldTypeMap)
    } else {
      writeToNebula(metaProvider, space, isVidStringType, fieldTypeMap)
    }
    metaProvider.close()
  }

  /**
   * filter and check row data for vertex, if streaming only print log
   * for not streaming datasource, if the vertex data is invalid, throw AssertException.
   */
  def isVertexValid(row: Row,
                    tagConfig: TagConfigEntry,
                    streamFlag: Boolean,
                    isVidStringType: Boolean): Boolean = {
    val index = row.schema.fieldIndex(tagConfig.vertexField)
    if (index < 0 || row.isNullAt(index)) {
      printChoice(streamFlag, s"vertexId must exist and cannot be null, your row data is $row")
      return false
    }
    if (!isVidStringType && (tagConfig.vertexPolicy.isEmpty && tagConfig.vertexPrefix != null)) {
      printChoice(streamFlag, s"space vidType is int, does not support prefix for vid")
    }

    val vertexId = row.get(index).toString
    // process int type vid
    if (tagConfig.vertexPolicy.isEmpty && !isVidStringType && !NebulaUtils.isNumic(vertexId)) {
      printChoice(
        streamFlag,
        s"space vidType is int, but your vertex id $vertexId is not numeric.your row data is $row")
      return false
    }
    // process string type vid
    if (tagConfig.vertexPolicy.isDefined && isVidStringType) {
      printChoice(
        streamFlag,
        s"only int vidType can use policy, but your vidType is FIXED_STRING.your row data is $row")
      return false
    }
    true
  }

  /**
   * Convert row data to {@link Vertex}
   */
  def convertToVertex(row: Row,
                      tagConfig: TagConfigEntry,
                      isVidStringType: Boolean,
                      fieldKeys: List[String],
                      fieldTypeMap: Map[String, Int]): Vertex = {
    val index = row.schema.fieldIndex(tagConfig.vertexField)
    var vertexId = row.get(index).toString.trim
    if (vertexId.equals(DEFAULT_EMPTY_VALUE)) {
      vertexId = ""
    }
    if (tagConfig.vertexPrefix != null) {
      vertexId = tagConfig.vertexPrefix + "_" + vertexId
    }

    if (tagConfig.vertexPolicy.isEmpty && isVidStringType) {
      vertexId = NebulaUtils.escapeUtil(vertexId).mkString("\"", "", "\"")
    }

    val values = for {
      property <- fieldKeys if property.trim.length != 0
    } yield extraValueForClient(row, property, fieldTypeMap)
    Vertex(vertexId, values)
  }

  /**
   * encode vertex
   */
  def encodeVertex(row: Row,
                   partitionNum: Int,
                   vidType: VidType.Value,
                   spaceVidLen: Int,
                   tagItem: TagItem,
                   fieldTypeMap: Map[String, Int]): (Array[Byte], Array[Byte]) = {
    val (orphanVertexKey, vertexKey, vertexValue) =
      getVertexKeyValue(row, partitionNum, vidType, spaceVidLen, tagItem, fieldTypeMap)
    (vertexKey, vertexValue)
  }

  /**
   * encode vertex for tagless
   */
  def encodeVertexForTageless(
                               row: Row,
                               partitionNum: Int,
                               vidType: VidType.Value,
                               spaceVidLen: Int,
                               tagItem: TagItem,
                               fieldTypeMap: Map[String, Int]): (Array[Byte], Array[Byte], Array[Byte]) = {
    getVertexKeyValue(row, partitionNum, vidType, spaceVidLen, tagItem, fieldTypeMap)
  }

  private def writeToSst(metaProvider: MetaProvider, space: String, partitionNum: Int, fieldTypeMap: Map[String, Int]): Unit = {
    val fileBaseConfig = tagConfig.dataSinkConfigEntry.asInstanceOf[FileBaseSinkConfigEntry]
    val namenode = fileBaseConfig.fsName.orNull
    val tagName = tagConfig.name
    val vidType = metaProvider.getVidType(space)
    val spaceVidLen = metaProvider.getSpaceVidLen(space)
    val tagItem = metaProvider.getTagItem(space, tagName)
    val emptyValue = ByteBuffer.allocate(0).array()
    var sstKeyValueData = if (tagConfig.enableTagless) {
      data.dropDuplicates(vertexId)
        .mapPartitions { iter =>
          iter.map { row =>
            encodeVertexForTageless(row,
              partitionNum,
              vidType,
              spaceVidLen,
              tagItem,
              fieldTypeMap)
          }
        }(Encoders.tuple(Encoders.BINARY, Encoders.BINARY, Encoders.BINARY))
        .flatMap(line => {
          List((line._1, emptyValue), (line._2, line._3))
        })(Encoders.tuple(Encoders.BINARY, Encoders.BINARY))
    } else {
      data.dropDuplicates(vertexId)
        .mapPartitions { iter =>
          iter.map { row =>
            encodeVertex(row, partitionNum, vidType, spaceVidLen, tagItem, fieldTypeMap)
          }
        }(Encoders.tuple(Encoders.BINARY, Encoders.BINARY))
    }

    // repartition dataframe according to nebula part, to make sure sst files for one part has no overlap
    if (tagConfig.repartitionWithNebula) {
      sstKeyValueData = customRepartition(spark, sstKeyValueData, partitionNum)
    } else {
      val partitionSize = data.sparkSession.sparkContext.defaultParallelism
      sstKeyValueData = sstKeyValueData.repartition(partitionSize)
    }
    sstKeyValueData
      .toDF("key", "value")
      .sortWithinPartitions("key")
      .foreachPartition { iterator: Iterator[Row] =>
        val generateSstFile = new GenerateSstFile
        generateSstFile.writeSstFiles(iterator.toSeq,
          fileBaseConfig,
          partitionNum,
          namenode,
          batchFailure,
          tagName)
      }
    val rowCount = sstKeyValueData.count()
    LOG.info(s"Export tag to Sst:${tagName} rowCount:${rowCount}")
  }

  private def writeToCsv(metaProvider: MetaProvider, space: String, partitionNum: Int, fieldTypeMap: Map[String, Int]): Unit = {
    val fileBaseConfig = tagConfig.dataSinkConfigEntry.asInstanceOf[FileBaseSinkConfigEntry]
    val tagName = tagConfig.name
    val vidType = metaProvider.getVidType(space)
    val tagItem = metaProvider.getTagItem(space, tagName)
    // val separator: String = '\001'.toString
    val schemaProvider = NebulaUtils.getSchemaProvider(tagItem.getSchema, tagItem.getVersion, fieldKeys)
    val st = com.ubisectech.nebula.common.connector.NebulaUtils.getDefaultSchema(true)
    val structFields = st.fields ++ fieldKeys.map(d => NebulaDataUtils.getStringStructField(schemaProvider, d))
    //fieldTypeMap.map(d => NebulaDataUtils.getStringStructField(schemaProvider, d._1)).toSeq
    val schema = StructType(structFields)
    val encoders = Encoders.row(schema)
    val separator = fileBaseConfig.separator.get
    val partitionSize = data.sparkSession.sparkContext.defaultParallelism
    val csvData = data.dropDuplicates(vertexId)
      .mapPartitions { iter =>
        val schemaProvider = NebulaUtils.getSchemaProvider(tagItem.getSchema, tagItem.getVersion, fieldKeys)
        iter.map { row =>
          val values: Array[Any] = getVertexToCsvString(row, vidType, fieldTypeMap, separator, schemaProvider)
          new GenericRowWithSchema(values, schema).asInstanceOf[Row]
        }
      }(encoders)
      .repartition(partitionSize)

    val remotePath = fileBaseConfig.remotePath + "/" + tagName
    csvData.write.format("csv").mode(SaveMode.Overwrite).save(remotePath)
    val rowCount = csvData.count()
    LOG.info(s"Export tag to Csv:${tagName} rowCount:${rowCount}")
  }

  private def dfByteToString(metaProvider: MetaProvider, space: String): DataFrame = {
    val tagName = tagConfig.name
    val tagItem = metaProvider.getTagItem(space, tagName)
    val schemaProvider = NebulaUtils.getSchemaProvider(tagItem.getSchema, tagItem.getVersion, nebulaKeys)
    val st = com.ubisectech.nebula.common.connector.NebulaUtils.getDefaultSchema(true)
    val structFields = st.fields ++ nebulaKeys.map(d => NebulaDataUtils.getStringStructField(schemaProvider, d))
    val schema = StructType(structFields)
    val encoders = Encoders.row(schema)
    val vidType = metaProvider.getVidType(space)
    val spaceVidLen = metaProvider.getSpaceVidLen(space)

    data.mapPartitions(iter => {
      val schemaProvider = NebulaUtils.getSchemaProvider(tagItem.getSchema, tagItem.getVersion, nebulaKeys)
      val decodecImpl = new NebulaDecodecImpl(nebulaKeys, schemaProvider)
      iter.map(row => {
        val keyBs = row.getAs[Array[Byte]]("key")
        val keyData = decodecImpl.parseTagKey(spaceVidLen, vidType, keyBs)
        val valueBs = row.getAs[Array[Byte]]("value")
        if (keyData.vid == "2053037734897888931") {
          println(keyData)
        }
        val valueData = decodecImpl.parseTagValue(valueBs)
        val values: Array[Any] = (Array(keyData.vid) ++ valueData.values).map(NebulaDataUtils.nebulaDataToNebulaString)
        new GenericRowWithSchema(values, schema).asInstanceOf[Row]
      })
    })(encoders)
  }

  private def dfCsvToString(metaProvider: MetaProvider, space: String): DataFrame = {
    val tagName = tagConfig.name
    val tagItem = metaProvider.getTagItem(space, tagName)
    val schemaProvider = NebulaUtils.getSchemaProvider(tagItem.getSchema, tagItem.getVersion, nebulaKeys)
    val st = com.ubisectech.nebula.common.connector.NebulaUtils.getDefaultSchema(true)
    val structFields = st.fields ++ nebulaKeys.map(d => NebulaDataUtils.getStringStructField(schemaProvider, d))
    val defFields = st.fields.map(_.name)
    val schema = StructType(structFields)
    val fields = schema.map(_.name)
    val encoders = Encoders.row(schema)
    data.toDF(fields: _*).mapPartitions(iter => {
      val schemaProvider = NebulaUtils.getSchemaProvider(tagItem.getSchema, tagItem.getVersion, nebulaKeys)
      iter.map(row => {
        val values = new Array[Any](fields.size)
        var index = 0
        for (_ <- defFields) {
          val value = row.get(index)
          values(index) = value
          index += 1
        }
        for (name <- nebulaKeys) {
          val valueStr = row.getAs[String](name)
          val field = schemaProvider.field(name)
          val value = NebulaDataUtils.stringToNebulaData(valueStr, field)
          val nebulaStr = NebulaDataUtils.nebulaDataToNebulaString(value)
          values(index) = nebulaStr
          index += 1
        }
        val row1 = new GenericRowWithSchema(values, schema).asInstanceOf[Row]
        row1
      })
    })(encoders)
  }

  private def writeToNebula(metaProvider: MetaProvider, space: String, isVidStringType: Boolean, fieldTypeMap: Map[String, Int]): Unit = {
    val streamFlag = data.isStreaming
    val df = if (tagConfig.dataSourceConfigEntry.category == SourceCategory.SST) {
      dfByteToString(metaProvider, space)
    } else if (tagConfig.dataSourceConfigEntry.category == SourceCategory.CSV) {
      dfCsvToString(metaProvider, space)
    } else {
      data
    }

    val vertices = df.filter(row => isVertexValid(row, tagConfig, streamFlag, isVidStringType))
      .map { row =>
        convertToVertex(row, tagConfig, isVidStringType, nebulaKeys, fieldTypeMap)
      }(Encoders.kryo[Vertex])

    // streaming write
    if (streamFlag) {
      val streamingDataSourceConfig =
        tagConfig.dataSourceConfigEntry.asInstanceOf[StreamingDataSourceConfigEntry]
      val wStream = vertices.writeStream
      if (tagConfig.checkPointPath.isDefined)
        wStream.option("checkpointLocation", tagConfig.checkPointPath.get)
      wStream
        .foreachBatch((vertexSet: Dataset[Vertex], batchId: Long) => {
          LOG.info(s"${tagConfig.name} tag start batch ${batchId}.")
          vertexSet.foreachPartition(processEachPartition _)
        })
        .trigger(Trigger.ProcessingTime(s"${streamingDataSourceConfig.intervalSeconds} seconds"))
        .start()
        .awaitTermination()
    } else {
      vertices.foreachPartition(processEachPartition _)
    }
    val rowCount = vertices.count()
    val tagName = tagConfig.name
    LOG.info(s"Import tag to Nebula:${tagName} rowCount:${rowCount}")
  }

  private def processEachPartition(iterator: Iterator[Vertex]): Unit = {
    val graphProvider =
      new GraphProvider(config.databaseConfig.getGraphAddress,
        config.connectionConfig.timeout,
        config.sslConfig)

    val writer = new NebulaGraphClientWriter(config.databaseConfig,
      config.userConfig,
      config.rateConfig,
      tagConfig,
      graphProvider)

    val errorBuffer = ArrayBuffer[String]()

    writer.prepare()
    // batch write tags
    val startTime = System.currentTimeMillis
    iterator.grouped(tagConfig.batch).foreach { vertex =>
      val inValidSeq=vertex.filter(d=>d.values(0).toString=="2053037734897888931")
      if(inValidSeq.nonEmpty){
        println(inValidSeq.head)
      }
      val vertices = Vertices(nebulaKeys, vertex.toList, tagConfig.vertexPolicy)
      val failStatement = writer.writeVertices(vertices, tagConfig.ignoreIndex)
      if (failStatement == null) {
        batchSuccess.add(1)
      } else {
        errorBuffer.append(failStatement)
        batchFailure.add(1)
        if (batchFailure.value >= config.errorConfig.errorMaxSize) {
          throw TooManyErrorsException(
            s"There are too many failed batches, batch amount: ${batchFailure.value}, " +
              s"your config max error size: ${config.errorConfig.errorMaxSize}")
        }
      }
    }
    if (errorBuffer.nonEmpty) {
      val appId = SparkEnv.get.blockManager.conf.getAppId
      ErrorHandler.save(
        errorBuffer,
        s"${config.errorConfig.errorPath}/${appId}/${tagConfig.name}.${TaskContext.getPartitionId()}")
      errorBuffer.clear()
    }
    LOG.info(s"tag ${tagConfig.name} import in spark partition ${
      TaskContext
        .getPartitionId()
    } cost ${System.currentTimeMillis() - startTime} ms")
    writer.close()
    graphProvider.close()
  }

  /**
   * encode vertex for tagless vertex key, vertex key with tag, vertex value
   */
  private def getVertexKeyValue(
                                 row: Row,
                                 partitionNum: Int,
                                 vidType: VidType.Value,
                                 spaceVidLen: Int,
                                 tagItem: TagItem,
                                 fieldTypeMap: Map[String, Int]): (Array[Byte], Array[Byte], Array[Byte]) = {
    // check if vertex id is valid, if not, throw AssertException
    isVertexValid(row, tagConfig, false, vidType == VidType.STRING)

    val index: Int = row.schema.fieldIndex(tagConfig.vertexField)
    var vertexId: String = row.get(index).toString.trim
    if (vertexId.equals(DEFAULT_EMPTY_VALUE)) {
      vertexId = ""
    }
    if(vertexId == "2053037734897888931"){
      println("2053037734897888931",row)
    }
    if (tagConfig.vertexPolicy.isDefined) {
      tagConfig.vertexPolicy.get match {
        case KeyPolicy.HASH =>
          vertexId = MurmurHash2
            .hash64(vertexId.getBytes(), vertexId.getBytes().length, 0xc70f6907)
            .toString
        case KeyPolicy.UUID =>
          throw new UnsupportedOperationException("do not support uuid yet")
        case _ =>
          throw new IllegalArgumentException(s"policy ${tagConfig.vertexPolicy.get} is invalidate")
      }
    }

    val partitionId = NebulaUtils.getPartitionId(vertexId, partitionNum, vidType)

    import java.nio.ByteBuffer
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
    val vertexKey = codec.vertexKey(spaceVidLen, partitionId, vidBytes, tagItem.getTag_id)
    //fieldKeys len == nebulaKeys len
    val values = for {
      property <- nebulaKeys if property.trim.length != 0
    } yield
      extraValueForSST(row, property, fieldTypeMap)
        .asInstanceOf[AnyRef]
    val vertexValue = codec.encodeTag(tagItem, nebulaKeys.asJava, values.asJava)
    val orphanVertexKey = codec.orphanVertexKey(spaceVidLen, partitionId, vidBytes)
    (orphanVertexKey, vertexKey, vertexValue)
  }


  private def getVertexToCsvString(row: Row,
                                   vidType: VidType.Value,
                                   fieldTypeMap: Map[String, Int],
                                   separator: String,
                                   schemaProvider: SchemaProvider): Array[Any] = {
    // check if vertex id is valid, if not, throw AssertException
    isVertexValid(row, tagConfig, false, vidType == VidType.STRING)

    val index: Int = row.schema.fieldIndex(tagConfig.vertexField)
    var vertexId: String = row.get(index).toString.trim
    if (vertexId.equals(DEFAULT_EMPTY_VALUE)) {
      vertexId = ""
    }
    if (tagConfig.vertexPolicy.isDefined) {
      tagConfig.vertexPolicy.get match {
        case KeyPolicy.HASH =>
          vertexId = MurmurHash2
            .hash64(vertexId.getBytes(), vertexId.getBytes().length, 0xc70f6907)
            .toString
        case KeyPolicy.UUID =>
          throw new UnsupportedOperationException("do not support uuid yet")
        case _ =>
          throw new IllegalArgumentException(s"policy ${tagConfig.vertexPolicy.get} is invalidate")
      }
    }

    //val partitionId = NebulaUtils.getPartitionId(vertexId, partitionNum, vidType)

    //fieldKeys len == nebulaKeys len
    val values = for {
      property <- nebulaKeys if property.trim.length != 0
    } yield
      extraValueForSST(row, property, fieldTypeMap).asInstanceOf[AnyRef]

    //    val sb = new StringBuffer()
    //    sb.append(vertexId).append(separator)
    val vertexValues = NebulaDataUtils.nebulaDataToString(nebulaKeys, values, schemaProvider)
    //    vertexValues.foreach(v => sb.append(v).append(separator))
    //    sb.delete(sb.length() - 1, sb.length())
    //sb.toString
    Array[Any](vertexId) ++ vertexValues
  }
}
