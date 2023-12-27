/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.ubisectech.nebula.exchange.processor

import com.google.common.geometry.{S2CellId, S2LatLng}
import com.ubisectech.nebula.exchange.TooManyErrorsException
import com.ubisectech.nebula.exchange.coder.NebulaDecodecImpl
import com.ubisectech.nebula.exchange.common.common.{Edge, Edges, KeyPolicy}
import com.ubisectech.nebula.exchange.common.config._
import com.ubisectech.nebula.exchange.common.processor.Processor
import com.ubisectech.nebula.exchange.common.utils.NebulaUtils
import com.ubisectech.nebula.exchange.common.utils.NebulaUtils.DEFAULT_EMPTY_VALUE
import com.ubisectech.nebula.exchange.common.writer.{GenerateSstFile, NebulaGraphClientWriter}
import com.ubisectech.nebula.exchange.common.{ErrorHandler, GraphProvider, MetaProvider, VidType}
import com.ubisectech.nebula.exchange.utils.NebulaDataUtils
import com.ubisectech.nebula.exchange.utils.NebulaDataUtils.nebulaDataToNebulaString
import com.vesoft.nebula.encoder.{NebulaCodecImpl, SchemaProvider}
import com.vesoft.nebula.meta.EdgeItem
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkEnv, TaskContext}
import org.slf4j.LoggerFactory

import java.nio.ByteOrder
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class EdgeProcessor(spark: SparkSession,
                    data: DataFrame,
                    edgeConfig: EdgeConfigEntry,
                    fieldKeys: List[String],
                    nebulaKeys: List[String],
                    config: Configs,
                    batchSuccess: LongAccumulator,
                    batchFailure: LongAccumulator)
  extends Processor {

  @transient
  private[this] lazy val LOG = LoggerFactory.getLogger(this.getClass)
  val srcId = com.ubisectech.nebula.common.connector.NebulaUtils.srcId
  val dstId = com.ubisectech.nebula.common.connector.NebulaUtils.dstId
  val rank = com.ubisectech.nebula.common.connector.NebulaUtils.rank
  private[this] val DEFAULT_MIN_CELL_LEVEL = 10
  private[this] val DEFAULT_MAX_CELL_LEVEL = 18

  override def process(): Unit = {
    val address = config.databaseConfig.getMetaAddress
    val space = config.databaseConfig.space
    val timeout = config.connectionConfig.timeout
    val retry = config.connectionConfig.retry
    val metaProvider = new MetaProvider(address, timeout, retry, config.sslConfig)
    val fieldTypeMap = NebulaUtils.getDataSourceFieldType(edgeConfig, space, metaProvider)
    val isVidStringType = metaProvider.getVidType(space) == VidType.STRING
    val partitionNum = metaProvider.getPartNumber(space)

    if (edgeConfig.dataSinkConfigEntry.category == SinkCategory.SST) {
      writeToSst(metaProvider, space, partitionNum, fieldTypeMap)
    } else if (edgeConfig.dataSinkConfigEntry.category == SinkCategory.CSV) {
      writeToCsv(metaProvider, fieldTypeMap)
    } else {
      writeToNebula(metaProvider, space, isVidStringType, fieldTypeMap)
    }
    metaProvider.close()
  }

  /**
   * filter and check row data for edge, if streaming only print log
   */
  def isEdgeValid(row: Row,
                  edgeConfig: EdgeConfigEntry,
                  streamFlag: Boolean,
                  isVidStringType: Boolean): Boolean = {
    val sourceFlag = checkField(edgeConfig.sourceField,
      "source_field",
      row,
      edgeConfig.sourcePolicy,
      streamFlag,
      isVidStringType)

    val targetFlag = checkField(edgeConfig.targetField,
      "target_field",
      row,
      edgeConfig.targetPolicy,
      streamFlag,
      isVidStringType)

    val edgeRankFlag = if (edgeConfig.rankingField.isDefined) {
      val index = row.schema.fieldIndex(edgeConfig.rankingField.get)
      if (index < 0 || row.isNullAt(index)) {
        printChoice(streamFlag, s"rank must exist and cannot be null, your row data is $row")
      }
      val ranking = row.get(index).toString.trim
      if (!NebulaUtils.isNumic(ranking)) {
        printChoice(streamFlag,
          s"Not support non-Numeric type for ranking field.your row data is $row")
        false
      } else true
    } else true
    sourceFlag && targetFlag && edgeRankFlag
  }

  /**
   * check if edge source id and target id valid
   */
  def checkField(field: String,
                 fieldType: String,
                 row: Row,
                 policy: Option[KeyPolicy.Value],
                 streamFlag: Boolean,
                 isVidStringType: Boolean): Boolean = {
    val fieldValue = if (edgeConfig.isGeo && "source_field".equals(fieldType)) {
      val lat = row.getDouble(row.schema.fieldIndex(edgeConfig.latitude.get))
      val lng = row.getDouble(row.schema.fieldIndex(edgeConfig.longitude.get))
      Some(indexCells(lat, lng).mkString(","))
    } else {
      val index = row.schema.fieldIndex(field)
      if (index < 0 || row.isNullAt(index)) {
        printChoice(streamFlag, s"$fieldType must exist and cannot be null, your row data is $row")
        None
      } else Some(row.get(index).toString.trim)
    }

    val idFlag = fieldValue.isDefined
    val policyFlag =
      if (idFlag && policy.isEmpty && !isVidStringType
        && !NebulaUtils.isNumic(fieldValue.get)) {
        printChoice(
          streamFlag,
          s"space vidType is int, but your $fieldType $fieldValue is not numeric.your row data is $row")
        false
      } else if (idFlag && policy.isDefined && isVidStringType) {
        printChoice(
          streamFlag,
          s"only int vidType can use policy, but your vidType is FIXED_STRING.your row data is $row")
        false
      } else true

    val udfFlag = isVidStringType || policy.isEmpty || (edgeConfig.sourcePrefix == null && edgeConfig.targetPrefix == null)
    idFlag && policyFlag && udfFlag
  }

  /**
   * convert row data to {@link Edge}
   */
  def convertToEdge(row: Row,
                    edgeConfig: EdgeConfigEntry,
                    isVidStringType: Boolean,
                    fieldKeys: List[String],
                    fieldTypeMap: Map[String, Int]): Edge = {
    val sourceField = processField(srcId,
      "source_field",
      row,
      edgeConfig.sourcePolicy,
      isVidStringType,
      edgeConfig.sourcePrefix)

    val targetField = processField(dstId,
      "target_field",
      row,
      edgeConfig.targetPolicy,
      isVidStringType,
      edgeConfig.targetPrefix)


    val values = for {
      property <- fieldKeys if property.trim.length != 0
    } yield extraValueForClient(row, property, fieldTypeMap)

    if (edgeConfig.rankingField.isDefined) {
      val index = row.schema.fieldIndex(rank)
      val ranking = row.get(index).toString.trim
      Edge(sourceField, targetField, Some(ranking.toLong), values)
    } else {
      Edge(sourceField, targetField, None, values)
    }
  }

  /**
   * process edge source and target field
   */
  def processField(field: String,
                   fieldType: String,
                   row: Row,
                   policy: Option[KeyPolicy.Value],
                   isVidStringType: Boolean,
                   prefix: String): String = {
    var fieldValue = if (edgeConfig.isGeo && "source_field".equals(fieldType)) {
      val lat = row.getDouble(row.schema.fieldIndex(edgeConfig.latitude.get))
      val lng = row.getDouble(row.schema.fieldIndex(edgeConfig.longitude.get))
      indexCells(lat, lng).mkString(",")
    } else {
      val index = row.schema.fieldIndex(field)
      val value = row.get(index).toString.trim
      if (value.equals(DEFAULT_EMPTY_VALUE)) "" else value
    }
    if (prefix != null) {
      fieldValue = prefix + "_" + fieldValue
    }
    // process string type vid
    if (policy.isEmpty && isVidStringType) {
      fieldValue = NebulaUtils.escapeUtil(fieldValue).mkString("\"", "", "\"")
    }
    fieldValue
  }

  /**
   * encode edge
   */
  def encodeEdge(row: Row,
                 partitionNum: Int,
                 vidType: VidType.Value,
                 spaceVidLen: Int,
                 edgeItem: EdgeItem,
                 fieldTypeMap: Map[String, Int]): (Array[Byte], Array[Byte], Array[Byte]) = {
    isEdgeValid(row, edgeConfig, false, vidType == VidType.STRING)

    val srcIndex: Int = row.schema.fieldIndex(edgeConfig.sourceField)
    var srcId: String = row.get(srcIndex).toString.trim
    if (srcId.equals(DEFAULT_EMPTY_VALUE)) {
      srcId = ""
    }

    val dstIndex: Int = row.schema.fieldIndex(edgeConfig.targetField)
    var dstId: String = row.get(dstIndex).toString.trim
    if (dstId.equals(DEFAULT_EMPTY_VALUE)) {
      dstId = ""
    }

    if (edgeConfig.sourcePolicy.isDefined) {
      edgeConfig.sourcePolicy.get match {
        case KeyPolicy.HASH =>
          srcId = MurmurHash2
            .hash64(srcId.getBytes(), srcId.getBytes().length, 0xc70f6907)
            .toString
        case KeyPolicy.UUID =>
          throw new UnsupportedOperationException("do not support uuid yet")
        case _ =>
          throw new IllegalArgumentException(s"policy ${edgeConfig.sourcePolicy.get} is invalidate")
      }
    }
    if (edgeConfig.targetPolicy.isDefined) {
      edgeConfig.targetPolicy.get match {
        case KeyPolicy.HASH =>
          dstId = MurmurHash2
            .hash64(dstId.getBytes(), dstId.getBytes().length, 0xc70f6907)
            .toString
        case KeyPolicy.UUID =>
          throw new UnsupportedOperationException("do not support uuid yet")
        case _ =>
          throw new IllegalArgumentException(s"policy ${edgeConfig.targetPolicy.get} is invalidate")
      }
    }

    val ranking: Long = if (edgeConfig.rankingField.isDefined) {
      val rankIndex = row.schema.fieldIndex(edgeConfig.rankingField.get)
      row.get(rankIndex).toString.trim.toLong
    } else {
      0
    }

    val srcPartitionId = NebulaUtils.getPartitionId(srcId, partitionNum, vidType)
    val dstPartitionId = NebulaUtils.getPartitionId(dstId, partitionNum, vidType)
    val codec = new NebulaCodecImpl()

    import java.nio.ByteBuffer
    val srcBytes = if (vidType == VidType.INT) {
      ByteBuffer
        .allocate(8)
        .order(ByteOrder.nativeOrder)
        .putLong(srcId.toLong)
        .array
    } else {
      srcId.getBytes()
    }

    val dstBytes = if (vidType == VidType.INT) {
      ByteBuffer
        .allocate(8)
        .order(ByteOrder.nativeOrder)
        .putLong(dstId.toLong)
        .array
    } else {
      dstId.getBytes()
    }
    val positiveEdgeKey = codec.edgeKeyByDefaultVer(spaceVidLen,
      srcPartitionId,
      srcBytes,
      edgeItem.getEdge_type,
      ranking,
      dstBytes)
    val reverseEdgeKey = codec.edgeKeyByDefaultVer(spaceVidLen,
      dstPartitionId,
      dstBytes,
      -edgeItem.getEdge_type,
      ranking,
      srcBytes)

    val values = for {
      property <- nebulaKeys if property.trim.length != 0
    } yield
      extraValueForSST(row, property, fieldTypeMap).asInstanceOf[AnyRef]

    val edgeValue = codec.encodeEdge(edgeItem, nebulaKeys.asJava, values.asJava)
    (positiveEdgeKey, reverseEdgeKey, edgeValue)
  }

  private def writeToSst(metaProvider: MetaProvider, space: String, partitionNum: Int, fieldTypeMap: Map[String, Int]): Unit = {
    val fileBaseConfig = edgeConfig.dataSinkConfigEntry.asInstanceOf[FileBaseSinkConfigEntry]
    val namenode = fileBaseConfig.fsName.orNull
    val edgeName = edgeConfig.name
    val vidType = metaProvider.getVidType(space)
    val spaceVidLen = metaProvider.getSpaceVidLen(space)
    val edgeItem = metaProvider.getEdgeItem(space, edgeName)

    var sstKeyValueData =
      data.dropDuplicates(srcId, dstId, rank)
        .mapPartitions { iter =>
          iter.map { row =>
            encodeEdge(row, partitionNum, vidType, spaceVidLen, edgeItem, fieldTypeMap)
          }
        }(Encoders.tuple(Encoders.BINARY, Encoders.BINARY, Encoders.BINARY))
        .map(d => (d._1, d._3))(Encoders.tuple(Encoders.BINARY, Encoders.BINARY))
    if (edgeConfig.repartitionWithNebula) {
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
        val rowSeq = iterator.toSeq
        generateSstFile.writeSstFiles(rowSeq,
          fileBaseConfig,
          partitionNum,
          namenode,
          batchFailure,
          edgeName)
      }
    val rowCount = sstKeyValueData.count()

    LOG.info(s"Export tag to Sst:${edgeName} rowCount:${rowCount}")
  }

  private def writeToCsv(metaProvider: MetaProvider, fieldTypeMap: Map[String, Int]): Unit = {
    val space = config.databaseConfig.space
    val fileBaseConfig = edgeConfig.dataSinkConfigEntry.asInstanceOf[FileBaseSinkConfigEntry]
    val edgeName = edgeConfig.name
    val vidType = metaProvider.getVidType(space)
    val edgeItem = metaProvider.getEdgeItem(space, edgeName)
    val separator = fileBaseConfig.separator.get

    val schemaProvider = NebulaUtils.getSchemaProvider(edgeItem.getSchema, edgeItem.getVersion, fieldKeys)
    val st = com.ubisectech.nebula.common.connector.NebulaUtils.getDefaultSchema(false)
    val structFields = st.fields ++ fieldKeys.map(d => NebulaDataUtils.getStringStructField(schemaProvider, d))
    val schema = StructType(structFields)
    val fields = schema.map(_.name)
    val encoders = Encoders.row(schema)
    val partitionSize = data.sparkSession.sparkContext.defaultParallelism

    val csvData = data.toDF(fields: _*)
      .dropDuplicates(srcId, dstId, rank)
      .mapPartitions { iter =>
        val schemaProvider = NebulaUtils.getSchemaProvider(edgeItem.getSchema, edgeItem.getVersion, fieldKeys)
        iter.map { row =>
          val values: Array[Any] = getEdgeToCsvString(row, vidType, fieldTypeMap, separator, schemaProvider)
          new GenericRowWithSchema(values, schema).asInstanceOf[Row]
        }
      }(encoders)
      .repartition(partitionSize)
    val remotePath = fileBaseConfig.remotePath + "/" + edgeName
    csvData.write.format("csv")
      //.option("delimiter", separator)
      .mode(SaveMode.Overwrite).save(remotePath)
    val rowCount = csvData.count()
    LOG.info(s"Export tag to Csv:${edgeName} rowCount:${rowCount}")
  }

  private def dfByteToString(metaProvider: MetaProvider, space: String, fieldTypeMap: Map[String, Int]): DataFrame = {
    val tagName = edgeConfig.name
    val edgeItem = metaProvider.getEdgeItem(space, tagName)
    val schemaProvider = NebulaUtils.getSchemaProvider(edgeItem.getSchema, edgeItem.getVersion, nebulaKeys)
    val st = com.ubisectech.nebula.common.connector.NebulaUtils.getDefaultSchema(false)
    val structFields = st.fields ++ nebulaKeys.map(d => NebulaDataUtils.getStringStructField(schemaProvider, d))
    val schema = StructType(structFields)
    val encoders = Encoders.row(schema)
    val vidType = metaProvider.getVidType(space)
    val spaceVidLen = metaProvider.getSpaceVidLen(space)
    data.mapPartitions(iter => {
      val schemaProvider = NebulaUtils.getSchemaProvider(edgeItem.getSchema, edgeItem.getVersion, nebulaKeys)
      val decodecImpl = new NebulaDecodecImpl(nebulaKeys, schemaProvider)
      iter.map(row => {
        val keyBs = row.getAs[Array[Byte]]("key")
        val keyData = decodecImpl.parseEdgeKey(spaceVidLen, vidType, keyBs)
        val valueBs = row.getAs[Array[Byte]]("value")
        val valueData = decodecImpl.parseEdgeValue(valueBs)
        val prefixValueArr = Array(keyData.srcId, keyData.dstId, keyData.edgeRank)
        val values: Array[Any] = (prefixValueArr ++ valueData.values).map(NebulaDataUtils.nebulaDataToNebulaString)
        new GenericRowWithSchema(values, schema).asInstanceOf[Row]
      })
    })(encoders)
  }

  private def dfCsvToString(metaProvider: MetaProvider, space: String, fieldTypeMap: Map[String, Int]): DataFrame = {
    val tagName = edgeConfig.name
    val edgeItem = metaProvider.getEdgeItem(space, tagName)
    val schemaProvider = NebulaUtils.getSchemaProvider(edgeItem.getSchema, edgeItem.getVersion, nebulaKeys)
    val st = com.ubisectech.nebula.common.connector.NebulaUtils.getDefaultSchema(false)
    val structFields = st.fields ++ nebulaKeys.map(d => NebulaDataUtils.getStringStructField(schemaProvider, d))
    val defFields = st.fields.map(_.name)
    val schema = StructType(structFields)
    val fields = schema.map(_.name)
    val encoders = Encoders.row(schema)
    data.toDF(fields: _*).mapPartitions(iter => {
      val schemaProvider = NebulaUtils.getSchemaProvider(edgeItem.getSchema, edgeItem.getVersion, nebulaKeys)
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
          val nebulaValue = NebulaDataUtils.stringToNebulaData(valueStr, field)
          val nebulaValueStr = nebulaDataToNebulaString(nebulaValue)
          values(index) = nebulaValueStr
          index += 1
        }
        new GenericRowWithSchema(values, schema).asInstanceOf[Row]
      })

    })(encoders)
  }

  private def writeToNebula(metaProvider: MetaProvider, space: String, isVidStringType: Boolean, fieldTypeMap: Map[String, Int]): Unit = {
    val streamFlag = data.isStreaming
    val df = if (edgeConfig.dataSourceConfigEntry.category == SourceCategory.SST) {
      dfByteToString(metaProvider, space, fieldTypeMap)
    } else if (edgeConfig.dataSourceConfigEntry.category == SourceCategory.CSV) {
      dfCsvToString(metaProvider, space, fieldTypeMap)
    } else {
      data
    }

    val edgeFrame = df.filter(row =>
      isEdgeValid(row, edgeConfig, streamFlag, isVidStringType))
      .map { row =>
        convertToEdge(row, edgeConfig, isVidStringType, nebulaKeys, fieldTypeMap)
      }(Encoders.kryo[Edge])

    // streaming write
    if (streamFlag) {
      val streamingDataSourceConfig =
        edgeConfig.dataSourceConfigEntry.asInstanceOf[StreamingDataSourceConfigEntry]
      val wStream = edgeFrame.writeStream
      if (edgeConfig.checkPointPath.isDefined)
        wStream.option("checkpointLocation", edgeConfig.checkPointPath.get)
      wStream
        .foreachBatch((edges: Dataset[Edge], batchId: Long) => {
          LOG.info(s"${edgeConfig.name} edge start batch ${batchId}.")
          edges.foreachPartition(processEachPartition _)
        })
        .trigger(Trigger.ProcessingTime(s"${streamingDataSourceConfig.intervalSeconds} seconds"))
        .start()
        .awaitTermination()
    } else {
      edgeFrame.foreachPartition(processEachPartition _)
    }
    val rowCount = edgeFrame.count()
    val edgeName = edgeConfig.name
    LOG.info(s"Import edge to Nebula:${edgeName} rowCount:${rowCount}")
  }

  private def processEachPartition(iterator: Iterator[Edge]): Unit = {
    val graphProvider =
      new GraphProvider(config.databaseConfig.getGraphAddress,
        config.connectionConfig.timeout,
        config.sslConfig)
    val writer = new NebulaGraphClientWriter(config.databaseConfig,
      config.userConfig,
      config.rateConfig,
      edgeConfig,
      graphProvider)
    val errorBuffer = ArrayBuffer[String]()

    writer.prepare()
    // batch write tags
    val startTime = System.currentTimeMillis
    iterator.grouped(edgeConfig.batch).foreach { edge =>
      val edges = Edges(nebulaKeys, edge.toList, edgeConfig.sourcePolicy, edgeConfig.targetPolicy)
      val failStatement = writer.writeEdges(edges, edgeConfig.ignoreIndex)
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
        s"${config.errorConfig.errorPath}/${appId}/${edgeConfig.name}.${TaskContext.getPartitionId}")
      errorBuffer.clear()
    }
    LOG.info(s"edge ${edgeConfig.name} import in spark partition ${
      TaskContext
        .getPartitionId()
    } cost ${System.currentTimeMillis() - startTime}ms")
    writer.close()
    graphProvider.close()
  }

  private[this] def indexCells(lat: Double, lng: Double): IndexedSeq[Long] = {
    val coordinate = S2LatLng.fromDegrees(lat, lng)
    val s2CellId = S2CellId.fromLatLng(coordinate)
    for (index <- DEFAULT_MIN_CELL_LEVEL to DEFAULT_MAX_CELL_LEVEL)
      yield s2CellId.parent(index).id()
  }

  private def getEdgeToCsvString(row: Row,
                                 vidType: VidType.Value,
                                 fieldTypeMap: Map[String, Int],
                                 separator: String,
                                 schemaProvider: SchemaProvider): Array[Any] = {
    // check if vertex id is valid, if not, throw AssertException
    isEdgeValid(row, edgeConfig, false, vidType == VidType.STRING)
    val srcIndex: Int = row.schema.fieldIndex(edgeConfig.sourceField)
    var srcId: String = row.get(srcIndex).toString.trim
    if (srcId.equals(DEFAULT_EMPTY_VALUE)) {
      srcId = ""
    }

    val dstIndex: Int = row.schema.fieldIndex(edgeConfig.targetField)
    var dstId: String = row.get(dstIndex).toString.trim
    if (dstId.equals(DEFAULT_EMPTY_VALUE)) {
      dstId = ""
    }

    if (edgeConfig.sourcePolicy.isDefined) {
      edgeConfig.sourcePolicy.get match {
        case KeyPolicy.HASH =>
          srcId = MurmurHash2
            .hash64(srcId.getBytes(), srcId.getBytes().length, 0xc70f6907)
            .toString
        case KeyPolicy.UUID =>
          throw new UnsupportedOperationException("do not support uuid yet")
        case _ =>
          throw new IllegalArgumentException(s"policy ${edgeConfig.sourcePolicy.get} is invalidate")
      }
    }
    if (edgeConfig.targetPolicy.isDefined) {
      edgeConfig.targetPolicy.get match {
        case KeyPolicy.HASH =>
          dstId = MurmurHash2
            .hash64(dstId.getBytes(), dstId.getBytes().length, 0xc70f6907)
            .toString
        case KeyPolicy.UUID =>
          throw new UnsupportedOperationException("do not support uuid yet")
        case _ =>
          throw new IllegalArgumentException(s"policy ${edgeConfig.targetPolicy.get} is invalidate")
      }
    }

    val ranking: Long = if (edgeConfig.rankingField.isDefined) {
      val rankIndex = row.schema.fieldIndex(edgeConfig.rankingField.get)
      row.get(rankIndex).toString.trim.toLong
    } else {
      0L
    }

    val values = for {
      property <- nebulaKeys if property.trim.length != 0
    } yield
      extraValueForSST(row, property, fieldTypeMap).asInstanceOf[AnyRef]

    val vertexValues = NebulaDataUtils.nebulaDataToString(nebulaKeys, values, schemaProvider)
    Array[Any](srcId, dstId, ranking) ++ vertexValues
  }
}
