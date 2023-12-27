/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.ubisectech.nebula.exchange.common.processor

import com.ubisectech.nebula.exchange.common.config.Configs
import com.ubisectech.nebula.exchange.common.writer.NebulaGraphClientWriter
import com.ubisectech.nebula.exchange.common.{ErrorHandler, GraphProvider}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkEnv, TaskContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

class ReloadProcessor(data: DataFrame,
                      config: Configs,
                      batchSuccess: LongAccumulator,
                      batchFailure: LongAccumulator)
  extends Processor {
  @transient
  private[this] lazy val LOG = LoggerFactory.getLogger(this.getClass)

  override def process(): Unit = {
    data.foreachPartition((rows: Iterator[Row]) => processEachPartition(rows))
  }

  private def processEachPartition(iterator: Iterator[Row]): Unit = {
    val graphProvider =
      new GraphProvider(config.databaseConfig.getGraphAddress,
        config.connectionConfig.timeout,
        config.sslConfig)

    val writer = new NebulaGraphClientWriter(config.databaseConfig,
      config.userConfig,
      config.rateConfig,
      null,
      graphProvider)

    val errorBuffer = ArrayBuffer[String]()

    writer.prepare()
    // batch write
    val startTime = System.currentTimeMillis
    iterator.foreach { row =>
      val failStatement = writer.writeNgql(row.getString(0))
      if (failStatement == null) {
        batchSuccess.add(1)
      } else {
        errorBuffer.append(failStatement)
        batchFailure.add(1)
      }
    }
    if (errorBuffer.nonEmpty) {
      ErrorHandler.save(
        errorBuffer,
        s"${config.errorConfig.errorPath}/${SparkEnv.get.blockManager.conf.getAppId}/reload.${
          TaskContext
            .getPartitionId()
        }")
      errorBuffer.clear()
    }
    LOG.info(s"data reload in partition ${
      TaskContext
        .getPartitionId()
    } cost ${System.currentTimeMillis() - startTime}ms")
    writer.close()
    graphProvider.close()
  }
}
