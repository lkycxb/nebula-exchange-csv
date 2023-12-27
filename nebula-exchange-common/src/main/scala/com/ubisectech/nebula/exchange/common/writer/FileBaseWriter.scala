/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.ubisectech.nebula.exchange.common.writer

import com.ubisectech.nebula.exchange.common.config.FileBaseSinkConfigEntry
import com.ubisectech.nebula.exchange.common.utils.HDFSUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.util.LongAccumulator
import org.rocksdb._
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.{Files, Paths}
import java.nio.{ByteBuffer, ByteOrder}

/**
 * NebulaSSTWriter
 */
class NebulaSSTWriter(path: String) extends Writer {
  private val LOG = LoggerFactory.getLogger(getClass)
  var isOpen = false

  try {
    RocksDB.loadLibrary()
    LOG.info("Loading RocksDB successfully")
  } catch {
    case _: Exception =>
      LOG.error("Can't load RocksDB library!")
  }

  // TODO More Config ...
  val options = new Options()
    .setCreateIfMissing(true)
  val env = new EnvOptions()

  var writer: SstFileWriter = _

  override def prepare(): Unit = {
    writer = new SstFileWriter(env, options)
    writer.open(path)
    isOpen = true
  }

  def write(key: Array[Byte], value: Array[Byte]): Unit = {
    writer.put(key, value)
  }

  override def close(): Unit = {
    if (isOpen) {
      writer.finish()
      writer.close()
    }
    options.close()
    env.close()
  }

}

class GenerateSstFile extends Serializable {
  private val LOG = LoggerFactory.getLogger(getClass)

  def writeSstFiles(iterator: Seq[Row],
                    fileBaseConfig: FileBaseSinkConfigEntry,
                    partitionNum: Int,
                    namenode: String,
                    batchFailure: LongAccumulator, tagName: String): Unit = {
    val taskID = TaskContext.get().taskAttemptId()
    var writer: NebulaSSTWriter = null
    var currentPart = -1
    var currentPrefix = -1
    val localPath = fileBaseConfig.localPath + "/" + tagName
    val remotePath = fileBaseConfig.remotePath + "/" + tagName

    val localDir = new File(localPath)
    if (!localDir.exists()) {
      localDir.mkdirs()
    }
    try {
      iterator.foreach { vertex =>
        val key = vertex.getAs[Array[Byte]](0)
        val value = vertex.getAs[Array[Byte]](1)
        var part = ByteBuffer
          .wrap(key, 0, 4)
          .order(ByteOrder.nativeOrder)
          .getInt >> 8
        if (part <= 0) {
          part = part + partitionNum
        }
        // extract the prefix value for vertex key, there's two values
        // 1: vertex key with tag, 7: vertex key without tag
        val prefix: Int = ByteBuffer.wrap(key, 0, 1).get

        if (part != currentPart || prefix != currentPrefix) {
          if (writer != null) {
            writer.close()
            val localFile = s"$localPath/$currentPart-$taskID-$currentPrefix.sst"
            HDFSUtils.upload(localFile,
              s"$remotePath/${currentPart}/$currentPart-$taskID-$currentPrefix.sst",
              namenode)
            Files.delete(Paths.get(localFile))
          }
          currentPart = part
          currentPrefix = prefix
          val tmp = s"$localPath/$currentPart-$taskID-$currentPrefix.sst"
          writer = new NebulaSSTWriter(tmp)
          writer.prepare()
        }
        writer.write(key, value)
      }
    } catch {
      case e: Throwable => {
        LOG.error("sst file write error,", e)
        batchFailure.add(1)
      }
    } finally {
      if (writer != null) {
        writer.close()
        val localFile = s"$localPath/$currentPart-$taskID-$currentPrefix.sst"
        HDFSUtils.upload(localFile,
          s"$remotePath/${currentPart}/$currentPart-$taskID-$currentPrefix.sst",
          namenode)
        Files.delete(Paths.get(localFile))
      }
    }
  }
}
