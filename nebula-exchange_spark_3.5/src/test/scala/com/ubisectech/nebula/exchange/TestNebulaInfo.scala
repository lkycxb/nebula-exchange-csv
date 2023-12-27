/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.nebula.exchange

import com.ubisectech.nebula.exchange.common.MetaProvider
import com.ubisectech.nebula.exchange.common.config.{CaSignParam, SelfSignParam, SslConfigEntry, SslType}
import com.ubisectech.nebula.exchange.common.utils.NebulaUtils
import com.ubisectech.nebula.exchange.utils.NebulaDataUtils
import com.vesoft.nebula.PropertyType
import com.vesoft.nebula.client.graph.data.HostAddress
import junit.framework.TestCase
import org.apache.spark.sql.Encoders
import org.rocksdb._

import scala.collection.mutable.ListBuffer

class TestNebulaInfo extends TestCase {
  private val metaAddresses = Seq("192.88.1.241:9559")
  private val space = "compograph"
  private val label = "compo"

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
    metaProvider = new MetaProvider(metaAddress, 10, 10, sslConfig)
  }

  def testDataType1(): Unit = {


    //if (dataType == Type.VERTEX) {
    val fieldTypeMap = metaProvider.getTagSchema(space, label)
    //    } else {
    //      nebulaSchemaMap = metaProvider.getEdgeSchema(space, label)
    //    }
    fieldTypeMap.foreach(kv => println(kv._1, PropertyType.findByValue(kv._2)))
    val encoders = fieldTypeMap.map(d => Encoders.bean(NebulaDataUtils.getClassByType(d._2)))
    encoders.foreach(d => println(d.schema))


    //    val wkb:Array[Byte]=null
    //   val  org.locationtech.jts.geom.Geometry = new org.locationtech.jts.io.WKBReader().read(wkb)

    //val buf = ByteBuffer.allocate(100)

  }

  def testSstFile(): Unit = {
    val writeOptions = new Options().setCreateIfMissing(true)
      .setCompressionType(CompressionType.DISABLE_COMPRESSION_OPTION);
    val writeEnv = new EnvOptions()
    val writer = new SstFileWriter(writeEnv, writeOptions);
    val path = "F:\\share\\vulncomponents\\nebula\\compograph\\sst\\test.sst"
    writer.open(path)
    val seq = for (i <- 8 to 15) yield i
    val dataSeq = seq.map(d => s"key:${d}".getBytes -> s"value:${d}".getBytes()).sortBy(d => new String(d._1))
    var index = 0
    dataSeq.foreach(data => {
      writer.put(data._1, data._2)
      println(s"done,key:${new String(data._1)},value:${new String(data._2)}")
      index += 1
    })

    writer.finish()
    writer.close()
    writeOptions.close()
    writeEnv.close()

    //read
    val readEnv = new Options()
    val reader = new SstFileReader(readEnv)
    reader.open(path);
    reader.verifyChecksum();
    println("numEntries:" + reader.getTableProperties().getNumEntries());
    val readOptions = new ReadOptions()
    val iterator = reader.newIterator(readOptions)
    iterator.seekToFirst
    while (iterator.isValid()) {
      println("key:" + new String(iterator.key()) + ",value:" + new String(iterator.value()))
      iterator.next()
    }

    reader.close()
    readOptions.close()
    readEnv.close()
  }


}
