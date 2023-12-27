/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.nebula.exchange

import com.ubisectech.nebula.exchange.common.utils.NebulaUtils
import com.vesoft.nebula.client.graph.data.HostAddress
import com.vesoft.nebula.client.meta.MetaClient
import junit.framework.TestCase

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class TestExchange extends TestCase {

  private val metaAddresses = Seq("192.88.1.241:9559")
  private val space = "compograph"
  private val label = "compo"
  private val hostAndPorts = new ListBuffer[HostAddress]
  for (address <- metaAddresses) {
    hostAndPorts.append(NebulaUtils.getAddressFromString(address))
  }
  private val metaAddress = hostAndPorts.toList


  override def setUp() = {
    System.setProperty("spark.master", "local[2]")

  }

  def testClient(): Unit = {
    //    val metaAddressStr = List("192.88.1.241:9559")
    //    val metaAddress = getMetaAddress(metaAddressStr).asJava
    val client = new MetaClient(metaAddress.asJava, 10, 2, 2)
    client.connect()
    val tagSchema = client.getTag("compograph", "compo")
    println(tagSchema.getColumns.asScala.map(_.getName).map(new String(_)).mkString(","))
  }


  //match (v1)-[e:cdc]-(v2) return v1 limit 5



  def testNebulaToSstTest(): Unit = {
    val args = new ArrayBuffer[String]
    args += "-c"
    args += "src/test/resources/config/nebula_to_sst_test.conf"
    Exchange.main(args.toArray)
  }
  def testSstToNebulaTest(): Unit = {
    val args = new ArrayBuffer[String]
    args += "-c"
    args += "src/test/resources/config/sst_to_nebula_test.conf"
    Exchange.main(args.toArray)
  }

  def testNebulaToCsvTest(): Unit = {
    val args = new ArrayBuffer[String]
    args += "-c"
    args += "src/test/resources/config/nebula_to_csv_test.conf"
    Exchange.main(args.toArray)
  }

  def testCsvToNebulaTest(): Unit = {
    val args = new ArrayBuffer[String]
    args += "-c"
    args += "src/test/resources/config/csv_to_nebula_test.conf"
    Exchange.main(args.toArray)
  }

  /**
   * sst,有可变字符串或geo数据类型时数据异常
   * 原因:在RowWriterImpl.processOutOfSpace方法里,写字符时疑似逻辑错误,应该先数据数据长度,再写字符数据
   */
  def testNebulaToSst(): Unit = {
    val args = new ArrayBuffer[String]
    args += "-c"
    args += "src/test/resources/config/nebula_to_sst.conf"
    Exchange.main(args.toArray)
  }

  def testSstToNebula(): Unit = {
    val args = new ArrayBuffer[String]
    args += "-c"
    args += "src/test/resources/config/sst_to_nebula.conf"
    Exchange.main(args.toArray)
  }

  def testNebulaToCsv(): Unit = {
    val args = new ArrayBuffer[String]
    args += "-c"
    args += "src/test/resources/config/nebula_to_csv.conf"
    Exchange.main(args.toArray)
  }

  def testCsvToNebula(): Unit = {
    val args = new ArrayBuffer[String]
    args += "-c"
    args += "src/test/resources/config/csv_to_nebula.conf"
    Exchange.main(args.toArray)
  }
}
