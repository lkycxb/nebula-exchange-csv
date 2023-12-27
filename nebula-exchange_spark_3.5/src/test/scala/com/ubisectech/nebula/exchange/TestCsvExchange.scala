/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.nebula.exchange

import com.ubisectech.nebula.exchange.common.utils.NebulaUtils
import com.vesoft.nebula.client.graph.data.HostAddress
import junit.framework.TestCase

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class TestCsvExchange extends TestCase {
  private val metaAddresses = Seq("192.88.1.241:9559")
  private val space = "compograph"
  private val label = "compo"
  private val hostAndPorts = new ListBuffer[HostAddress]
  for (address <- metaAddresses) {
    hostAndPorts.append(NebulaUtils.getAddressFromString(address))
  }
  private val metaAddress = hostAndPorts.toList


  def testNebulaToCsv(): Unit = {
    val args = new ArrayBuffer[String]
    args += "target/test-classes/config/nebula_to_csv.properties"
   // CsvExchange.main(args.toArray)
  }

  def testCsvToNebula(): Unit = {
    val args = new ArrayBuffer[String]
    args += "target/test-classes/config/csv_to_nebula.conf"
    //CsvExchange.main(args.toArray)
  }
}
