/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.ubisectech.nebula.exchange.common

import com.google.common.base.Optional
import com.google.common.util.concurrent.ListenableFuture
import com.ubisectech.nebula.exchange.common.utils.NebulaUtils

import scala.collection.mutable.ListBuffer

package object common {

  type GraphSpaceID = Int
  type PartitionID = Int
  type TagID = Int
  type EdgeType = Int
  type SchemaID = (TagID, EdgeType)
  type TagVersion = Long
  type EdgeVersion = Long
  type SchemaVersion = (TagVersion, EdgeVersion)
  type VertexID = Long
  type VertexIDSlice = String
  type EdgeRank = Long
  type PropertyNames = List[String]
  type PropertyValues = List[Any]
  type ProcessResult = ListBuffer[WriterResult]
  type WriterResult = ListenableFuture[Optional[Integer]]

  case class Vertex(vertexID: VertexIDSlice, values: PropertyValues) {

    def propertyValues = values.mkString(", ")

    override def toString: String = {
      s"Vertex ID: ${vertexID}, " +
        s"Values: ${values.mkString(", ")}"
    }
  }

  case class Vertices(names: PropertyNames,
                      values: List[Vertex],
                      policy: Option[KeyPolicy.Value] = None) {

    def propertyNames: String = NebulaUtils.escapePropName(names).mkString(",")

    override def toString: String = {
      s"Vertices: " +
        s"Property Names: ${names.mkString(", ")}" +
        s"Vertex Values: ${values.mkString(", ")} " +
        s"with policy ${policy}"
    }
  }

  case class Edge(source: VertexIDSlice,
                  destination: VertexIDSlice,
                  ranking: Option[EdgeRank],
                  values: PropertyValues) {

    def this(source: VertexIDSlice, destination: VertexIDSlice, values: PropertyValues) = {
      this(source, destination, None, values)
    }

    override def toString: String = {
      val rank = if (ranking.isEmpty) 0 else ranking.get
      s"Edge: ${source}->${destination}@${rank} values: ${propertyValues}"
    }

    def propertyValues: String = values.mkString(", ")
  }

  case class Edges(names: PropertyNames,
                   values: List[Edge],
                   sourcePolicy: Option[KeyPolicy.Value] = None,
                   targetPolicy: Option[KeyPolicy.Value] = None) {
    def propertyNames: String = NebulaUtils.escapePropName(names).mkString(",")

    override def toString: String = {
      "Edges:" +
        s"Property Names: ${names.mkString(", ")}" +
        s"with source policy ${sourcePolicy}" +
        s"with target policy ${targetPolicy}"
    }
  }

  case class Offset(start: Long, size: Long)

  object KeyPolicy extends Enumeration {
    type POLICY = Value
    val HASH = Value("hash")
    val UUID = Value("uuid")
  }
}

final case class Argument(config: String = "application.conf",
                          hive: Boolean = false,
                          directly: Boolean = false,
                          dry: Boolean = false,
                          reload: String = "",
                          variable: Boolean = false,
                          param: String = "")
