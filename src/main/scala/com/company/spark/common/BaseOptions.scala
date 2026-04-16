package com.company.spark.common

final case class BaseOptions(
    url: String,
    user: String,
    password: String,
    dbtable: Option[String],
    query: Option[String],
    fetchSize: Int,
    partitionColumn: Option[String],
    lowerBound: Option[String],
    upperBound: Option[String],
    numPartitions: Option[Int])
    extends Serializable
