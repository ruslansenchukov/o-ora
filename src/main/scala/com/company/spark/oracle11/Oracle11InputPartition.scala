package com.company.spark.oracle11

import org.apache.spark.sql.connector.read.InputPartition

final case class Oracle11InputPartition(predicate: Option[Oracle11SqlPredicate]) extends InputPartition
