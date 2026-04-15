package com.company.spark.oracle11

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

final class Oracle11Batch(
    options: Oracle11Options,
    fullSchema: StructType,
    requiredSchema: StructType,
    pushedPredicate: Option[Oracle11SqlPredicate],
    pushedLimit: Option[Int])
    extends Batch {

  override def planInputPartitions(): Array[InputPartition] =
    Oracle11PartitionPlanner
      .plan(options, options.relation, fullSchema, pushedPredicate)
      .map(identity[InputPartition])

  override def createReaderFactory(): PartitionReaderFactory =
    new Oracle11PartitionReaderFactory(
      options = options,
      relation = options.relation,
      requiredSchema = requiredSchema,
      pushedPredicate = pushedPredicate,
      pushedLimit = pushedLimit
    )
}
