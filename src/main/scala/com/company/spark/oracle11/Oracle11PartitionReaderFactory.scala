package com.company.spark.oracle11

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

final class Oracle11PartitionReaderFactory(
    options: Oracle11Options,
    relation: Oracle11Relation,
    requiredSchema: StructType,
    pushedPredicate: Option[Oracle11SqlPredicate])
    extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val typedPartition = partition match {
      case p: Oracle11InputPartition => p
      case other =>
        throw new IllegalArgumentException(
          s"Expected Oracle11InputPartition, got ${other.getClass.getName}")
    }

    new Oracle11PartitionReader(
      options = options,
      relation = relation,
      requiredSchema = requiredSchema,
      partitionPredicate = typedPartition.predicate,
      pushedPredicate = pushedPredicate
    )
  }
}
