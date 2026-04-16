package com.company.spark.common

final case class PartitionRange(
    lowerInclusive: Option[Any],
    upperExclusive: Option[Any],
    includeNullsInLowerBucket: Boolean)
    extends Serializable

object PartitionPlanner {

  // Produces Spark JDBC-compatible windows: (< first OR IS NULL), middle [start, end), and (>= last).
  def planPartitions(boundaries: Seq[Any]): Seq[PartitionRange] = {
    if (boundaries.isEmpty) {
      Seq.empty
    } else {
      val first = PartitionRange(
        lowerInclusive = None,
        upperExclusive = Some(boundaries.head),
        includeNullsInLowerBucket = true
      )

      val middle = boundaries.sliding(2).toSeq.map {
        case Seq(start, end) =>
          PartitionRange(
            lowerInclusive = Some(start),
            upperExclusive = Some(end),
            includeNullsInLowerBucket = false
          )
        case _ =>
          throw new IllegalStateException("Unexpected partition boundary window")
      }

      val last = PartitionRange(
        lowerInclusive = Some(boundaries.last),
        upperExclusive = None,
        includeNullsInLowerBucket = false
      )

      first +: middle :+ last
    }
  }
}
