package org.encodedcc

import org.bdgenomics.adam.plugins.ADAMPlugin
import org.bdgenomics.formats.avro.AlignmentRecord
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Converts ENCODE BAM files to ADAM files
 *
 * @author  Nikhil R Podduturi
 */
class Converter extends ADAMPlugin[AlignmentRecord, Tuple2[String, Int]] with Serializable {
   override def projection: Option[Schema] = None
   override def predicate: Option[(AlignmentRecord) => Boolean] = None

   override def run(sc: SparkContext, recs: RDD[AlignmentRecord], args: String): RDD[Tuple2[String, Int]] = {
     recs.map(rec => if (rec.getReadMapped) rec.getContig.getContigName else "unmapped")
       .map(contigName => (contigName, 1))
       .reduceByKey(_ + _)
   }
}
