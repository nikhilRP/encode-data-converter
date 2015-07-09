package org.encodedcc

import org.bdgenomics.adam.plugins.ADAMPlugin
import org.bdgenomics.formats.avro.AlignmentRecord
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


/**
 * Converts ENCODE BAM files to ADAM files
 *
 * @author  Nikhil R Podduturi
 */
class Convert extends ADAMPlugin[AlignmentRecord, Tuple2[String, Int]] with Serializable {
   override def projection: Option[Schema] = None
   override def predicate: Option[(AlignmentRecord) => Boolean] = None

   override def run(sc: SparkContext, reads: RDD[AlignmentRecord], args: String): RDD[Tuple2[String, Int]] = {
     val sd = reads.adamGetSequenceDictionary()
     var referenceCounts : Array[(String, Int)] = new Array[(String, Int)](0)
     for(reference <- sd.toSAMSequenceDictionary.getSequences()) {
        def overlapsQuery(read: AlignmentRecord): Boolean = read.getReadMapped && read.getContig.getContigName.toString == reference.getSequenceName()
        val rdd = reads.filter(overlapsQuery).repartition(160)
        rdd.adamParquetSave("/user/nikhilrp/encoded-data/mm10/" + reference.getSequenceName() + "/" + args)
        referenceCounts :+ (reference.getSequenceName(), reference.getSequenceLength())
     }
     return referenceCounts
   }
}
