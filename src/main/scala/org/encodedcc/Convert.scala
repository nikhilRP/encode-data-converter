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
     val referenceCounts = reads.map(rec => if (rec.getReadMapped) rec.getContig.getContigName else "unmapped")
       .map(contigName => (contigName, 1)).reduceByKey(_ + _)
       
     val referenceNames = referenceCounts.collect().toMap
     for(reference <- referenceNames) {
	if(reference._1 != "unmapped") {
        	def overlapsQuery(read: AlignmentRecord): Boolean = 
          		read.getReadMapped && read.getContig.getContigName.toString == reference._1
        	val rdd = reads.filter(overlapsQuery)
        	rdd.adamParquetSave("/user/nikhilrp/encoded-data/hg19/" + reference._1 + "/" + args)
	}
     }
     return referenceCounts
   }
}
