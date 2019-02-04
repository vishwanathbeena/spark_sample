package com.spark.practice.common

import scala.collection.immutable.TreeMap
import collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import com.spark.practice.model.SampleModel
import com.spark.practice.model.SampleModel2

object ReferenceConverter {
  
  
  def seq1Map(seq1:Array[SampleModel2]):Map[String,String]={
    seq1.map(rec => (rec.id.toString(),rec.desc.toString())).toMap
  }
  
  def mapAcctDvcRefWithStgCnsmHHFnlTest(seq2:Map[String,String]):SampleModel => SampleModel = account_dvc_ref_df => {
    var temp_var:SampleModel = SampleModel("NA","NA","NA");
    val seq1_key = account_dvc_ref_df.id.toString()
    println("searching for key===>" + seq1_key)
   if(!seq2.getOrElse(seq1_key, "null").isEmpty && !seq2.getOrElse(seq1_key, "null").equalsIgnoreCase("null")){
     temp_var = account_dvc_ref_df.copy(desc2=seq2.get(seq1_key).get)   
   }
    temp_var
    
    
  }
}