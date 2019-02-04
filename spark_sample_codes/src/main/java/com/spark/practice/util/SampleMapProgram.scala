package com.spark.practice.util

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.row_number

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.spark.practice.model.ExecuteArgs
import com.spark.practice.core.provider.ParquetDatasetProvider
import com.spark.practice.core.provider.FileSource
import com.spark.practice.model.SampleModel
import com.spark.practice.common.ReferenceConverter
import com.spark.practice.model.SampleModel2

object SampleMapProgram {

  def main(args: Array[String]): Unit = {

    //run2();
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkSession = SparkUtils.getLocalSparkContext("Test", "");
    import sparkSession.implicits._
    sparkSession.conf.set("spark.sql.warehouse.dir", "C:\\Users\\vbeena.jupudi\\spark-warehouse\\");

    val executeArgs: ExecuteArgs = new ExecuteArgs(
      "", //mode
      false, //help
      false, //version
      "", //inputLocation
      "", //outputLocation
      "2018-11-09", //processingDatadate
      "", //persistModeVal
      "", //appNameval
      "", //prodiverId:String
      false, //debug:
      true //dryrun:
    )
    //implicit val acctEncoder: Encoder[AccountDvcRef] = ExpressionEncoder[AccountDvcRef]
    //implicit val stgCnsmHHActvSttsEncoder: Encoder[StgCnsmHHActvStts] = ExpressionEncoder[StgCnsmHHActvStts]
    implicit val rowEncoder: Encoder[SampleModel] = ExpressionEncoder[SampleModel]

    // val stgCnsmHHFnlValues = ReferenceConverter.getStgCnsmHhActvSttsMap(stg_cnsm_hh_actv_stts_df.collect())
    // val temp_val = account_dvc_ref_df.map(ReferenceConverter.mapAcctDvcRefWithStgCnsmHHFnl(stgCnsmHHFnlValues, sparkSession))
    //println("before printing")

    // temp_val.foreach(rec => println(rec))
    //val acctDvcRefValues = ReferenceConverter.getAcctDvcRef(account_dvc_ref_df.collect())
    //account_dvc_ref_df.map(func)
    // val mapAcctDefWithHHFnl = ReferenceConverter.mapAcctDvcRefHHFnl(stg_cnsm_hh_actv_stts_df.collect(),acctDvcRefValues,sparkSession)
    /* mapAcctDefWithHHFnl.printSchema()
    mapAcctDefWithHHFnl.show()
    println(mapAcctDefWithHHFnl.count())*/

    /*val dateRange = NcsUtils.getPreviousDateRange("2018-11-09", -10)
    dateRange.foreach(println)
    val pathSeq = NcsUtils.getStbCnsmIntbSttsPath(dateRange, "abc")
    pathSeq.foreach(println)*/
    //mapAcctDefWithHHFnl.foreach(println)
    // var passingMap = Map.empty[String,String]
    /*var finalResultMap = Map.empty[String,String]
    val array1 = Array("1","2","3")
    val array2 = Array("1","2","3")
    val Array3 = Array("1","2","3","3")

    val array4 = Array("1","2","3")
    val array5 = Array("1","2","3")
   finalResultMap =  NcsUtils.dqcNcsDistictCheck(array1, array2, finalResultMap, "check_One")
   finalResultMap =  NcsUtils.dqcNcsDistictCheck(array1, array2, finalResultMap, "check_Two")
   //finalResultMap =  NcsUtils.getDistinctCountCheck(Array3,finalResultMap, "check_Three")
   //finalResultMap = NcsUtils.getNcsLiveCheck(array1, array4, array2, array5, finalResultMap, "check_Four")
  println(finalResultMap)*/

    val seq1 = Seq((1, "One", ""), (2, "Two", ""), (4, "Three", ""), (5, "Three", ""), (6, "Three", ""))
    val seq2 = Seq((1, "One_val"), (2, "Two_val"), (3, "Four_val"))
    val seq1_rdd = sparkSession.sparkContext.parallelize(seq1)
    val seq2_rdd = sparkSession.sparkContext.parallelize(seq2)

    val seq1_df = seq1_rdd.map(x => SampleModel(x._1.toString(), x._2, x._3)).toDS()
    val seq2_df = seq2_rdd.map(x => SampleModel2(x._1.toString(), x._2)).toDS()
    val seq2Array = ReferenceConverter.seq1Map(seq2_df.collect())

    //seq2Array.foreach(println)
    val seq_test = seq1_df.map(ReferenceConverter.mapAcctDvcRefWithStgCnsmHHFnlTest(seq2Array))
    seq_test.show()
    seq_test.filter(col("id").!==(lit("NA"))).show()
    //seq_test.foreach(rec => println(rec))
    //seq1_df.show()
    //seq2_df.show()

  }

}
