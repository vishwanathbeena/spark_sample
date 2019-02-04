package com.spark.practice.util

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.LongType

object SparkUtils {
  
 def getSparkContext(appName:String,masterConf:String):SparkSession = {
			val SparkConext = SparkSession.builder()
			.appName(appName)
			.getOrCreate()
			return SparkConext
	}
 
 def getLocalSparkContext(appName:String,masterConf:String):SparkSession = {
			val SparkConext = SparkSession.builder()
			.appName(appName)
			.master("local[*]")
			.getOrCreate()
			return SparkConext
	}
 
 
 def stopSparkSession(sparkSession:SparkSession){
     sparkSession.stop()
 }
 
   def maxColumnValueFromDfNcs(df: Dataset[Row],maxColumnNam:String,maxColumnNameAlias:String,condition :String):  Dataset[Row] = {
    require(df != null)
    //val maxDf: Dataset[Row]=   df.select("*").filter(condition).agg(max(maxColumnNam) as(maxColumnNameAlias))
     val maxDf: Dataset[Row]=   df.filter(condition).agg(max(maxColumnNam).cast(LongType) as(maxColumnNameAlias)).na.fill(0)
    maxDf
  }
 
}
