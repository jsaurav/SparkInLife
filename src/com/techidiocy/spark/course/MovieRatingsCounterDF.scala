package com.techidiocy.spark.course

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit,col,sum}

object MovieRatingsCounterDF {
  
  val SOURCE_FILE_PATH = "C:\\saurav\\self\\learning\\spark\\udemy\\movie-data\\ml-20m\\ratings.csv"
  
  def main(params:Array[String]) {
    
    val spark = SparkSession.builder().appName("MovieRatingsCounterDF").master("local").getOrCreate()
    
    val sourceDF = spark.read.format("csv").option("inferSchema", "true").option("header","true").load(SOURCE_FILE_PATH)
    
    val extraColumnDF = sourceDF.withColumn("count", lit(1))
    
    val ratingsCountDF = extraColumnDF.groupBy(col("rating")).agg(sum(col("count").as("total"))).sort(col("rating"))
    
    ratingsCountDF.show()
     
  }
  
}