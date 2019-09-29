package com.techidiocy.spark.course

import org.apache.spark.SparkContext;

object MovieRatingsCounter {
  
  def main(params:Array[String]) {
    
    val sc = new SparkContext("local[*]", "MovieRatingsCounter");
    
    val linesRDD = sc.textFile("C:\\saurav\\self\\learning\\spark\\udemy\\movie-data\\ml-100k\\u.data")
    
    val ratingsRDD = linesRDD.map(line => line.toString().split("\t")(2))
    
    val results = ratingsRDD.countByValue()
    
    val sortedResults = results.toSeq.sortBy(_._1)
    
    sortedResults.foreach(println)
     
  }  
}