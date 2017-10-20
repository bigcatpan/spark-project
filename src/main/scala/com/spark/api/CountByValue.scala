package com.spark.api

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计某个数据出现次数，以map形式返回
  */
object CountByValue {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("CountByValue")
    val sc = new SparkContext(conf)
    val arr = sc.parallelize(Array(1,2,2,4,5,6))
    val result = arr.countByValue()
    result.foreach(print)
  }
}
