package com.spark.api

import org.apache.spark.{SparkConf, SparkContext}

object TestAggregate {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("TestAggregate")
    val sc = new SparkContext(conf)
    val partitionNum = args.length match {
      case x:Int if x > 0 => args(0).toInt
      case _ => 2
    }
    val arr = sc.parallelize(Array(1,2,3,4,5,6),partitionNum)
    //math.max找到分区数组中最大元素值，最后将所有分区最大值进行相加
    val result = arr.aggregate(0)(math.max, _ + _)
    println(result)
  }
}