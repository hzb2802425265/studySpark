package cn.spark.study01

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: huzhibin_zz
 * @Date: 2022/8/30
 * @Description :
 *
 */
object SparkSQL {
  def main(args: Array[String]): Unit = {
    val cof = new SparkConf().setMaster("local[3]").setAppName("RddStudy01")
    val sc = new SparkContext(cof)
  }

}
