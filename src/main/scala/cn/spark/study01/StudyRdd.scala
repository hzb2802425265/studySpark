package cn.spark.study01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: huzhibin_zz
 * @Date: 2022/8/26
 * @Description :spark
 *
 */
object StudyRdd {
  def main(args: Array[String]): Unit = {
    val cof = new SparkConf().setMaster("local[3]").setAppName("RddStudy01")
    val sc = new SparkContext(cof)
    val data = Array(1, 2, 3, 4, 5)
    //用数据创建RDD数据集
    //默认分区数为cpu指定的核数，3
    val dataRdd = sc.parallelize(data)
    print(dataRdd.getNumPartitions )//查看分区数
    print('\n')
    //创建时指定分区
    val dataRdd2 = sc.parallelize(data, 2)
    print(dataRdd2.getNumPartitions )//查看分区数
    print('\n')

    /**
     * 获取外部数据源函数textFile
     * wordcount案例
     */
    //获取外部数据源
//   返回类型时RDD[String] 返回的是文件内容，每一个RDD元素就是一行数据
    val fileRDD = sc.textFile("E:\\code\\IdeaProject\\studySpark\\src\\main\\resources\\DataFile\\word",2)
    //获取第一行
    val word : RDD[String] = fileRDD.flatMap(line => line.split(" "))//筛选算子
    val wordkv = word.map(word => (word,1))//map映射算子
    wordkv.reduceByKey((x,y)=>x+y).foreach(println)//action算子，会引入shuffle
  }
}