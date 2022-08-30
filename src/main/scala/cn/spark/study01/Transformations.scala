package cn.spark.study01

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: huzhibin_zz
 * @Date: 2022/8/30
 * @Description :
 *Transformations主要分为value类型和key-value类型
 *
 */
object Transformations {

  def main(args: Array[String]): Unit = {
    val cof = new SparkConf().setMaster("local[3]").setAppName("RddStudy01")
    //默认分区为cpu核数3
    val sc = new SparkContext(cof)
//    设置分区为2
    val dataRDD = sc.parallelize(Array(1,2,3,4,2,1,4,5),2)
    val strRDD = sc.parallelize(Array("word spark","word word","spark word","word"),2)
    /** map
     * Transformations算子 map和Actions算子foreach算子
     * spark当遇到action时才开始计算，这时spark的延迟计算思想
     */
    //map(func),将RDD中每一个元素的值*2组成新的RDD
    val result1 = dataRDD.map(x => x * 2)
    result1.foreach(println)
    //map(func),将RDD中每一个元素的作为key,1为value
    val result2 = dataRDD.map(x => (x, 1))
    result2.foreach(println)

    /** mapPartitions对每个分区单独进行计算
     * func 的函数类型必须是 Iterator[T] => Iterator[U]。
     * 假设有 N 个元素，有 M 个分区，那么 map 的函数的将被调用N 次,而 mapPartitions 被调用 M 次,一个函数一次处理所有分区。
     * 和map的效果类似
     */
      //这里对分区中的元素每个*2，x是分区的集合（迭代器）
    val result3 = dataRDD.mapPartitions(x => x.map(_ * 2))
    result3.foreach(println)
    val result4 = dataRDD.mapPartitions(x => x.map((_,1)))
    result4.foreach(println)
    /** mapPartitionsWithIndex(func)
     * 类似于mapPartitions，但 func 带有一个整数参数表示分片的索引值，
     * 因此在类型为 T 的RDD 上运行时，func 的函数类型必须是(Int, Interator[T]) => Iterator[U]；
     */
    //使每个元素跟所在分区形成一个元组组成一个新的RDD
//    index是分区索引，x是分区所有数数据的集合（迭代器），对每个分区的集合操作，是的元素都变为（index,value）
    val result5 = dataRDD.mapPartitionsWithIndex((index, x) => x.map((index,_)))
    result5.foreach(println)

    /**
     * flatMap
     * 类似与map,使每个元素跟所在分区形成一个元组组成一个新的RDD
     * 以 func 应该返回一个序列，而不是单一元素
     */
    val result6 = strRDD.flatMap(x => x.split(" "))
    result6.foreach(println)
    val strings = result6.take(2)
    val l = result6.count()//求RDD元素个数的算子
    println(l)

    /**
     * filter(func)
     * 筛选func返回true的值
     */
    dataRDD.filter(_==1).foreach(println)

    /**
     * 将每一个分区形成一个数组，形成新的RDD 类型时 RDD[Array[T]]
     */
    //   每个分区的数据放到一个数组
    val result7 = result6.glom()
//    result7.foreach(println)



    /**
     * sample(withReplacement, fraction, seed)
     * 以指定的随机种子随机抽样出数量为 fraction 的数据，
     * withReplacement 表示是抽出的数据是否放回，true 为有放回的抽样，false 为无放回的抽样，
     * seed 用于指定随机数生成器种子。????
     *
     */
    dataRDD.sample(true,0.5,0).foreach(println)

    /** distinct([numTasks]))
     *
     * 对源RDD 进行去重后返回一个新的 RDD。
     * 默认情况下，只有 8 个并行任务来操作，但是可以传入一个可选的 numTasks 参数改变它。
     *
     */
    dataRDD.distinct().foreach(println)

    /**
     * groupByKey([numPartitions])
     * groupByKey 按key分组
     * (1,CompactBuffer(1, 1))
     */
      val group = result2.groupByKey()
       group.foreach(println)
       group.map((x)=>(x._1,x._2.sum)).foreach(println)

    /**
     * reduceByKey(func, [numPartitions])
     *1在一个(K,V)的 RDD 上调用，返回一个(K,V)的 RDD，使用指定的reduce 函数，将相同
     * key 的值聚合到一起，reduce 任务的个数可以通过第二个可选的参数来设置。
     *
     * 1.reduceByKey：按照 key 进行聚合，在 shuffle 之前有 combine（预聚合）操作，返回结果是 RDD[k,v]。
     * 2.groupByKey：按照 key 进行分组，直接进行 shuffle。
     * 3.开发指导：reduceByKey 比 groupByKey，建议使用。但是需要注意是否会影响业务逻辑。
     */
    result2.reduceByKey((x,y)=>x+y).foreach(println)

    /**
     * aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions])
     * 1在 kv 对的RDD 中，，按 key 将 value 进行分组合并，合并时，将每个 value 和初始值作为 seq 函数的参数，进行计算，返回的结果作为一个新的 kv 对，然后再将结果按照key 进行合并，最后将每个分组的 value 传递给 combine 函数进行计算（先将前两个 value
     * 进行计算，将返回结果和下一个 value 传给 combine 函数，以此类推），将 key 与计算结果作为一个新的kv 对输出。
     */





    /**
     * sortByKey([ascending], [numPartitions])
     * 在一个(K,V)的 RDD 上调用，K 必须实现Ordered 接口，返回一个按照 key 进行排序的(K,V)的 RDD
     * ascending ：true 正序，false 倒叙
     * numPartitions：新RDD的分区数
     */
    result2.sortByKey(false,2).foreach(println)


    /**
     * pipe(command, [envVars])
     * 管道，针对每个分区，都执行一个 shell 脚本，返回输出的RDD。
     * 需要shell脚本咱不掩饰
     * 参数：脚本路径字符串
     */

    //数据分片重分布
    /**
     * coalesce(numPartitions)
     * 缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
     */
    println(dataRDD.getNumPartitions)
    val dataRDD2 = dataRDD.coalesce(1)
    println(dataRDD2.getNumPartitions)
    /**
     * repartition(numPartitions)
     * 根据分区数，重新通过网络随机洗牌所有数据。
     * 重新分区
     *二者区别
     *1.coalesce 重新分区，可以选择是否进行 shuffle 过程。由参数 shuffle: Boolean = false/true决定。######个人认为功能一样用这个
     *2.repartition 实际上是调用的 coalesce，进行 shuffle。源码如下：
     *
     */
    println(dataRDD.getNumPartitions)
    val dataRDD3 = dataRDD.repartition(1)
    println(dataRDD3.getNumPartitions)


    /**
     * repartitionAndSortWithinPartitions(partitioner)
     * 适用于k-v形式的RDD,
     * 按照指定的分区规则重新分区并且在分区内进行排序
     * 分区规则怎么写？？？？？
     */
//    println(result2.getNumPartitions)
//
//    val dataRDD4 = result2.repartitionAndSortWithinPartitions(partitioner=)
//    println(dataRDD4.getNumPartitions)
//    dataRDD4.foreach(println)

    /**
     * union(otherDataset)
     * RDD的 并 操作
     * 1.对源 RDD 和参数 RDD 求并集后返回一个新的RDD
     * 用于两个类型一致的RDD
     */
    result1.union(dataRDD).foreach(println)//rdd[int]
    result2.union(result2).foreach(println)//rdd[int,int]

    /**
     * intersection(otherDataset)
     * 1.对源RDD 和参数 RDD 求交集后返回一个新的RDD
     * 用于两个类型一致的RDD
     */
    result1.intersection(dataRDD).foreach(println)//rdd[int]


    /**
     * join(otherDataset, [numPartitions])
     * 1.在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素对在一起的(K,(V,W))的 RDD
     * 结果
     * (aaa,(1,ddd))
     * (vvv,(2,eee))
     */
    val a1 = Array(("aaa", 1), ("vvv", 2))
    val a2 = Array(("aaa", "ddd"), ("vvv", "eee"))
    val join1 = sc.parallelize(a1)
    val join2 = sc.parallelize(a2)
    val  Result = join1.join(join2).foreach(println)




    /**
     * cogroup(otherDataset, [numPartitions])
     * 1.在类型为(K,V)和(K,W)的 RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
     * 和join不同的是将value一个类型的集合在一起形成迭代器
     * 结果
     * (aaa,(CompactBuffer(1),CompactBuffer(ddd)))
     * (vvv,(CompactBuffer(2),CompactBuffer(eee)))
     */
    val Result2 = join1.cogroup(join2).foreach(println)



    /**
     * cartesian(otherDataset)
     * 笛卡尔积（尽量避免使用）？？？
     * RDD(a)
     * RDD(B)
     * RDD(a,b)所有情况
     * 结果：
     * (1,2)
     * (1,4)
     * (1,3)
     * (1,5)
     * (2,2)
     * (2,3)
     * (2,4)
     * (2,5)
     * (3,2)
     * (3,3)
     * (3,4)
     * (3,5)
     */
    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(2 to 5)
    rdd1.cartesian(rdd2).foreach(println)


  }

}
