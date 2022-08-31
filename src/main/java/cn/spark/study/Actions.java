package cn.spark.study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Auther: huzhibin_zz
 * @Date: 2022/8/30
 */
public class Actions {
    private static String DATAPATH="E:\\code\\IdeaProject\\studySpark\\src\\main\\resources\\DataFile\\";
    private static String OUTPATH="E:\\code\\IdeaProject\\studySpark\\src\\main\\resources\\OUT\\";
    private static SparkConf conf = new SparkConf().setAppName("studyRdd").setMaster("local");
    //必须使用JavaSparkContext
    private static JavaSparkContext sc=new JavaSparkContext(conf);

    /**
     * Action算子相对较少
     * @param args
     */
    public static void main(String[] args) {
        Integer intData2[] = {10,5,6};
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> stringJavaRDD = sc.parallelize(Arrays.asList(strdata));
        JavaRDD<String> wordRDD = stringJavaRDD.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        //设置分区为5
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(intData2));

        /**reduce(func)
         * 1.通过 func 函数聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据。
         */
//      对RDD数据做计算，例子是求和
//        Integer reduceRDD = rdd.reduce((x, y) -> x + y);
        Integer reduceRDD = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        System.out.println(reduceRDD);

        /**collect()
         * 将RDD中的所有元素放进(列表)数组中返回到Driver端
         */
        List<Integer> collectList = rdd.collect();
        System.out.println(collectList);

        /**
         * count()
         * 1.返回RDD 中元素的个数
         */
        long count = rdd.count();
        System.out.println(count);

        /**
         * first()
         * 返回RDD 中的第一个元素
         */
        Integer first = rdd.first();
        System.out.println(first);

        /**
         * take(n)
         * 返回一个由 RDD 的前 n 个元素组成的(列表)（scala 数组）
         * takeOrdered(n)
         * 返回该 RDD 排序后的前 n 个元素组成的数组
         */
        List<Integer> takeN = rdd.take(2);
        System.out.println(takeN);
//       这里可以提供多一个comparator()
        List<Integer> takeOrdered = rdd.takeOrdered(2);
        System.out.println(takeOrdered);

        /**takeSample(withReplacement, num, [seed])
         * 返回一个数组，其中包含RDD中num个元素的随机样本
         * withReplacement,抽样是否放回 true ,false
         * seed 可选地预先指定随机数生成器种子??????
         */
        List<Integer> list = rdd.takeSample(true, 3);
        System.out.println(list);

        /**
         * saveAsTextFile(path)
         * 将数据集的元素作为文本文件(或一组文本文件)写入本地文件系统、
         * HDFS或任何其他hadoop支持的文件系统中的给定目录中。
         * Spark将对每个元素调用toString，将其转换为文件中的一行文本。
         */
        rdd.saveAsTextFile(OUTPATH+"result4");
        /**
         * saveAsSequenceFile(path)
         * 将数据集的元素作为Hadoop SequenceFile写入本地文件系统、
         * HDFS或任何其他Hadoop支持的文件系统中的给定路径。
         * 这在实现Hadoop可写接口的键-值对的rdd上可用。
         * 在Scala中，它也可以用于隐式转换为可写的类型(Spark包括对基本类型的转换，如Int, Double, String等)
         *
         */

        /**
         * saveAsObjectFile(path)
         * 使用Java序列化以简单的格式写入数据集的元素，然后可以使用SparkContext.objectFile()加载这些元素。
         */
        /**
         * countByKey()
         * 仅在类型为(K, V)的rdd上可用。返回一个(K, Int)对的map，表示每个键的计数。
         *
         */
        JavaPairRDD<String, Integer> pairRDD = wordRDD.mapToPair(x -> new Tuple2(x, 1));
        Map<String, Long> map = pairRDD.countByKey();
        System.out.println(map);

        /**
         * foreach(func)
         * 在数据集的每个元素上运行一个函数func。这通常是为了应对副作用，如更新累加器或与外部存储系统交互。
         * 在数据集的每一个元素上，运行函数 func 进行更新
         * 打印功能
         */
        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                 integer++;
                System.out.println(integer);
            }

        });

    }
}
