package cn.spark.study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple1;
import scala.Tuple2;

import java.util.*;


/**
 * @Auther: huzhibin_zz
 * @Date: 2022/8/30
 * @Description :
 */
public class Transformations {
    static String DATAPATH="E:\\code\\IdeaProject\\studySpark\\src\\main\\resources\\DataFile\\";
    static String OUTPATH="E:\\code\\IdeaProject\\studySpark\\src\\main\\resources\\OUT\\";
    static SparkConf conf = new SparkConf().setAppName("studyRdd").setMaster("local");
    //必须使用JavaSparkContext
    static JavaSparkContext sc=new JavaSparkContext(conf);

    public static void main(String[] args) {
        map();
    }

    /** map(func),mapPartitions(func),mapPartitionsWithIndex(func),flatmap(func)
     * map
     * 对RDD中的每一个元素都操作，形成新的RDD
     *
     * mapPartitions
     * 和map类似但是他处理的是分区，可以再对分区进行map算子映射成想要的结果.
     *func的函数类型必须是 Iterator[T] => Iterator[U]假设有 N 个元素，有 M 个分区，那么 map 的函数的将被调用N 次,而 mapPartitions 被调用 M 次,一个函数一次处理所有分区。
     *
     * mapPartitionsWithIndex(func)
     * 类似于mapPartitions，但 func 带有一个整数参数表示分片的索引值，因此在类型为 T 的RDD 上运行时，func 的函数类型必须是(Int, Interator[T]) => Iterator[U]；
     *
     * flatmap  将某一个元素拆分多个元素，返回成一个RDD,因此对每一个元素的操作返回的都是一个迭代器
     * 算子的编写可以使用lambda表达式或者函数式接口写
     */
    public  static void map(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        Integer intData[] = {1,2,3,4,6,6,2,5};
        //创建RDD并指定分区数
        JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(intData),3);

        System.out.println(intRDD.getNumPartitions());

        //Java的map算子有两种 map是对同类型RDD进行操作（输入和输出类型）,mapToPair把value类的rdd映射为key-value类型的RDD
        intRDD.map(x->x+1).foreach(x-> System.out.println(x));

        intRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                List<Integer> list = new ArrayList<>();
                while (integerIterator.hasNext())
                {
//                   对分区中的元素进行处理
                    Integer intnum = integerIterator.next();
                    intnum++;
                    list.add(intnum);
                }
                return list.iterator();
            }
        }).foreach(x-> System.out.println(x));
//#####################################################################################
//        intRDD.mapPartitionsWithIndex((index,x)->{
////            List<Integer> list = new ArrayList<>();
//            while (x.hasNext()) {
//                Integer integer = x.next();
//                integer++;
//
////                list.add(integer);
//            }
//            return new Tuple2<>(index)
//        })
//#####################################################################################
        JavaRDD<String> dataRDD = sc.parallelize(Arrays.asList(strdata));

        JavaRDD<String> flatMapRDD = dataRDD.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        //把value类的rdd映射为key-value类型的RDD
        JavaPairRDD pairRDD = flatMapRDD.mapToPair(x -> new Tuple2(x, 1));

        System.out.println(pairRDD.collect());

    }



}
