package cn.spark.study;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import org.junit.Test;

import scala.Tuple2;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * @Auther: huzhibin_zz
 * @Date: 2022/8/29
 * @Description :
 */
public class study01{

    static String DATAPATH="E:\\code\\IdeaProject\\studySpark\\src\\main\\resources\\DataFile\\";
    static String OUTPATH="E:\\code\\IdeaProject\\studySpark\\src\\main\\resources\\OUT\\";
    static SparkConf conf = new SparkConf().setAppName("studyRdd").setMaster("local");
    //必须使用JavaSparkContext
    static JavaSparkContext sc=new JavaSparkContext(conf);

    public static void main(String[] args) {
//        WordCount();
        studyRDD();
    }

    public static void WordCount(){

        String pathS = DATAPATH + "word";
        //JavaRDD<String>就是RDD[String]
        JavaRDD<String> inputData = sc.textFile(pathS);
        //new一个算子中的函数，对数据做分割处理,也可以使用Lambda表达式
//        JavaRDD<String> word =inputData.flatMap(new FlatMapFunction() {
//            @Override
//            public Iterator<String> call(String line) throws Exception {
//                String[] split = line.split(" ");
//
//                return Arrays.asList(split).iterator();
//            }
//        });
        JavaRDD<String> word = inputData.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

//        Lambda表达式,映射处理word -> word,1
//        JavaPairRDD<String, Integer> pairRDD = word.mapToPair(x -> new Tuple2<>(x, 1));
        JavaPairRDD<String, Integer> pairRDD = word.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> result = pairRDD.reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

        JavaPairRDD<String, Integer> RESULT = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        }).sortByKey().mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<>(integerStringTuple2._2, integerStringTuple2._1);
            }
        });

        RESULT.foreach(x-> System.out.println(x));

//        pairRDD.reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                System.out.println(stringIntegerTuple2);
//            }
//        });

//        JavaPairRDD<String, Integer>kvPairRDD = pairRDD.reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);
//
//        kvPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                System.out.println(stringIntegerTuple2);
//            }
//        });
//    }

        System.out.println(RESULT.first());//返回RDD中的第一个元素
        System.out.println(RESULT.count());//返回数据集中的元素数。
        System.out.println(RESULT.collect());//以数组的形式返回RDD中所有的元素
        System.out.println(RESULT.take(2));//以数组的形式返回RDD中所有前2个元素
//        RESULT.saveAsTextFile(OUTPATH+"result02");

    }

    /**
     * Rdd的学习
     * RDD弹性分布式数据集
     * 对RDD的操作算子，分为两种，Transformations转换算子和Actions行动算子
     */
    public static void studyRDD() {
        Integer data[] = {1,2,5,7,2,5,5,10,4,5,6,7,1,7};
        String Str[] = {"aaba","bbb","cbcc","aba","dd","bb"};
        JavaRDD<Integer> numRDD = sc.parallelize(Arrays.asList(data));
        JavaRDD<String> strRDD = sc.parallelize(Arrays.asList(Str));
//        sc.textFile("hdfs://hadoop102:9000/RELEASE")


//        首先学习转换算子
        /**
         * 对单个RDD进行转换的算子
         */
        //map算子：官方解释: 返回一个新的分布式数据集,通过一个函数func对每个元素进行转换。
        //我的理解是将数据转换为<key,value>键值对
        JavaPairRDD<String, Integer> kvRDD = numRDD.mapToPair(new PairFunction<Integer, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Integer integer) throws Exception {
                if (integer > 0 && integer <= 5) {
                    return new Tuple2<>("[1-5]", 1);
                } else {
                    return new Tuple2<>("[6-10]", 1);
                }
            }
        });
        kvRDD.foreach(x-> System.out.println(x));//打印Rdd内容的算子，是Actions算子

        //flatMap:解释，对RDD做映射操作的算子，一般用于将RDD的每个元素进行操作，获得一个新的RDD，例如把line按空格分成单词，lineRDD->wordRDD
        JavaRDD<String> flatMapRDD = strRDD.flatMap(new FlatMapFunction<String, String>(

        ) {
            @Override
            public Iterator<String> call(String s) throws Exception {
                char[] chars = s.toCharArray();

                List list = new ArrayList<Character>();
                list.add(chars.toString());

                return list.iterator();
            }
        });
        flatMapRDD.foreach(x-> System.out.println(x));
        System.out.println(flatMapRDD.count());


    }
}
