package cn.spark.study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * @Auther: huzhibin_zz
 * @Date: 2022/8/30
 *
 */
public class Transformations {
    static String DATAPATH="E:\\code\\IdeaProject\\studySpark\\src\\main\\resources\\DataFile\\";
    static String OUTPATH="E:\\code\\IdeaProject\\studySpark\\src\\main\\resources\\OUT\\";
    static SparkConf conf = new SparkConf().setAppName("studyRdd").setMaster("local");
    //必须使用JavaSparkContext
    static JavaSparkContext sc=new JavaSparkContext(conf);

    public static void main(String[] args) {

        //map,mapPartitions,mapPartitionsWithIndex,flatmap
        map();
        filter();
        sample();
        distinct();
        groupby();
        groupBykey();
        reduceByKey();
        sortByKey();
        coalesce();
        repartition();
        union();
        intersection();
        join();
        cogroup();
        cartesian();


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

//      JavaRDD<String> flatMapRDD = dataRDD.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaRDD<String> flatMapRDD = dataRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split).iterator();
            }
        });

        //把value类的rdd映射为key-value类型的RDD
        JavaPairRDD pairRDD = flatMapRDD.mapToPair(x -> new Tuple2(x, 1));

        System.out.println(pairRDD.collect());

    }

    public static void filter(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata));

//     JavaRDD<String> word = rdd.filter(x -> x.contains("word"));
        JavaRDD<String> word = rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                boolean flag = s.contains("word");
                return flag;
            }
        });


        System.out.println(rdd.collect());
        System.out.println(word.collect());

    }

    /**##########################################################
     * sample算子，抽样算子，将RDD按照一点的比例进行抽样，形成新的rdd
     * sample(withReplacement, fraction, seed)
     * withReplacement :抽出后是否放回，true,false
     * fraction :0-1，抽出样本的大致比例
     * seed ：给的随机数生成器种子 ############(选填)
     */
    public static void sample(){
        Integer intData[] = {1,2,3,4,6,6,2,5};
        //创建RDD并指定分区数
        JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(intData),1);
        intRDD.sample(true,0.3,1).foreach(x-> System.out.println(x));
    }

    /** distinct([numTasks]))
     *
     * 对源RDD 进行去重后返回一个新的 RDD。
     * 默认情况下，只有 8 个并行任务来操作，但是可以传入一个可选的 numTasks 参数改变它。
     *
     */
    public static void distinct(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata));
        JavaRDD<String> wordRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        //distinct()
        JavaRDD<String> distinctWordRDD = wordRDD.distinct();

        System.out.println(distinctWordRDD.collect());

    }

    /** groupby(func)
     * 1.分组，按照传入函数的返回值进行分组。将相同的 key 对应的值放入一个迭代器。
     * 个人觉得类似与filter,func是对源RDD数据的一个处理
     */
    public static void groupby(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata));
        JavaRDD<String> wordRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        wordRDD.groupBy(new Function<String, Iterator<String>>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                boolean o = s.contains("o");
                List<String> list = new ArrayList<>();
                if(o){
                    list.add(s);
                }
                return list.iterator();
            }
        });
    }
    /**
     * 对key-value类型的RDD按照key进行分组
     * groupByKey([numPartitions])
     *  groupByKey 按key分组
     * 结果 (spark,[1, 1, 1, 1]), (hadoop,[1, 1]), (word,[1, 1])
     */
    public static void groupBykey(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata));
        JavaRDD<String> wordRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD pairRDD = wordRDD.mapToPair(x -> new Tuple2(x, 1));
        JavaPairRDD groupByKey = pairRDD.groupByKey();
        System.out.println(groupByKey.getNumPartitions());
        System.out.println(groupByKey.collect());

        JavaPairRDD groupByKey2 = pairRDD.groupByKey(3);
        System.out.println(groupByKey2.getNumPartitions());
        System.out.println(groupByKey2.collect());


    }

    /**
     * reduceByKey(func, [numPartitions])
     *1在一个(K,V)的 RDD 上调用，返回一个(K,V)的 RDD，使用指定的reduce 函数，将相同
     * key 的值聚合到一起，reduce 任务的个数可以通过第二个可选的参数来设置。
     *
     * func结合自己的输入和输出决定用哪一个函数式接口，他提示的func做参考
     *
     * 1.reduceByKey：按照 key 进行聚合，在 shuffle 之前有 combine（预聚合）操作，返回结果是 RDD[k,v]。
     * 2.groupByKey：按照 key 进行分组，直接进行 shuffle。
     * 3.开发指导：reduceByKey 比 groupByKey，建议使用。但是需要注意是否会影响业务逻辑。
     */
    public static void reduceByKey(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata));
        JavaRDD<String> wordRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD pairRDD = wordRDD.mapToPair(x -> new Tuple2(x, 1));
        JavaPairRDD reduce = pairRDD.reduceByKey((Function2<Integer,Integer,Integer>) (x,y)->(x+y));
        System.out.println(reduce.collect());

    }
    /**
     * aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions])
     * 1在 kv 对的RDD 中，，按 key 将 value 进行分组合并，合并时，将每个 value 和初始值作为 seq 函数的参数，进行计算，返回的结果作为一个新的 kv 对，然后再将结果按照key 进行合并，最后将每个分组的 value 传递给 combine 函数进行计算（先将前两个 value
     * 进行计算，将返回结果和下一个 value 传给 combine 函数，以此类推），将 key 与计算结果作为一个新的kv 对输出。
     */
    public static void aggregateByKey(){

    }

    /**
     * sortByKey([ascending], [numPartitions])
     * 在一个(K,V)的 RDD 上调用，K 必须实现Ordered 接口，返回一个按照 key 进行排序的(K,V)的 RDD
     * ascending ：true 正序，false 倒叙
     * numPartitions：新RDD的分区数
     */
    public static void sortByKey(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata));
        JavaRDD<String> wordRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairRDD = wordRDD.mapToPair(x -> new Tuple2(x, 1));
        JavaPairRDD<String, Integer> reduce = pairRDD.reduceByKey((Function2<Integer,Integer,Integer>) (x,y)->(x+y));
//        System.out.println(reduce.collect());
        JavaPairRDD<Integer,String> pairRDD1 = reduce.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });
//        true 正序，false倒叙
        System.out.println(pairRDD1.sortByKey(true).collect());
        System.out.println(pairRDD1.sortByKey(false).collect());
    }
    /**
     * pipe(command, [envVars])
     * 管道，针对每个分区，都执行一个 shell 脚本，返回输出的RDD。
     * 需要shell脚本咱不掩饰
     * 参数：脚本路径字符串
     */
    public static void pipe(){

    }

    //数据重分布算子
    /**
     * coalesce(numPartitions)
     * 缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
     */
    public static void coalesce(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata),5);//设置分区为5
        System.out.println(rdd.getNumPartitions());
        JavaRDD<String> c = rdd.coalesce(3);//将分区数缩小到三
        System.out.println(c.getNumPartitions());

    }
    /**
     * repartition(numPartitions)
     * 根据分区数，重新通过网络随机洗牌所有数据。
     * 重新分区
     *二者区别
     *1.coalesce 重新分区，可以选择是否进行 shuffle 过程。由参数 shuffle: Boolean = false/true决定。######个人认为功能一样用这个
     *2.repartition 实际上是调用的 coalesce，进行 shuffle。源码如下：
     *
     */
    public static void repartition(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata),2);
        System.out.println(rdd.getNumPartitions());
        JavaRDD<String> c = rdd.repartition(5);
        System.out.println(c.getNumPartitions());
    }
    /**
     * ################################################################
     * repartitionAndSortWithinPartitions(partitioner)
     * 适用于k-v形式的RDD,
     * 按照指定的分区规则重新分区并且在分区内进行排序
     * 分区规则怎么写？？？？？
     */
    public static void repartitionAndSortWithinPartitions(){

        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata),2);
        JavaRDD<String> wordRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairRDD = wordRDD.mapToPair(x -> new Tuple2(x, 1));
//        pairRDD.repartitionAndSortWithinPartitions(partitioner);
    }

    /**
     * union(otherDataset)
     * RDD的 并 操作
     * 1.对源 RDD 和参数 RDD 求并集后返回一个新的RDD
     * 用于两个类型一致的RDD
     */
    public static void union(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata),2);
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(strdata), 1);

        JavaRDD<String> wordRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaRDD<String> wordRDD1= rdd1.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaRDD<String> unionRDD = wordRDD.union(wordRDD1);
        System.out.println(unionRDD.collect());

    }
    /**
     * intersection(otherDataset)
     * 1.对源RDD 和参数 RDD 求交集后返回一个新的RDD
     * 用于两个类型一致的RDD
     */
    public static void intersection(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        String strdata2[] = {"word  hadoop","hadoop abk","aaa spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata),2);
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(strdata2), 1);

        JavaRDD<String> wordRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaRDD<String> wordRDD1= rdd1.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaRDD<String> intersectionRDD = wordRDD.intersection(wordRDD1);
        System.out.println(intersectionRDD.collect());

    }

    /**
     * join(otherDataset, [numPartitions])
     * 1.在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素对在一起的(K,(V,W))的 RDD
     * 结果
     * (word,(1,word)),
     * (word,(1,word)),
     * (word,(1,word)),
     * (word,(1,word)),
     * (spark,(1,spark))
     *
     */
    public static void join(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata),2);
        JavaRDD<String> wordRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairRDD = wordRDD.mapToPair(x -> new Tuple2(x, 1));
        JavaPairRDD<String, String> pairRDD1 = wordRDD.mapToPair(x -> new Tuple2(x, x));
        JavaPairRDD<String, Tuple2<Integer, String>> joinRDD = pairRDD.join(pairRDD1);
        System.out.println(joinRDD.collect());

    }
    /**
     * cogroup(otherDataset, [numPartitions])
     * 1.在类型为(K,V)和(K,W)的 RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
     * 和join不同的是将value一个类型的集合在一起形成迭代器
     * 结果
     * (word,([1, 1],[word, word])),
     * (spark,([1, 1, 1, 1],[spark, spark, spark, spark]))
     * (hadoop,([1, 1],[hadoop, hadoop]))
     */
    public static void cogroup(){
        String strdata[] = {"word spark hadoop","hadoop spark","word spark spark"};
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(strdata),2);
        JavaRDD<String> wordRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairRDD = wordRDD.mapToPair(x -> new Tuple2(x, 1));
        JavaPairRDD<String, String> pairRDD1 = wordRDD.mapToPair(x -> new Tuple2(x, x));
        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<String>>> cogroupRDD = pairRDD.cogroup(pairRDD1);
        System.out.println(cogroupRDD.collect());
    }

    /**
     * cartesian(otherDataset)
     * 笛卡尔积（尽量避免使用）
     * RDD(a)
     * RDD(B)
     * RDD(a,b)所有情况
     * 结果：(1,1), (1,2), (1,3), (2,1), (2,2), (2,3), (3,1), (3,2), (3,3)
     */
    public static void cartesian(){
        Integer intData[] = {1,2,3};
        Integer intData2[] = {2,5,6};
        JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(intData),3);
        JavaRDD<Integer> intRDD2 = sc.parallelize(Arrays.asList(intData),3);
        JavaPairRDD<Integer, Integer> cartesianRDD = intRDD.cartesian(intRDD2);
        System.out.println(cartesianRDD.collect());


    }



}
