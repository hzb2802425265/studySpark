package cn.spark.study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Auther: huzhibin_zz
 * @Date: 2022/8/30
 * @Description :
 */
public class SparkSQL {
    static String DATAPATH="E:\\code\\IdeaProject\\studySpark\\src\\main\\resources\\DataFile\\";
    static String OUTPATH="E:\\code\\IdeaProject\\studySpark\\src\\main\\resources\\OUT\\";
    static SparkConf conf = new SparkConf().setAppName("studyRdd").setMaster("local");
    //必须使用JavaSparkContext
    static JavaSparkContext sc=new JavaSparkContext(conf);

    public static void main(String[] args) {

    }
}
