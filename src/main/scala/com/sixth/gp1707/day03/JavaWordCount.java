package com.sixth.gp1707.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 15:01 2018/7/25
 * @
 */
public class JavaWordCount {
    public static void main(String[] args) {
        // 模板代码
        SparkConf conf = new SparkConf()
                .setAppName("JavaWordCount")
                .setMaster("local[2]");

        // 上下文对象
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 获取数据
        JavaRDD<String> lines = jsc.textFile("e:\\hadoopdata\\123.txt");

        // 切分数据并压平
        // 输入string， 输出string
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> {
            // 返回一个List
            return Arrays.asList(s.split(" "));
        });

        // 生成元组
        // 第一个是输出的单词的类型
        // 第二个是生成的元组的key的类型
        // 第三个是生成元组的value的类型
        JavaPairRDD<String, Integer> tuples = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        // 聚合
        // v1是第一个元组的value， v2是第二个元组的value
        JavaPairRDD<String, Integer> summed = tuples.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        // Java的API没有提供sortBy算子，如果需要以value字段进行排序，需要把数据反转一下，排序完成后，再反转回来
        JavaPairRDD<Integer, String> swapped = summed.mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) tup -> new Tuple2<Integer, String>(tup._2, tup._1));

        // 排序
        JavaPairRDD<Integer, String> sorted = swapped.sortByKey(false);

        // 把排序后的数据反转回来
        JavaPairRDD<String, Integer> res = sorted.mapToPair((PairFunction<Tuple2<Integer, String>, String, Integer>) tup -> tup.swap());

        System.out.println(res.collect());

        // 将资源关闭
        jsc.stop();
    }
}
