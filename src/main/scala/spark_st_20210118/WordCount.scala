package spark_st_20210118

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  val url = "local"
  val appName = "wordCount"
  val source_Path = "D:\\data\\idea\\wordspace\\spark_test\\input"
  val dest_Path = ""


  def main(args: Array[String]): Unit = {

//    1、准备spark环境

    val sparkConf: SparkConf = new SparkConf().setMaster(url).setAppName(appName)
//    2、建立连接

    val sc = new SparkContext(sparkConf)
//    3、业务逻辑
//          3.1 读取文件
    val input: RDD[String] = sc.textFile(source_Path)
//          3.1 切分单词
    val wordRDD: RDD[String] = input.flatMap(_.split(" "))
    val mapRDD01: RDD[(String, Int)] = wordRDD.map(word => (word, 1))
//          3.1 聚合
//    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(word => word)
//    val mapRDD: RDD[(String, Int)] = groupRDD.map {
////      case (word, iter) => {
////        (word, iter.size)
////      }
////    }
    val reduceRDD: RDD[(String, Int)] = mapRDD01.reduceByKey(_ + _)
//          3.1 展示
    val array: Array[(String, Int)] = reduceRDD.collect()
    println(array.mkString(","))

//    4、关闭连接
    sc.stop()
  }

}
