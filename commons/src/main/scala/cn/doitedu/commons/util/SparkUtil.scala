package cn.doitedu.commons.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtil {

  /**
   * 构建SparkSession函数
   * @param appName
   * @param master
   * @param confMap
   * @return
   */
  def getSparkSession(appName:String = "app",master:String = "local[*]",confMap:Map[String,String] = Map.empty):SparkSession = {
    val conf = new SparkConf()
    conf.setAll(confMap)
    SparkSession.builder()
      .appName(appName)
      .master(master)
      .config(conf)
      .getOrCreate()
  }

}
